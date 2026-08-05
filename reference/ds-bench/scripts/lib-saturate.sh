#!/usr/bin/env bash
# Saturation walker: ramp client pods up a per-stream-count ladder until the
# server's throughput plateaus, then pin + confirm. Reuses the lib-bench engine.
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$REPO_ROOT/scripts/lib-bench.sh"

# _sat_cell_dir <pods> <rep> — where a measured cell's merged.json/samples land.
_sat_cell_dir() {
  echo "${SAT_RESULT_ROOT:-${REPO_ROOT}/results/_sat}/${SAT_MODE}/n${SAT_SC}/p${1}-r${2}"
}

# measure_pods <pods> -> "cpu_pct thr". Default = a real write cell via the engine;
# overridable by the test via MEASURE_FN. Builds the 7-arg _run_cell_one call the
# matrix uses (gke-bench.sh run_one), deriving streams/pod = ceil(SAT_SC/pods) and
# the target/api/namespace from the addressing globals deploy_mode set (T_TARGET,
# T_API, T_NS). Throughput + cpu come straight back from _run_cell_one.
measure_pods() {
  local pods="$1"
  local sc="${SAT_SC:?measure_pods: SAT_SC unset (walk_cell sets it)}"
  local mode="${SAT_MODE:?measure_pods: SAT_MODE unset}"
  local rep="${SAT_REP:-1}"
  local perpod=$(( (sc + pods - 1) / pods ))
  local warmup="${WARMUP_SECS:-15}" settle="${SETTLE_SECS:-5}" dur="${MEASURE_SECS:-20}"
  local cell_dir; cell_dir="$(_sat_cell_dir "$pods" "$rep")"
  mkdir -p "$cell_dir"
  # setup-concurrency throttles stream CREATION (decoupled from pod count / load):
  # at high cardinality, pods × 256 concurrent creates overwhelmed the creation
  # endpoint (the 100k creation_choke). A lower per-pod value keeps total concurrent
  # creation bounded while pods still drive full load after setup.
  local setup_conc="${SETUP_CONCURRENCY:-32}" payload="${PAYLOAD_BYTES:-256}"
  # Saturation cells SUM per-pod rates, so the fleet start barrier is mandatory:
  # every pod holds after stream creation until all are ready, then measures over
  # the same wall window (prevention; hdr-merge's windows_aligned is the check).
  export BARRIER_DIR="${BARRIER_DIR:-/barrier}"
  # The fleet's post-release lifetime is bounded (warmup+settle+measure+upload) —
  # size the completion wait to it instead of the generic default, which a large
  # fleet outlives (the old silent failure mode: the coordinator merged a partial,
  # time-skewed subset while pods were still running).
  export FLEET_TIMEOUT="${FLEET_TIMEOUT_OVERRIDE:-$(( warmup + settle + dur + 240 ))}"
  # CONNS_PER_POD>0 switches the client to the bounded-concurrency pool model: each
  # pod offers exactly CONNS_PER_POD in-flight appends cycled over its ${perpod}
  # streams (decouples offered load from stream count). 0/unset = legacy 1-per-stream.
  local conns_flag=""
  [ "${CONNS_PER_POD:-0}" -gt 0 ] 2>/dev/null && conns_flag="--connections ${CONNS_PER_POD}"
  # batch>1 → N records per POST (pool model); the dominant fleet-cost lever.
  local batch_flag=""
  [ "${BATCH_PER_POD:-1}" -gt 1 ] 2>/dev/null && batch_flag="--batch ${BATCH_PER_POD}"
  # Pool model: pods get the full GLOBAL stream count and derive their own
  # disjoint slice of it from DS_BENCH_INSTANCE/DS_BENCH_SHARDS (even key-space
  # coverage, no cross-pod stream sharing; each pod pre-creates its slice
  # before the barrier). Legacy: pods own disjoint pod-prefixed slices of
  # ceil(sc/pods) streams each.
  local streams_arg="$perpod"
  [ "${CONNS_PER_POD:-0}" -gt 0 ] 2>/dev/null && streams_arg="$sc"
  local bench_cmd="multi-stream --target ${T_TARGET:?} --api-style ${T_API:?} ${T_NS:-} --streams ${streams_arg} ${conns_flag} ${batch_flag} --duration-secs ${dur} --payload-bytes ${payload} --setup-concurrency ${setup_conc} --warmup-secs ${warmup} --settle-secs ${settle}"
  local merge_cmd="ds-bench hdr-merge --hdr-dir /merge --results-dir /merge --label-prefix multi-stream-"
  _run_cell_one "${mode}-write-n${sc}-p${pods}" "$bench_cmd" "write" "$merge_cmd" "$pods" "$rep" "$cell_dir"
}

_sat_get() {  # _sat_get s <python-expr-over-Suite s> — read a suite field
  python3 - "$SUITE_FILE" "$2" <<'PY'
import sys; sys.path.insert(0, "scripts")
from suite import Suite
s = Suite.load(sys.argv[1])
print(eval(sys.argv[2]))
PY
}

walk_cell() {
  local mode="$1" sc="$2" cells_json="$3" digest="$4"
  local fn="${MEASURE_FN:-measure_pods}"
  export SAT_MODE="$mode" SAT_SC="$sc"
  local plateau; plateau="$(_sat_get s 's.saturation["plateau_pct"]')"
  local repeats; repeats="$(_sat_get s 's.saturation["repeats"]')"
  # patience = consecutive sub-threshold gains required to call a plateau (default
  # 1 = legacy single-shot). >=2 makes one noisy-low rung no longer trigger a false
  # plateau (an under-reported ceiling). See saturation.plateau_pin.
  local patience; patience="$(_sat_get s 's.saturation.get("patience", 1)')"
  # Cap each rung at the stream count (+ dedup) so a low-cardinality cell never
  # over-provisions (pods > streams would drive more streams than intended).
  local ladder;  ladder="$(python3 -c "import sys;sys.path.insert(0,'scripts');from suite import Suite;from saturation import cap_ladder;s=Suite.load('$SUITE_FILE');print(' '.join(map(str,cap_ladder(s.ladder_for($sc),$sc))))")"

  local prev_pods=0 prev_thr=0 walk="[]" pods _cpu thr aligned p50 p99 decision
  for pods in $ladder; do
    SAT_REP=1; reset_state "$mode"
    # Optional third field: 0 = the fleet's measure windows didn't overlap, so the
    # rung has no valid throughput reading (thr arrives as 0 in that case). Test
    # mocks / older measure fns emit two fields → aligned defaults to 1. Fourth/
    # fifth: the rung's merged p50/p99 ms — recorded in the walk so the report can
    # separate pre-saturation (knee) latency from plateau queueing latency.
    read -r _cpu thr aligned p50 p99 < <("$fn" "$pods")
    aligned="${aligned:-1}"
    walk="$(python3 -c "
import json,sys
w=json.loads(sys.argv[1])
def f(x):
    try: return float(x)
    except (ValueError,TypeError): return None
e=[int(sys.argv[2]), float(sys.argv[3])]
p50, p99 = f(sys.argv[4]), f(sys.argv[5])
if p50 is not None: e += [p50, p99]
w.append(e); print(json.dumps(w))" "$walk" "$pods" "$thr" "${p50:-None}" "${p99:-None}")"
    # thr<=0 → error; else plateau_pin applies `patience`-rung hysteresis over the
    # walk so far and returns "plateau <pin_pods> <pin_thr>" or "continue".
    decision="$(python3 -c "
import sys, json; sys.path.insert(0,'scripts')
from saturation import plateau_pin
walk=json.loads(sys.argv[1]); thr=float(sys.argv[2]); plateau=float(sys.argv[3]); patience=int(sys.argv[4])
if thr <= 0:
    print('error')
else:
    pin = plateau_pin(walk, plateau, patience)
    print(f'plateau {pin[0]} {pin[1]}' if pin else 'continue')
" "$walk" "$thr" "$plateau" "$patience")"
    case "$decision" in
      error)
        local err_reason=creation_choke
        [ "$aligned" = "0" ] && err_reason=misaligned_windows
        _record "$cells_json" "$sc" "$digest" "$walk" None 0 None False error "$err_reason" None
        return 0 ;;
      plateau\ *)
        # plateau_pin gave the saturation rung; confirm it with `repeats` reps.
        # Capture via $(...) — it BLOCKS until _confirm fully EXITS. (Previously
        # `read < <(_confirm)` returned on _confirm's first stdout line — reset_state's
        # "deployment rolled out" — leaving _confirm running in the BACKGROUND, so the
        # next stream-count's walk ran CONCURRENTLY with the lingering confirm: two
        # walkers, interleaved fleets, "already exists", stale merged reuse.)
        local pin_pods pin_thr conf_out conf_p50 conf_p99 conf_thr rec_thr mem_mb
        read -r _ pin_pods pin_thr <<< "$decision"
        conf_out="$(_confirm "$fn" "$mode" "$pin_pods" "$repeats")"
        read -r conf_p50 conf_p99 conf_thr <<< "$conf_out"
        conf_p50="${conf_p50:-None}"; conf_p99="${conf_p99:-None}"   # never pass empty to _record
        # Report the MEAN throughput over the confirm reps when available (a
        # replicated headline, not a single shot); fall back to the walk's pin
        # throughput when the reps produced no reading (e.g. the unit-test mock).
        rec_thr="${conf_thr:-$pin_thr}"; [ -z "$rec_thr" ] && rec_thr="$pin_thr"
        mem_mb="$(_sat_peak_podmem "$sc")"
        _record "$cells_json" "$sc" "$digest" "$walk" "$pin_pods" "$rec_thr" "$conf_p50" True ok plateau "$conf_p99" "$mem_mb"
        return 0 ;;
      continue)
        prev_pods="$pods"; prev_thr="$thr" ;;
    esac
  done
  # ladder exhausted without plateau
  local mem_mb; mem_mb="$(_sat_peak_podmem "$sc")"
  _record "$cells_json" "$sc" "$digest" "$walk" "$prev_pods" "$prev_thr" None False ok ladder_exhausted None "$mem_mb"
}

# _confirm <fn> <mode> <pods> <reps> — rerun the pinned pods `reps` times; echo
# "p50 p99 thr_mean": the MEDIAN latency (ms) and the MEAN throughput (ops/s) over
# the reps, read from each rep's merged.json. This is where the reported headline
# gets replicated — `repeats>1` averages the throughput instead of quoting a single
# walk shot. No merged.json (the unit-test mock fn) -> "None None" (thr omitted, so
# the caller falls back to the walk's pin throughput).
_confirm() {
  local fn="$1" mode="$2" pods="$3" reps="$4" i p50s="" p99s="" thrs="" cd p50 p99 thr
  for ((i=1;i<=reps;i++)); do
    # reset_state -> stderr so _confirm's STDOUT carries ONLY the final result line
    # (its rollout/reset chatter must not be captured as the result).
    SAT_REP="$i"; reset_state "$mode" >&2; "$fn" "$pods" >/dev/null
    cd="$(_sat_cell_dir "$pods" "$i")"
    p50="$(grep -oE '"p50_ms"[: ]*[0-9.]+' "$cd/merged.json" 2>/dev/null | grep -oE '[0-9.]+$' | head -1)"
    p99="$(grep -oE '"p99_ms"[: ]*[0-9.]+' "$cd/merged.json" 2>/dev/null | grep -oE '[0-9.]+$' | head -1)"
    thr="$(grep -oE '"aggregate_ops_per_sec"[: ]*[0-9.]+' "$cd/merged.json" 2>/dev/null | grep -oE '[0-9.]+$' | head -1)"
    p50s="$p50s ${p50:-}"; p99s="$p99s ${p99:-}"; thrs="$thrs ${thr:-}"
  done
  # True median: average the two middle values for an even count (the old
  # a[int((NR+1)/2)] returned the LOWER middle — wrong once repeats is even, which
  # the reference suites now set via repeats:2).
  local med='{a[NR]=$1} END{ if(!NR){print "None"} else if(NR%2){print a[(NR+1)/2]} else {print (a[NR/2]+a[NR/2+1])/2} }'
  local mean='{s+=$1; n++} END{print (n? s/n : "")}'   # "" so caller can fall back
  echo "$(echo "$p50s" | tr ' ' '\n' | grep -v '^$' | sort -n | awk "$med")" \
       "$(echo "$p99s" | tr ' ' '\n' | grep -v '^$' | sort -n | awk "$med")" \
       "$(echo "$thrs" | tr ' ' '\n' | grep -v '^$' | awk "$mean")"
}

_record() {  # bridge to cells.py — args: cells sc digest walk pods thr p50 sat status reason p99 [mem_mb]
  python3 -c "
import sys; sys.path.insert(0,'scripts')
import cells
# Tolerant parsing: a malformed pinned_pods/p50/p99/throughput must NEVER crash the
# record and drop an otherwise-good cell (a bad confirm value silently lost cells).
def _f(x, d=None):
    try: return float(x)
    except (ValueError, TypeError): return d
def _i(x, d=None):
    try: return int(x)
    except (ValueError, TypeError): return d
pp = None if sys.argv[5]=='None' else _i(sys.argv[5])
p50 = None if sys.argv[7]=='None' else _f(sys.argv[7])
p99 = None if sys.argv[11]=='None' else _f(sys.argv[11])
mem = _f(sys.argv[12]) if len(sys.argv) > 12 and sys.argv[12] not in ('None','') else None
cells.record(sys.argv[1], int(sys.argv[2]), image_digest=sys.argv[3],
  walk=__import__('json').loads(sys.argv[4]), pinned_pods=pp, throughput=(_f(sys.argv[6]) or 0.0),
  p50=p50, p99=p99, saturated=(sys.argv[8]=='True'), status=sys.argv[9], reason=sys.argv[10], pod_mem_mb=mem)
" "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9" "${10}" "${11}" "${12:-None}"
}

# _sat_peak_podmem <stream_count> — max pod working-set MiB across every rep's
# samples.csv for this stream-count (the high-water mark over the whole walk).
_sat_peak_podmem() {
  local sc="$1" root="${SAT_RESULT_ROOT:-}" f peak=0 m
  [ -z "$root" ] && { echo 0; return; }
  for f in "$root"/*/n"$sc"/*/samples.csv; do
    [ -f "$f" ] || continue
    m="$(compute_server_podmem_mb "$f" 2>/dev/null || echo 0)"
    [ "${m:-0}" -gt "$peak" ] 2>/dev/null && peak="$m"
  done
  echo "$peak"
}
