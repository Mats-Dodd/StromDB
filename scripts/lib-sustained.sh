#!/usr/bin/env bash
# Sustained-load runner for the declarative suite framework. Unlike the saturation
# walker (which ramps pods to find a throughput peak), sustained holds a FIXED
# low offered rate over a LONG window and records stability: achieved throughput,
# tail latency, server RSS peak/drift, and mean CPU. One measurement per
# (config, stream_count) — no ladder. Reuses the lib-bench engine via lib-saturate.
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$REPO_ROOT/scripts/lib-saturate.sh"   # brings in lib-bench engine + _sat_cell_dir/_sat_get/reset_state

# measure_sustained <pods> -> "cpu_pct thr". Default real cell via the engine;
# overridable by tests via MEASURE_FN. Mirrors measure_pods but runs the
# `ds-bench sustained` subcommand (steady rate, long duration, snapshot series).
measure_sustained() {
  local pods="$1"
  local sc="${SAT_SC:?measure_sustained: SAT_SC unset}"
  local mode="${SAT_MODE:?measure_sustained: SAT_MODE unset}"
  local rep="${SAT_REP:-1}"
  local perpod=$(( (sc + pods - 1) / pods ))
  local rate="${SUS_RATE:-10}" dur="${SUS_DURATION:-90}" snap="${SUS_SNAPSHOT:-5}"
  local pb="${PAYLOAD_BYTES:-256}" sconc="${SETUP_CONCURRENCY:-64}"
  local cell_dir; cell_dir="$(_sat_cell_dir "$pods" "$rep")"; mkdir -p "$cell_dir"
  local bench_cmd="sustained --target ${T_TARGET:?} --api-style ${T_API:?} ${T_NS:-} --streams ${perpod} --rate-per-stream ${rate} --duration-secs ${dur} --snapshot-secs ${snap} --payload-bytes ${pb} --setup-concurrency ${sconc}"
  local merge_cmd="ds-bench hdr-merge --hdr-dir /merge --results-dir /merge --label-prefix sustained-"
  _run_cell_one "${mode}-sustained-n${sc}-p${pods}" "$bench_cmd" "sustained" "$merge_cmd" "$pods" "$rep" "$cell_dir"
}

# _sus_pct <merged.json> <field> — extract one percentile (e.g. p99_ms); empty if absent.
_sus_pct() {
  grep -oE "\"$2\"[: ]*[0-9.]+" "$1" 2>/dev/null | grep -oE '[0-9.]+$' | head -1
}

# run_sustained_cell <mode> <sc> <cells_json> <digest> — one fixed-rate, long
# measurement; record throughput + tail latency + server RSS peak/drift + CPU.
run_sustained_cell() {
  local mode="$1" sc="$2" cells_json="$3" digest="$4"
  export SAT_MODE="$mode" SAT_SC="$sc"
  export SUS_RATE;        SUS_RATE="$(_sat_get s 's.sustained.get("rate_per_stream",10)')"
  export SUS_DURATION;    SUS_DURATION="$(_sat_get s 's.sustained.get("duration_secs",90)')"
  export SUS_SNAPSHOT;    SUS_SNAPSHOT="$(_sat_get s 's.sustained.get("snapshot_secs",5)')"
  export PAYLOAD_BYTES;   PAYLOAD_BYTES="$(_sat_get s 's.sustained.get("payload_bytes",256)')"
  export SETUP_CONCURRENCY; SETUP_CONCURRENCY="$(_sat_get s 's.sustained.get("setup_concurrency",64)')"
  local pods; pods="$(_sat_get s 's.sustained.get("pods",1)')"
  local fn="${MEASURE_FN:-measure_sustained}"

  SAT_REP=1; reset_state "$mode" >&2
  local out cpu thr; out="$("$fn" "$pods")"; cpu="${out%% *}"; thr="${out##* }"

  local cd; cd="$(_sat_cell_dir "$pods" 1)"
  local p50 p99 p999
  p50="$(_sus_pct "$cd/merged.json" p50_ms)"
  p99="$(_sus_pct "$cd/merged.json" p99_ms)"
  p999="$(_sus_pct "$cd/merged.json" p999_ms)"
  local rss_peak rss_drift
  read -r rss_peak rss_drift < <(compute_server_mem_mb "$cd/samples.csv" 2>/dev/null || echo "0 0")

  python3 -c "
import sys; sys.path.insert(0,'scripts')
import sustained_cells
def _f(x,d=None):
    try: return float(x)
    except (ValueError, TypeError): return d
thr=_f(sys.argv[6], 0.0) or 0.0
ok = thr > 0
drift=_f(sys.argv[11], 0.0) or 0.0
stable = ok and abs(drift) < 50.0   # <50 MiB drift over the window = steady
sustained_cells.record(sys.argv[1], int(sys.argv[2]), image_digest=sys.argv[3],
  pods=int(sys.argv[4]), rate_per_stream=int(sys.argv[5]), duration_secs=int(sys.argv[12]),
  throughput=thr, p50=_f(sys.argv[7]), p99=_f(sys.argv[8]), p999=_f(sys.argv[9]),
  cpu_mean=_f(sys.argv[13], 0.0), rss_peak_mb=_f(sys.argv[10], 0.0), rss_drift_mb=drift,
  stable=bool(stable), status=('ok' if ok else 'error'),
  reason=('complete' if ok else 'creation_choke'))
" "$cells_json" "$sc" "$digest" "$pods" "${SUS_RATE}" "${thr:-0}" "${p50:-None}" "${p99:-None}" "${p999:-None}" "${rss_peak:-0}" "${rss_drift:-0}" "${SUS_DURATION}" "${cpu:-0}"
}
