#!/usr/bin/env bash
# Read-scalability workload: sustained hot-resident reads of N seeded streams,
# swept over connection levels (the load axis) for each stream_count (the
# cardinality axis). Readers are pinned to stream `idx % N`. One sub-cell per
# (stream_count, connections); the cell is `complete` once every level is done.
# Driven by `ds-bench reads`. S2 is excluded (paginated JSON read is incomparable).
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$REPO_ROOT/scripts/lib-saturate.sh"   # engine + _sat_cell_dir/_sat_get/reset_state

# measure_reads <pods> -> "cpu_pct thr". READS_CONN carries the connection level,
# READS_SC the stream_count; RD_* carry the suite knobs. Exposes the built command
# as READS_BENCH_CMD so tests can assert it. Overridable via MEASURE_FN.
measure_reads() {
  local pods="$1"
  local sc="${READS_SC:?measure_reads: READS_SC unset}"
  local conn="${READS_CONN:?measure_reads: READS_CONN unset}"
  local mode="${SAT_MODE:?measure_reads: SAT_MODE unset}"
  local rep="${SAT_REP:-1}"
  local cell_dir; cell_dir="$(_sat_cell_dir "$pods" "$rep")"; mkdir -p "$cell_dir"
  READS_BENCH_CMD="reads --mode ${RD_MODE:-catchup} --target ${T_TARGET:?} --api-style ${T_API:?} ${T_NS:-} --streams ${sc} --connections ${conn} --read-size-bytes ${RD_READ_SIZE} --seed-bytes ${RD_SEED} --append-rate-per-sec ${RD_APPEND_RATE:-50} --duration-secs ${RD_DURATION} --warmup-secs ${RD_WARMUP} --settle-secs ${RD_SETTLE}"
  local merge_cmd="ds-bench hdr-merge --hdr-dir /merge --results-dir /merge --label-prefix reads-"
  _run_cell_one "${mode}-reads-n${sc}-c${conn}" "$READS_BENCH_CMD" "reads" "$merge_cmd" "$pods" "$rep" "$cell_dir"
}

_rd_field() { grep -oE "\"$2\"[: ]*-?[0-9.]+" "$1" 2>/dev/null | grep -oE '\-?[0-9.]+$' | head -1; }

# record_reads_cell — thin wrapper so tests can stub recording. Args positional to
# avoid a fragile here-doc: path sc conn digest ops bps p50 p99 bp err.
record_reads_cell() {
  python3 -c "
import sys; sys.path.insert(0,'scripts')
import reads_cells
def _f(x,d=None):
    try: return float(x)
    except (ValueError, TypeError): return d
def _i(x,d=0):
    try: return int(float(x))
    except (ValueError, TypeError): return d
p99=_f(sys.argv[8]); ok = p99 is not None and p99 > 0
reads_cells.record(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), image_digest=sys.argv[4],
  ops_per_sec=_f(sys.argv[5]), bytes_per_sec=_f(sys.argv[6]), p50=_f(sys.argv[7]), p99=p99,
  backpressure=_i(sys.argv[9]), other_err=_i(sys.argv[10]),
  status=('ok' if ok else 'error'), reason=('complete' if ok else 'no_reads'))
" "$@"
}

# run_reads_cell <mode> <stream_count> <cells_json> <digest>
run_reads_cell() {
  local mode="$1" sc="$2" cells_json="$3" digest="$4"
  export SAT_MODE="$mode" READS_SC="$sc"
  export RD_MODE;      RD_MODE="$(_sat_get s 's.reads.get("mode","catchup")')"
  export RD_APPEND_RATE; RD_APPEND_RATE="$(_sat_get s 's.reads.get("append_rate_per_sec",50)')"
  export RD_READ_SIZE; RD_READ_SIZE="$(_sat_get s 's.reads.get("read_size_bytes",4096)')"
  export RD_SEED;      RD_SEED="$(_sat_get s 's.reads.get("seed_bytes",16777216)')"
  export RD_DURATION;  RD_DURATION="$(_sat_get s 's.reads.get("duration_secs",60)')"
  export RD_WARMUP;    RD_WARMUP="$(_sat_get s 's.reads.get("warmup_secs",0)')"
  export RD_SETTLE;    RD_SETTLE="$(_sat_get s 's.reads.get("settle_secs",0)')"
  local conns; conns="$(_sat_get s "' '.join(map(str, s.reads.get('connection_levels',[8])))")"
  local pods;  pods="$(_sat_get s 's.reads.get("pods",1)')"
  local fn="${MEASURE_FN:-measure_reads}"

  local conn
  for conn in $conns; do
    if [ "$(python3 -c "import sys;sys.path.insert(0,'scripts');import reads_cells;print(reads_cells.conn_status('$cells_json',$sc,$conn,'$digest'))")" = "done" ]; then
      echo "[reads $mode n$sc c$conn] already done, skip" >&2; continue
    fi
    export READS_CONN="$conn"
    # _sat_cell_dir (lib-saturate) builds the cell path from $SAT_SC under `set -u`.
    # Encode the connection level into SAT_SC so each (sc,conn) gets its OWN cell dir
    # (.../n<sc>-c<conn>/p1-r1) — otherwise every connection level would overwrite the
    # same merged.json. READS_SC stays the bare integer used for `--streams`.
    export SAT_SC="${sc}-c${conn}"
    SAT_REP=1; reset_state "$mode" >&2
    "$fn" "$pods" >/dev/null   # progress on stderr; metrics from merged.json below

    local cd ops bps p50 p99 bp err
    cd="$(_sat_cell_dir "$pods" 1)"
    ops="$(_rd_field "$cd/merged.json" aggregate_ops_per_sec)"
    bps="$(_rd_field "$cd/merged.json" bytes_per_sec)"
    p50="$(_rd_field "$cd/merged.json" p50_ms)"
    p99="$(_rd_field "$cd/merged.json" p99_ms)"
    bp="$(_rd_field  "$cd/merged.json" backpressure_total)"
    err="$(_rd_field "$cd/merged.json" other_err_total)"
    record_reads_cell "$cells_json" "$sc" "$conn" "$digest" "${ops:-0}" "${bps:-0}" "${p50:-None}" "${p99:-None}" "${bp:-0}" "${err:-0}"
  done

  python3 -c "import sys;sys.path.insert(0,'scripts');import reads_cells;reads_cells.mark_complete('$cells_json',$sc,'$digest')"
}
