#!/usr/bin/env bash
# Mixed read/write workload: writers, catch-up readers and SSE subscribers share
# the same streams so each class's latency shows the OTHERS' interference. Swept
# over `mixed.levels` along `mixed.sweep` — "readers" (fixed write load, ramp
# catch-up readers: do readers hurt writers?) or "writer_rate" (fixed subscriber
# fan-out, ramp per-writer append rate: do writes hurt delivery?) — for each
# stream_count (the cardinality axis = `mixed --streams`). One sub-cell per
# (stream_count, level); the cell is `complete` once every level is done.
# Driven by `ds-bench mixed`. S2 is excluded (no comparable catch-up read).
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$REPO_ROOT/scripts/lib-saturate.sh"   # engine + _sat_cell_dir/_sat_get/reset_state

# The coordinator merge for a mixed cell is PER-CLASS: three hdr-merge calls
# (write / fanout / read label prefixes) composed into one JSON doc, plus the raw
# per-pod MixedResults (for counts + elapsed → read/delivery rates). Only the
# write call scans --results-dir (aggregate_ops_per_sec = summed write ops/s).
# $f/$sep survive envsubst (not in its var list) and expand in the coordinator.
MIXED_MERGE_CMD="printf '{\"write\":'; ds-bench hdr-merge --hdr-dir /merge --results-dir /merge --label-prefix mixed-write-; printf ',\"fanout\":'; ds-bench hdr-merge --hdr-dir /merge --label-prefix mixed-fanout-; printf ',\"read\":'; ds-bench hdr-merge --hdr-dir /merge --label-prefix mixed-read-; printf ',\"pods\":['; sep=; for f in /merge/mixed-*.json; do [ -e \"\$f\" ] || continue; printf '%s' \"\$sep\"; cat \"\$f\"; sep=,; done; printf ']}'"

# measure_mixed <pods> — one mixed run. MIXED_SC carries the stream count, MX_*
# the resolved knobs for this level. Exposes MIXED_BENCH_CMD for tests.
measure_mixed() {
  local pods="$1"
  local sc="${MIXED_SC:?measure_mixed: MIXED_SC unset}"
  local mode="${SAT_MODE:?measure_mixed: SAT_MODE unset}"
  local rep="${SAT_REP:-1}"
  local cell_dir; cell_dir="$(_sat_cell_dir "$pods" "$rep")"; mkdir -p "$cell_dir"
  MIXED_BENCH_CMD="mixed --target ${T_TARGET:?} --api-style ${T_API:?} ${T_NS:-} --streams ${sc} --writers-per-stream ${MX_WPS} --writer-rate ${MX_RATE} --readers ${MX_READERS} --read-rate ${MX_READRATE} --read-interval-ms ${MX_READINT} --backfill-events ${MX_BACKFILL} --subscribers ${MX_SUBS} --duration-secs ${MX_DURATION} --payload-bytes ${MX_PAYLOAD} --setup-concurrency ${MX_SETUP}"
  _run_cell_one "${mode}-mixed-n${sc}-l${MIXED_LEVEL}" "$MIXED_BENCH_CMD" "mixed" "$MIXED_MERGE_CMD" "$pods" "$rep" "$cell_dir"
}

# record_mixed_cell — thin wrapper so tests can stub recording. Positional:
# cells_json sc level digest merged_path readers subscribers writer_rate read_rate.
record_mixed_cell() {
  python3 -c "
import sys; sys.path.insert(0,'scripts')
import mixed_cells
st = mixed_cells.record_merged(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]),
    image_digest=sys.argv[4], merged_path=sys.argv[5], readers=int(sys.argv[6]),
    subscribers=int(sys.argv[7]), writer_rate=int(sys.argv[8]), read_rate=int(sys.argv[9]))
print(st)
" "$@"
}

# run_mixed_cell <mode> <stream_count> <cells_json> <digest>
run_mixed_cell() {
  local mode="$1" sc="$2" cells_json="$3" digest="$4"
  export SAT_MODE="$mode" MIXED_SC="$sc"
  local axis;  axis="$(_sat_get s 's.mixed.get("sweep","readers")')"
  export MX_WPS;      MX_WPS="$(_sat_get s 's.mixed.get("writers_per_stream",1)')"
  export MX_DURATION; MX_DURATION="$(_sat_get s 's.mixed.get("duration_secs",20)')"
  export MX_PAYLOAD;  MX_PAYLOAD="$(_sat_get s 's.mixed.get("payload_bytes",256)')"
  export MX_SETUP;    MX_SETUP="$(_sat_get s 's.mixed.get("setup_concurrency",16)')"
  export MX_READRATE; MX_READRATE="$(_sat_get s 's.mixed.get("read_rate",0)')"
  export MX_READINT;  MX_READINT="$(_sat_get s 's.mixed.get("read_interval_ms",0)')"
  export MX_BACKFILL; MX_BACKFILL="$(_sat_get s 's.mixed.get("backfill_events",200)')"
  local fixed_rate;    fixed_rate="$(_sat_get s 's.mixed.get("writer_rate",50)')"
  local fixed_readers; fixed_readers="$(_sat_get s 's.mixed.get("readers",0)')"
  local fixed_subs;    fixed_subs="$(_sat_get s 's.mixed.get("subscribers",0)')"
  local levels; levels="$(_sat_get s "' '.join(map(str, s.mixed.get('levels',[0])))")"
  local pods;   pods="$(_sat_get s 's.mixed.get("pods",1)')"
  local fn="${MEASURE_FN:-measure_mixed}"
  # Setup (create streams + backfill) precedes the measured window inside one
  # fleet run, so give the Job room beyond the drive duration. High-cardinality
  # cells (streams × backfill appends) override via mixed.fleet_timeout_secs.
  export FLEET_TIMEOUT
  FLEET_TIMEOUT="$(_sat_get s 's.mixed.get("fleet_timeout_secs", s.mixed.get("duration_secs",20) + 180)')"

  local level
  for level in $levels; do
    if [ "$(python3 -c "import sys;sys.path.insert(0,'scripts');import mixed_cells;print(mixed_cells.level_status('$cells_json',$sc,$level,'$digest'))")" = "done" ]; then
      echo "[mixed $mode n$sc l$level] already done, skip" >&2; continue
    fi
    export MIXED_LEVEL="$level"
    case "$axis" in
      readers)     export MX_READERS="$level" MX_RATE="$fixed_rate" MX_SUBS="$fixed_subs" ;;
      writer_rate) export MX_RATE="$level" MX_READERS="$fixed_readers" MX_SUBS="$fixed_subs" ;;
      *) echo "run_mixed_cell: unknown sweep axis '$axis'" >&2; return 2 ;;
    esac
    # Encode the level into SAT_SC so each (sc,level) gets its OWN cell dir
    # (.../n<sc>-l<level>/p1-r1) — MIXED_SC stays the bare int for --streams.
    export SAT_SC="${sc}-l${level}"
    SAT_REP=1; reset_state "$mode" >&2
    "$fn" "$pods" >/dev/null   # progress on stderr; metrics parsed from merged.json

    local cd st
    cd="$(_sat_cell_dir "$pods" 1)"
    st="$(record_mixed_cell "$cells_json" "$sc" "$level" "$digest" "$cd/merged.json" "$MX_READERS" "$MX_SUBS" "$MX_RATE" "$MX_READRATE")"
    echo "[mixed $mode n$sc l$level] recorded status=$st" >&2
  done

  python3 -c "import sys;sys.path.insert(0,'scripts');import mixed_cells;mixed_cells.mark_complete('$cells_json',$sc,'$digest')"
}
