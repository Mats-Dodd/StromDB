#!/usr/bin/env bash
# Catch-up / reconnect benchmark, reproducing Ursula's published methodology
# (https://ursula.tonbo.io/benchmark): N clients each reconnect to their OWN
# pre-populated stream and catch up via each system's NATIVE replay path —
# ursula GET /bootstrap (snapshot+tail), durable offset=-1 (full log), s2
# /records?seq_num=0 (full log). Driven by `ds-bench bootstrap --per-client-stream`
# (the backend's replay_request_for picks the per-system path; s2 IS supported here,
# unlike the one-shared-stream `catch-up` subcommand). Metric: per-client catch-up
# p50/p99 + response body size. One measurement per pre_events value (no ladder).
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$REPO_ROOT/scripts/lib-saturate.sh"   # engine + _sat_cell_dir/_sat_get/reset_state

# measure_catchup <pods> -> "cpu_pct thr". SAT_SC carries pre_events. Overridable via MEASURE_FN.
measure_catchup() {
  local pods="$1"
  local pre="${SAT_SC:?measure_catchup: SAT_SC unset}"
  local mode="${SAT_MODE:?measure_catchup: SAT_MODE unset}"
  local rep="${SAT_REP:-1}"
  local clients="${CU_CLIENTS:-1000}" eb="${CU_EVENT_BYTES:-1024}" sb="${CU_SNAPSHOT_BYTES:-0}" sconc="${SETUP_CONCURRENCY:-64}"
  local pcs=""; [ "${CU_PER_CLIENT_STREAM:-true}" = "true" ] && pcs="--per-client-stream"
  local cell_dir; cell_dir="$(_sat_cell_dir "$pods" "$rep")"; mkdir -p "$cell_dir"
  local bench_cmd="bootstrap --target ${T_TARGET:?} --api-style ${T_API:?} ${T_NS:-} --clients ${clients} ${pcs} --pre-events ${pre} --event-bytes ${eb} --snapshot-bytes ${sb} --setup-concurrency ${sconc}"
  local merge_cmd="ds-bench hdr-merge --hdr-dir /merge --results-dir /merge --label-prefix bootstrap-"
  _run_cell_one "${mode}-catchup-pe${pre}-p${pods}" "$bench_cmd" "catchup" "$merge_cmd" "$pods" "$rep" "$cell_dir"
}

_cu_field() { grep -oE "\"$2\"[: ]*[0-9.]+" "$1" 2>/dev/null | grep -oE '[0-9.]+$' | head -1; }

# run_catchup_cell <mode> <pre_events> <cells_json> <digest>
run_catchup_cell() {
  local mode="$1" pre="$2" cells_json="$3" digest="$4"
  export SAT_MODE="$mode" SAT_SC="$pre"
  export CU_CLIENTS;            CU_CLIENTS="$(_sat_get s 's.catchup.get("clients",1000)')"
  export CU_EVENT_BYTES;        CU_EVENT_BYTES="$(_sat_get s 's.catchup.get("event_bytes",1024)')"
  export CU_SNAPSHOT_BYTES;     CU_SNAPSHOT_BYTES="$(_sat_get s 's.catchup.get("snapshot_bytes",0)')"
  export CU_PER_CLIENT_STREAM;  CU_PER_CLIENT_STREAM="$(_sat_get s 's.catchup.get("per_client_stream",True) and "true" or "false"')"
  export SETUP_CONCURRENCY;     SETUP_CONCURRENCY="$(_sat_get s 's.catchup.get("setup_concurrency",64)')"
  local pods; pods="$(_sat_get s 's.catchup.get("pods",1)')"
  local fn="${MEASURE_FN:-measure_catchup}"

  SAT_REP=1; reset_state "$mode" >&2
  "$fn" "$pods" >/dev/null   # progress on stderr; metrics from merged.json below

  local cd; cd="$(_sat_cell_dir "$pods" 1)"
  local p50 p99 bytes mbps
  p50="$(_cu_field "$cd/merged.json" p50_ms)"
  p99="$(_cu_field "$cd/merged.json" p99_ms)"
  bytes="$(_cu_field "$cd/merged.json" bytes_received_total)"
  mbps="$(_cu_field "$cd/merged.json" aggregate_mb_per_sec)"   # total replay throughput (MiB/s)

  python3 -c "
import sys; sys.path.insert(0,'scripts')
import catchup_cells
def _f(x,d=None):
    try: return float(x)
    except (ValueError, TypeError): return d
p99=_f(sys.argv[6]); ok = p99 is not None and p99 > 0
bt=_f(sys.argv[7]); clients=int(sys.argv[4])
body_kb = round(bt/clients/1024.0, 1) if (bt and clients) else None
catchup_cells.record(sys.argv[1], int(sys.argv[2]), image_digest=sys.argv[3],
  clients=clients, event_bytes=int(sys.argv[8]), snapshot_bytes=int(sys.argv[9]), pods=int(sys.argv[10]),
  p50=_f(sys.argv[5]), p99=p99, bytes_received_total=bt, body_kb=body_kb, mb_per_sec=_f(sys.argv[11]),
  status=('ok' if ok else 'error'), reason=('complete' if ok else 'creation_choke'))
" "$cells_json" "$pre" "$digest" "${CU_CLIENTS}" "${p50:-None}" "${p99:-None}" "${bytes:-None}" "${CU_EVENT_BYTES}" "${CU_SNAPSHOT_BYTES}" "${pods}" "${mbps:-None}"
}
