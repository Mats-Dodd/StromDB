#!/usr/bin/env bash
# Cluster-free test for run_catchup_cell (bootstrap-based reconnect): stub reset_state +
# MEASURE_FN, pre-create the cell's merged.json, assert the recorded cell carries the
# per-client catch-up p50/p99 + response body size (body_kb = bytes/clients/1024).
set -uo pipefail
cd "$(dirname "$0")/.."
export DS_TARGET=local KIND_CLUSTER=ds-bench
. scripts/lib-catchup.sh

SUITE_DIR="$(mktemp -d)"; export SUITE_FILE="$SUITE_DIR/suite.json"
cat > "$SUITE_FILE" <<'JSON'
{ "suite":"cu-test", "workload":"catchup", "cluster":{},
  "catchup":{ "clients":1000, "per_client_stream":true, "event_bytes":1024, "snapshot_bytes":51200, "pods":1, "setup_concurrency":64 },
  "modes":["wal"], "stream_counts":[200] }
JSON

reset_state() { :; }
measure_stub() { echo "5 0"; }   # cpu thr (metrics read from merged.json)
export MEASURE_FN=measure_stub

export SAT_RESULT_ROOT="$(mktemp -d)/cells"
cd_dir="$SAT_RESULT_ROOT/wal/n200/p1-r1"; mkdir -p "$cd_dir"
# bytes_received_total = 1000 clients × 172 KiB = 176128000 -> body_kb 172.0
cat > "$cd_dir/merged.json" <<'J'
{ "p50_ms": 120.0, "p99_ms": 253.0, "bytes_received_total": 176128000, "aggregate_mb_per_sec": 119.4, "stampede_elapsed_secs": 1.4 }
J

tmp="$(mktemp -d)/cells.json"
run_catchup_cell wal 200 "$tmp" "digestX"
python3 - "$tmp" <<'PY'
import sys, json
c = json.load(open(sys.argv[1]))["cells"]["200"]
assert c["status"] == "ok" and c["reason"] == "complete", c
assert c["p50"] == 120.0 and c["p99"] == 253.0, c
assert c["bytes_received_total"] == 176128000.0, c
assert c["body_kb"] == 172.0, c                 # 176128000 / 1000 / 1024
assert c["mb_per_sec"] == 119.4, c
assert c["clients"] == 1000 and c["snapshot_bytes"] == 51200, c
print("PASS run_catchup_cell records p50/p99 + body size")
PY

# absent/zero p99 -> status error
echo '{ "bytes_received_total": 0 }' > "$cd_dir/merged.json"
tmp2="$(mktemp -d)/cells.json"
run_catchup_cell wal 200 "$tmp2" "digestX"
python3 - "$tmp2" <<'PY'
import sys, json
c = json.load(open(sys.argv[1]))["cells"]["200"]
assert c["status"] == "error" and c["reason"] == "creation_choke", c
print("PASS run_catchup_cell flags missing latency")
PY
