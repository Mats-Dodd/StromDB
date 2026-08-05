#!/usr/bin/env bash
# Cluster-free test for run_sustained_cell: stub reset_state + MEASURE_FN, pre-create
# the cell's merged.json + samples.csv, and assert the recorded sustained cell carries
# the right throughput / latency / RSS-drift / status.
set -uo pipefail
cd "$(dirname "$0")/.."
export DS_TARGET=local KIND_CLUSTER=ds-bench
. scripts/lib-sustained.sh

# self-contained sustained suite fixture
SUITE_DIR="$(mktemp -d)"; export SUITE_FILE="$SUITE_DIR/suite.json"
cat > "$SUITE_FILE" <<'JSON'
{ "suite":"sus-test", "workload":"sustained", "cluster":{},
  "sustained":{ "rate_per_stream":10, "duration_secs":90, "pods":1, "payload_bytes":256 },
  "modes":["wal"], "stream_counts":[10] }
JSON

# stubs: reset_state is a no-op; MEASURE_FN returns "cpu thr" without touching a cluster
reset_state() { :; }
measure_stub() { echo "7 1500"; }
export MEASURE_FN=measure_stub

# results land under SAT_RESULT_ROOT/<mode>/n<sc>/p1-r1 — pre-create the artifacts there
export SAT_RESULT_ROOT="$(mktemp -d)/cells"
cd_dir="$SAT_RESULT_ROOT/wal/n10/p1-r1"; mkdir -p "$cd_dir"
cat > "$cd_dir/merged.json" <<'J'
{ "p50_ms": 1.20, "p99_ms": 8.00, "p999_ms": 20.0, "aggregate_ops_per_sec": 1500 }
J
# samples.csv: ts_ms,rss_bytes,cpu_ticks,write_bytes — RSS 10 MiB -> 12 MiB (peak 12, drift 2)
cat > "$cd_dir/samples.csv" <<'C'
ts_ms,rss_bytes,cpu_ticks,write_bytes
1000,10485760,100,0
2000,11534336,150,0
3000,12582912,200,0
C

tmp="$(mktemp -d)/cells.json"
run_sustained_cell wal 10 "$tmp" "digestX"

python3 - "$tmp" <<'PY'
import sys, json
c = json.load(open(sys.argv[1]))["cells"]["10"]
assert c["status"] == "ok" and c["reason"] == "complete", c
assert c["throughput"] == 1500.0, c
assert c["p50"] == 1.2 and c["p99"] == 8.0 and c["p999"] == 20.0, c
assert c["rss_peak_mb"] == 12.0, c          # max rss / MiB
assert c["rss_drift_mb"] == 2.0, c          # (last-first)/MiB
assert c["cpu_mean"] == 7.0, c              # from the stub's "7 1500"
assert c["stable"] is True, c               # drift 2 MiB < 50
print("PASS run_sustained_cell records throughput/latency/RSS")
PY

# A zero-throughput (choked) measurement must record status=error, not crash.
measure_choke() { echo "0 0"; }
export MEASURE_FN=measure_choke
tmp2="$(mktemp -d)/cells.json"
run_sustained_cell wal 10 "$tmp2" "digestX"
python3 - "$tmp2" <<'PY'
import sys, json
c = json.load(open(sys.argv[1]))["cells"]["10"]
assert c["status"] == "error" and c["reason"] == "creation_choke", c
assert c["stable"] is False, c
print("PASS run_sustained_cell flags choked cell")
PY
