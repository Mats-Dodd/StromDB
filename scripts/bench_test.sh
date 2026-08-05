#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Usage / dispatch
out="$(scripts/bench 2>&1 || true)"
echo "$out" | grep -qi "usage" || { echo "FAIL: no usage on no args"; exit 1; }

# teardown reads only the suite's recorded clusters (dry-run).
# IMPORTANT: use a THROWAWAY suite name so the test never writes/rm's the state
# file of a real suite (doing so once orphaned a live cluster from `teardown`).
mkdir -p .bench-state
TDSUITE="$(mktemp -d)/td.json"
cat > "$TDSUITE" <<'JSON'
{"suite":"td-test","modes":["wal"],"stream_counts":[1],"cluster":{},"saturation":{},"pod_ladder":{"1":[1]}}
JSON
echo '{"clusters":[{"name":"bench-wal","zone":"europe-west4-a"}]}' > .bench-state/td-test.json
out="$(BENCH_DRYRUN=1 scripts/bench "$TDSUITE" teardown)"
echo "$out" | grep -q "clusters delete bench-wal" || { echo "FAIL: teardown wrong cluster"; exit 1; }
echo "$out" | grep -q "bench-ursula" && { echo "FAIL: teardown touched a cluster it didn't create"; exit 1; }
rm -f .bench-state/td-test.json

# auto-teardown decision (dry-run): complete+clean -> teardown; error cells -> keep
TSUITE="$(mktemp -d)/mini.json"
cat > "$TSUITE" <<'JSON'
{"suite":"mini-tc","modes":["wal"],"stream_counts":[1],"cluster":{},"saturation":{},"pod_ladder":{"1":[1]}}
JSON
mkdir -p results/mini-tc/wal .bench-state
echo '{"clusters":[{"name":"bench-wal","zone":"europe-west4-a"}]}' > .bench-state/mini-tc.json
cell_ok='{"cells":{"1":{"stream_count":1,"status":"ok","saturated":true,"reason":"plateau","throughput":1,"p99":1,"pinned_pods":1,"walk":[[1,1]],"image_digest":"x"}}}'
cell_err='{"cells":{"1":{"stream_count":1,"status":"error","saturated":false,"reason":"creation_choke","throughput":0,"p99":null,"pinned_pods":null,"walk":[[1,0]],"image_digest":"x"}}}'

echo "$cell_ok" > results/mini-tc/wal/cells.json
python3 scripts/report.py "$TSUITE" >/dev/null 2>&1   # writes results/mini-tc/report.md
out="$(BENCH_DRYRUN=1 scripts/bench "$TSUITE" teardown-if-complete)"
echo "$out" | grep -q "clusters delete bench-wal" || { echo "FAIL: complete suite not torn down"; exit 1; }

echo "$cell_err" > results/mini-tc/wal/cells.json
out="$(BENCH_DRYRUN=1 scripts/bench "$TSUITE" teardown-if-complete)"
echo "$out" | grep -q "clusters delete" && { echo "FAIL: error suite torn down (should keep)"; exit 1; }

rm -rf results/mini-tc .bench-state/mini-tc.json
echo "PASS bench auto-teardown decision"
echo "PASS bench dispatch+teardown"
