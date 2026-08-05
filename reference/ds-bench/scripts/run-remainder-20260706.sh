#!/usr/bin/env bash
# Remainder v2 (post second kill): reads-catchup, reads-longpoll already complete.
# Left: reads-sse-remote → run-durable suspect-cell revalidation → run-sse → done.
set -uo pipefail
cd "$(cd "$(dirname "$0")" && pwd)"
export DS_TARGET=remote PROJECT=vaxine SKIP_BUILD=1

MAIN_LOG=/tmp/run-complete-0706.log
log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" >> "$MAIN_LOG"; }

run_suite() {
  local s="$1"
  log "START $s"
  scripts/bench "suites/$s.json" run > "/tmp/run-$s.log" 2>&1
  local rc=$?
  log "DONE $s rc=$rc"
  return $rc
}

log "=== remainder v2 starting ==="

run_suite reads-sse-remote

log "START run-durable-revalidate"
python3 - <<'EOF'
import json
p = 'results/run-durable/wal/cells.json'
d = json.load(open(p))
for k in ('100000', '200000', '500000'):
    if k in d.get('cells', {}):
        del d['cells'][k]
json.dump(d, open(p, 'w'), indent=1)
print('dropped suspect cells')
EOF
run_suite run-durable
log "DONE run-durable-revalidate"

log "START run-sse"
SKIP_BUILD=1 scripts/run-sse.sh > /tmp/run-sse.log 2>&1
log "DONE run-sse rc=$?"

log "=== remainder v2 finished ==="
mkdir -p .bench-state
touch .bench-state/run-complete-0706.done
