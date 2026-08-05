#!/usr/bin/env bash
# Complete durable-streams benchmark matrix — 2026-07-06 build (electric main +
# PR #4690, commit 0d33d9564). Collision-safe scheduling: ≤3 clusters at once,
# suites that share a cluster name (bench-wal / bench-ursula / bench-mixed) run
# sequentially.
#
#   Wave 1 (parallel):  run-durable (bench-wal, long pole)
#                       mixed-cal → mixed-writes → mixed-writes-hot → mixed-delivery (bench-mixed)
#                       sustained (bench-sustained)
#   Wave 2:             catchup-durable (bench-cu-durable) in parallel with
#                       reads-catchup → reads-longpoll → reads-sse-remote
#                       (each needs bench-wal + bench-ursula; start after run-durable)
#   Wave 3:             run-sse.sh (bench-sse)
#
# Watchdog: arm scripts/teardown-watchdog.sh separately with
# DONE_MARKER=.bench-state/run-complete-0706.done (touched at the end here).
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

log "=== complete matrix run starting ==="

# ---- Wave 1: three disjoint clusters in parallel ----
run_suite run-durable &
PID_DURABLE=$!

( run_suite mixed-cal
  run_suite mixed-writes
  run_suite mixed-writes-hot
  run_suite mixed-delivery ) &
PID_MIXED=$!

run_suite sustained &
PID_SUSTAINED=$!

wait "$PID_MIXED"; log "wave1 mixed chain finished"
wait "$PID_SUSTAINED"; log "wave1 sustained finished"

# ---- Wave 2: catchup on its own cluster; reads need bench-wal, so wait for run-durable ----
run_suite catchup-durable &
PID_CATCHUP=$!

wait "$PID_DURABLE"; log "wave1 run-durable finished"

run_suite reads-catchup
run_suite reads-longpoll
run_suite reads-sse-remote

wait "$PID_CATCHUP"; log "wave2 catchup-durable finished"

# ---- Wave 3: SSE fan-out (bench-sse; durable + ursula + s2 comparison) ----
log "START run-sse"
SKIP_BUILD=1 scripts/run-sse.sh > /tmp/run-sse.log 2>&1
log "DONE run-sse rc=$?"

log "=== complete matrix run finished ==="
mkdir -p .bench-state
touch .bench-state/run-complete-0706.done
