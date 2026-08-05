#!/usr/bin/env bash
# Barrier confirmation run: run-durable wal ladder (100 -> 500k streams) on the
# barrier-enabled ds-bench:dev image. Success criteria: barrier release lines per
# cell, windows_aligned=true in merged.json, and physically-plausible ceilings at
# 200k/500k (or error/misaligned_windows — never a silent inflated number).
set -uo pipefail
cd "$(cd "$(dirname "$0")" && pwd)"
export DS_TARGET=remote PROJECT=vaxine SKIP_BUILD=1

MAIN_LOG=/tmp/run-complete-0706.log
log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" >> "$MAIN_LOG"; }

log "=== barrier confirmation run starting ==="
log "START run-durable(barrier)"
scripts/bench suites/run-durable.json run > /tmp/run-run-durable.log 2>&1
log "DONE run-durable(barrier) rc=$?"
log "=== barrier confirmation run finished ==="
mkdir -p .bench-state
touch .bench-state/barrier-confirm.done
