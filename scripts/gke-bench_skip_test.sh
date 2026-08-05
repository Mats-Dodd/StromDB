#!/usr/bin/env bash
# gke-bench_skip_test.sh — cluster-free unit test for the read-path variant skip filter.
# Tests the skip_read_filter function that mirrors the guard in gke-bench.sh matrix loop.
# Exit 0 = PASS, non-zero = FAIL.
set -uo pipefail

PASS=true

# returns 0 (skip) / 1 (run) — mirrors the guard in gke-bench.sh matrix loop
skip_read_filter() {  # sys wl var
  local sys="$1" wl="$2" var="$3"
  [ "$sys" = durable ] && [ "$wl" != write ] && [ "$wl" != sustained ] && [ "$var" != fast ] && return 0
  return 1
}

check() {  # sys wl var expected_behavior
  local sys="$1" wl="$2" var="$3" expected="$4"
  local result
  if skip_read_filter "$sys" "$wl" "$var"; then
    result="SKIP"
  else
    result="RUN"
  fi
  if [ "$result" != "$expected" ]; then
    echo "FAIL [$sys $wl $var]: expected $expected, got $result"; PASS=false
  else
    echo "ok [$sys $wl $var]: $result"
  fi
}

# Assert test cases (skip=SKIP, run=RUN):
# The bug: sustained must NOT be skipped on durable:strict/wal
check durable sustained strict RUN
check durable sustained wal RUN
check durable sustained fast RUN

# write always runs on any durable variant
check durable write strict RUN

# read paths (sse/replay) are skipped on durable non-fast variants
check durable sse strict SKIP
check durable replay wal SKIP

# fast variant always runs (survives the filter)
check durable sse fast RUN

# non-durable systems: filter only applies to durable; sustained would be blocked by supports() separately
check ursula sustained memory RUN

$PASS && { echo "PASS"; exit 0; } || { echo "FAILED"; exit 1; }
