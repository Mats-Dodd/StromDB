#!/usr/bin/env bash
# Verifies run_mixed_cell builds the expected `mixed` bench_cmd per sweep level,
# on BOTH axes (readers / writer_rate). We let measure_mixed ACTUALLY RUN (that
# is where MIXED_BENCH_CMD is built) and stub the engine boundary _run_cell_one
# to capture its bench_cmd arg ($2). No cluster needed.
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CAPTURE="$(mktemp)"; export CAPTURE
export DS_TARGET=local KIND_CLUSTER=ds-bench
export T_TARGET="http://durable-streams:4438" T_API="durable" T_NS=""

# shellcheck source=scripts/lib-mixed.sh
. "$REPO_ROOT/scripts/lib-mixed.sh"

# Stub the engine AFTER sourcing lib-mixed (which sources lib-saturate).
_run_cell_one() { echo "$2" >> "$CAPTURE"; }
_sat_cell_dir() { echo "/tmp/mixed-test-cell"; }
reset_state() { :; }
record_mixed_cell() { echo ok; }

# Axis 1: sweep readers at a fixed writer_rate + subscribers.
MIXED_AXIS="readers"
_sat_get() {
  case "$2" in
    *sweep*)              echo "$MIXED_AXIS" ;;
    *levels*)             echo "0 16" ;;
    *writers_per_stream*) echo "1" ;;
    *read_interval_ms*)   echo "0" ;;
    *backfill_events*)    echo "200" ;;
    *fleet_timeout*)      echo "300" ;;
    *read_rate*)          echo "2" ;;
    *writer_rate*)        echo "40" ;;
    *\"readers\"*)        echo "8" ;;
    *subscribers*)        echo "5" ;;
    *duration_secs*)      echo "20" ;;
    *payload_bytes*)      echo "256" ;;
    *setup_concurrency*)  echo "16" ;;
    *pods*)               echo "1" ;;
    *) echo "" ;;
  esac
}

mkdir -p /tmp/mixed-test-cell
rm -f /tmp/mixed-cells.json
run_mixed_cell "wal" "50" "/tmp/mixed-cells.json" "digestX" >/dev/null 2>&1

fail=0
grep -q -- "--streams 50 --writers-per-stream 1 --writer-rate 40 --readers 0 --read-rate 2 --read-interval-ms 0 --backfill-events 200 --subscribers 5 " "$CAPTURE" \
  || { echo "FAIL: readers axis, level 0 cmd"; fail=1; }
grep -q -- "--writer-rate 40 --readers 16 --read-rate 2 --read-interval-ms 0 --backfill-events 200 --subscribers 5 " "$CAPTURE" \
  || { echo "FAIL: readers axis, level 16 cmd"; fail=1; }

# Axis 2: sweep writer_rate at fixed subscribers (readers pinned to 0).
: > "$CAPTURE"
MIXED_AXIS="writer_rate"
_sat_get() {
  case "$2" in
    *sweep*)              echo "$MIXED_AXIS" ;;
    *levels*)             echo "10 0" ;;
    *writers_per_stream*) echo "1" ;;
    *read_interval_ms*)   echo "30000" ;;
    *backfill_events*)    echo "50" ;;
    *fleet_timeout*)      echo "600" ;;
    *read_rate*)          echo "0" ;;
    *writer_rate*)        echo "40" ;;
    *\"readers\"*)        echo "0" ;;
    *subscribers*)        echo "100" ;;
    *duration_secs*)      echo "20" ;;
    *payload_bytes*)      echo "256" ;;
    *setup_concurrency*)  echo "16" ;;
    *pods*)               echo "1" ;;
    *) echo "" ;;
  esac
}
rm -f /tmp/mixed-cells.json
run_mixed_cell "wal" "50" "/tmp/mixed-cells.json" "digestX" >/dev/null 2>&1

grep -q -- "--writer-rate 10 --readers 0 --read-rate 0 --read-interval-ms 30000 --backfill-events 50 --subscribers 100 " "$CAPTURE" \
  || { echo "FAIL: writer_rate axis, level 10 cmd"; fail=1; }
grep -q -- "--writer-rate 0 --readers 0 --read-rate 0 --read-interval-ms 30000 --backfill-events 50 --subscribers 100 " "$CAPTURE" \
  || { echo "FAIL: writer_rate axis, level 0 (max) cmd"; fail=1; }
grep -q -- "--duration-secs 20 --payload-bytes 256 --setup-concurrency 16" "$CAPTURE" \
  || { echo "FAIL: fixed knob args"; fail=1; }

[ "$fail" = 0 ] && echo "PASS: lib-mixed builds expected bench_cmds on both axes" || exit 1
