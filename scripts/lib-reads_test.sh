#!/usr/bin/env bash
# Verifies run_reads_cell builds the expected `reads` bench_cmd per connection
# level. We let measure_reads ACTUALLY RUN (that is where READS_BENCH_CMD is built)
# and stub the engine boundary `_run_cell_one` to capture its bench_cmd arg ($2).
# No cluster needed.
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CAPTURE="$(mktemp)"; export CAPTURE
export DS_TARGET=local KIND_CLUSTER=ds-bench
export T_TARGET="http://durable-streams:4438" T_API="durable" T_NS=""

# shellcheck source=scripts/lib-reads.sh
. "$REPO_ROOT/scripts/lib-reads.sh"

# Stub the engine AFTER sourcing lib-reads (which sources lib-saturate). The real
# measure_reads runs and calls _run_cell_one "<name>" "<bench_cmd>" "reads" ...;
# capturing $2 gives us the exact command under test.
_run_cell_one() { echo "$2" >> "$CAPTURE"; }
_sat_cell_dir() { echo "/tmp/reads-test-cell"; }
reset_state() { :; }
# _sat_get returns suite fields; stub the ones run_reads_cell reads.
_sat_get() {
  case "$2" in
    *connection_levels*) echo "8 32" ;;
    *read_size_bytes*)   echo "4096" ;;
    *seed_bytes*)        echo "1048576" ;;
    *duration_secs*)     echo "10" ;;
    *warmup_secs*)       echo "2" ;;
    *settle_secs*)       echo "1" ;;
    *pods*)              echo "1" ;;
    *) echo "" ;;
  esac
}

# Make recording a no-op and the merged.json reads return empty (the store + field
# extraction are tested separately). The per-level skip check is a python subprocess
# (`reads_cells.conn_status`), so we can't stub it from the shell — instead start
# from a clean cells file so every level is "absent" and therefore runs.
record_reads_cell() { :; }
_rd_field() { echo ""; }   # no merged.json in this unit test

mkdir -p /tmp/reads-test-cell
rm -f /tmp/reads-cells.json
run_reads_cell "wal" "100" "/tmp/reads-cells.json" "digestX" >/dev/null 2>&1

fail=0
grep -q -- "--streams 100 --connections 8 " "$CAPTURE"  || { echo "FAIL: missing conn=8 cmd"; fail=1; }
grep -q -- "--streams 100 --connections 32 " "$CAPTURE" || { echo "FAIL: missing conn=32 cmd"; fail=1; }
grep -q -- "--read-size-bytes 4096 --seed-bytes 1048576" "$CAPTURE" || { echo "FAIL: payload/seed args"; fail=1; }
grep -q -- "--warmup-secs 2 --settle-secs 1" "$CAPTURE"  || { echo "FAIL: warmup/settle args"; fail=1; }
[ "$fail" = 0 ] && echo "PASS: lib-reads builds expected bench_cmds" || exit 1
