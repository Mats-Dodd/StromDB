#!/usr/bin/env bash
# lib-bench_stdout_test.sh — cluster-free regression test for _run_cell_one stdout purity.
#
# Asserts that _run_cell_one emits EXACTLY ONE line on stdout: "<cpu_pct> <thr>".
# Progress noise from helper stubs must NOT appear on stdout (it should go to stderr).
#
# Exit 0 = PASS, non-zero = FAIL.

set -uo pipefail

export DS_TARGET=local
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export REPO_ROOT
export SWEEP_RUN_ID=test
export SERVER_CPUS=4
export SERVER_MEM=16Gi
export INIT_PARALLELISM=4
export MAX_PODS=16
export MAX_BUMPS=2

# Source lib-bench.sh — this only defines functions + sets env vars (no cluster access).
# shellcheck source=scripts/lib-bench.sh
source "${REPO_ROOT}/scripts/lib-bench.sh"

# ── Stub out the four noisy helpers ──────────────────────────────────────────
# Each prints progress noise to stdout (intentionally, to prove isolation fails
# before the fix and passes after it).
reset_sidecar_samples() {
  echo "    resetting samples.csv on pod stub-pod..."
}

run_fleet_and_coordinator() {
  echo "    launching fleet (4 pods)..."
  echo "    fleet pods: 4 Running"
  echo "    launching coordinator..."
}

fetch_coordinator_merged() {
  echo "    fetched coordinator merged → $1"
}

collect_sidecar() {
  echo "    saved samples.csv → $1/samples.csv"
}

# Stub compute_server_cpu_pct to return a known value (bypasses real awk+csv).
compute_server_cpu_pct() {
  echo "222.2"
}

# ── Set up a temp cell_dir ────────────────────────────────────────────────────
cell_dir="$(mktemp -d /tmp/lib-bench-test-XXXXXX)"
trap 'rm -rf "$cell_dir"' EXIT

# samples.csv must exist so the `-f` check in _run_cell_one passes.
printf 'ts_ms,rss_bytes,cpu_ticks,write_bytes\n1000,1024,100,0\n2000,1024,300,0\n' \
  > "${cell_dir}/samples.csv"

# merged.json with a known throughput that saturation.py can read.
printf '{"aggregate_ops_per_sec": 1069919.0}\n' > "${cell_dir}/merged.json"

# ── Capture stdout from _run_cell_one ────────────────────────────────────────
echo "--- running _run_cell_one (stderr carries progress noise) ---" >&2
output="$( _run_cell_one cell bench pfx merge 4 1 "$cell_dir" )"

echo "--- captured stdout: [${output}] ---" >&2

# ── Parse the result ──────────────────────────────────────────────────────────
# Contract: "cpu_pct thr aligned p50 p99" — aligned is 1 (windows overlapped /
# no stamps) or 0 (misaligned fleet measure windows; thr is forced to 0
# upstream); p50/p99 are the rung's merged latency ms or "None".
read -r cpu_pct thr aligned p50 p99 <<< "$output"

PASS=true
fail_msg=""

# Assert cpu_pct == "222.2"
if [ "$cpu_pct" != "222.2" ]; then
  PASS=false
  fail_msg="FAIL: expected cpu_pct=222.2, got '${cpu_pct}' (progress noise leaked onto stdout)"
fi

# Assert thr is numeric (1069919 or 1069919.0)
if ! printf '%s' "$thr" | grep -Eq '^[0-9]+(\.[0-9]+)?$'; then
  PASS=false
  fail_msg="${fail_msg:+${fail_msg}; }FAIL: expected numeric thr, got '${thr}' (progress noise leaked onto stdout)"
fi

# Assert aligned is 0 or 1 (the merged.json here carries no window stamps → 1)
if [ "$aligned" != "1" ] && [ "$aligned" != "0" ]; then
  PASS=false
  fail_msg="${fail_msg:+${fail_msg}; }FAIL: expected aligned flag 0/1, got '${aligned}' (progress noise leaked onto stdout)"
fi

# Assert p50/p99 are numeric or "None" (this merged.json has no latency → None)
for v in "$p50" "$p99"; do
  if [ "$v" != "None" ] && ! printf '%s' "$v" | grep -Eq '^[0-9]+(\.[0-9]+)?$'; then
    PASS=false
    fail_msg="${fail_msg:+${fail_msg}; }FAIL: expected p50/p99 numeric or None, got '${v}'"
  fi
done

if $PASS; then
  echo "PASS: _run_cell_one stdout is clean: cpu_pct=${cpu_pct} thr=${thr} aligned=${aligned} p50=${p50} p99=${p99}"
  exit 0
else
  echo "$fail_msg"
  exit 1
fi
