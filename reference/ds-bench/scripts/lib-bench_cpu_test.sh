#!/usr/bin/env bash
# lib-bench_cpu_test.sh — cluster-free unit test for compute_server_cpu_pct,
# including measure-window scoping (cpu% must reflect the LOADED window, not the
# whole cell diluted by idle setup/warmup/upload). Exit 0 = PASS.
set -uo pipefail

export DS_TARGET=local
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"; export REPO_ROOT
# shellcheck source=scripts/lib-bench.sh
source "${REPO_ROOT}/scripts/lib-bench.sh"

tmp="$(mktemp -d /tmp/lib-bench-cpu-XXXXXX)"
trap 'rm -rf "$tmp"' EXIT
PASS=true

# CLK_TCK=100. Rows: ts_ms,rss,cpu_ticks,write,ws. cpu_ticks is cumulative.
# ts=1000 c=0 (setup, idle), 2000 c=50, 3000 c=100 (measure start),
# 4000 c=400, 5000 c=700 (measure end).
CSV=$'ts_ms,rss_bytes,cpu_ticks,write_bytes,pod_ws_bytes\n1000,0,0,0,0\n2000,0,50,0,0\n3000,0,100,0,0\n4000,0,400,0,0\n5000,0,700,0,0\n'
printf '%s' "$CSV" > "${tmp}/s.csv"

check() {  # name got expected
  if [ "$2" != "$3" ]; then echo "FAIL [$1]: expected '$3', got '$2'"; PASS=false
  else echo "ok [$1]: $2"; fi
}

# Whole cell (no window): Δticks=700 over 4s → (700/100)/4*100 = 175.0
check whole_cell "$(compute_server_cpu_pct "${tmp}/s.csv")" "175.0"

# Windowed to the measure interval [3000,5000]: rows c=100..700 over 2s →
# (600/100)/2*100 = 300.0 — the true loaded-window CPU, undiluted by idle setup.
check measure_window "$(compute_server_cpu_pct "${tmp}/s.csv" 3000 5000)" "300.0"

# A window with <2 samples inside → 0 (cannot compute a rate).
check window_single_sample "$(compute_server_cpu_pct "${tmp}/s.csv" 5000 5000)" "0"

# Header only → 0 (unchanged).
printf 'ts_ms,rss_bytes,cpu_ticks,write_bytes\n' > "${tmp}/empty.csv"
check empty "$(compute_server_cpu_pct "${tmp}/empty.csv")" "0"

$PASS && { echo "PASS"; exit 0; } || { echo "FAILED"; exit 1; }
