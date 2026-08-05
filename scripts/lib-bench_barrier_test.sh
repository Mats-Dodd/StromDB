#!/usr/bin/env bash
# lib-bench_barrier_test.sh — cluster-free unit test for _barrier_release_fleet
# (the fleet start-barrier leader): waits for N ready markers, then publishes a
# go time in the future; on timeout it releases anyway.
# Exit 0 = PASS, non-zero = FAIL.
set -uo pipefail

export DS_TARGET=local
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"; export REPO_ROOT
# shellcheck source=scripts/lib-bench.sh
source "${REPO_ROOT}/scripts/lib-bench.sh"

tmp="$(mktemp -d /tmp/lib-bench-barrier-XXXXXX)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "${tmp}/ready"
PASS=true

# Mock the MinIO round-trip with a directory: `mc ls` lists ready markers,
# `mc pipe …/go` captures the published go time, `mc rm` clears state.
_barrier_mc() {
  case "$*" in
    *"mc rm"*)   rm -rf "${tmp}/ready" "${tmp}/go"; mkdir -p "${tmp}/ready" ;;
    *"mc ls"*)   ls "${tmp}/ready" 2>/dev/null | sed 's/^/marker /' ;;
    *"mc pipe"*) # the leader embeds "echo <ms> | mc pipe …" — extract the ms
                 printf '%s' "$*" | grep -oE 'echo [0-9]+' | grep -oE '[0-9]+' > "${tmp}/go" ;;
  esac
}

export RUN_ID="barrier-test" BARRIER_POLL_SECS=1 BARRIER_GO_HEADROOM_SECS=5

# ── Case 1: all pods ready → go published with a future start time ────────────
touch "${tmp}/ready/ready-0" "${tmp}/ready/ready-1"
( sleep 2; touch "${tmp}/ready/ready-2" ) &   # last pod arrives late
t0=$(date +%s)
BARRIER_SETUP_TIMEOUT_SECS=30 _barrier_release_fleet 3 >/dev/null
t1=$(date +%s)
wait

if [ ! -s "${tmp}/go" ]; then
  echo "FAIL [release]: go was not published"; PASS=false
else
  go_ms="$(cat "${tmp}/go")"
  if [ $(( t1 - t0 )) -lt 2 ]; then
    echo "FAIL [release]: returned before the 3rd ready marker"; PASS=false
  elif [ "$go_ms" -le $(( t1 * 1000 )) ]; then
    echo "FAIL [release]: go time ${go_ms} is not in the future"; PASS=false
  else
    echo "ok [release]: waited for 3/3 ready, go=${go_ms}"
  fi
fi

# ── Case 2: timeout with missing pods → still releases (windows_aligned judges) ─
rm -f "${tmp}/go"; rm -rf "${tmp}/ready"; mkdir -p "${tmp}/ready"
touch "${tmp}/ready/ready-0"
BARRIER_SETUP_TIMEOUT_SECS=2 _barrier_release_fleet 5 >/dev/null
if [ ! -s "${tmp}/go" ]; then
  echo "FAIL [timeout]: go was not published after timeout"; PASS=false
else
  echo "ok [timeout]: released with 1/5 ready after deadline"
fi

# ── Case 3: _barrier_reset clears a previous pass's markers + go (RUN_ID reuse) ─
touch "${tmp}/ready/ready-0" "${tmp}/ready/ready-1"; echo 123 > "${tmp}/go"
_barrier_reset
if [ -s "${tmp}/go" ] || [ -n "$(ls "${tmp}/ready" 2>/dev/null)" ]; then
  echo "FAIL [reset]: stale barrier state survived reset"; PASS=false
else
  echo "ok [reset]: stale ready markers + go cleared"
fi

$PASS && echo "PASS: _barrier_release_fleet" && exit 0
echo "FAIL: _barrier_release_fleet"; exit 1
