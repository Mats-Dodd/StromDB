#!/usr/bin/env bash
# run-matrix.sh — build images once, then run the per-system benchmark suites with
# at most MAX_PARALLEL_CLUSTERS (default 3) GKE clusters in parallel.
#
# Each suite is one system = its own cluster + zone (cluster bench-<mode>; the zone
# is derived from the mode in scripts/bench: wal→a, ursula→b, s2→c, node→d), so the
# parallel runs never collide. Each `scripts/bench … run` self-tears-down its cluster
# on clean completion; arm scripts/teardown-watchdog.sh separately (detached) as the
# hard-deadline safety net for a run that errors and keeps its cluster.
#
# Usage:
#   [SKIP_BUILD=1] [MAX_PARALLEL_CLUSTERS=3] scripts/run-matrix.sh [suite-basename ...]
#   default suites: run-durable run-ursula run-s2 run-node  (durable first = long pole)
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)"

MAXP="${MAX_PARALLEL_CLUSTERS:-3}"
SUITES=("$@"); [ ${#SUITES[@]} -eq 0 ] && SUITES=(run-durable run-ursula run-s2 run-node)
: > /tmp/run-matrix.log

if [ "${SKIP_BUILD:-0}" != 1 ]; then
  echo "[run-matrix] building + pushing images to Artifact Registry (Cloud Build)..."
  DS_TARGET=remote scripts/build-images.sh || { echo "[run-matrix] image build FAILED" >&2; exit 1; }
fi

# Portable concurrency cap (bash 3.2 has no `wait -n`): poll the live PIDs.
pids=()
# "${pids[@]:-}" guards the empty-array-under-set-u quirk in bash 3.2.
alive() { local n=0 p; for p in "${pids[@]:-}"; do [ -n "$p" ] && kill -0 "$p" 2>/dev/null && n=$((n+1)); done; echo "$n"; }

for s in "${SUITES[@]}"; do
  while [ "$(alive)" -ge "$MAXP" ]; do sleep 10; done
  echo "[run-matrix] launching $s ($(date -u +%H:%M:%SZ)) → /tmp/run-$s.log"
  (
    DS_TARGET=remote scripts/bench "suites/$s.json" run > "/tmp/run-$s.log" 2>&1
    echo "[run-matrix] $s exited rc=$? ($(date -u +%H:%M:%SZ))" >> /tmp/run-matrix.log
  ) &
  pids+=($!)
  sleep 30   # stagger cluster-up so the GKE control-plane API isn't hit all at once
done

echo "[run-matrix] all ${#SUITES[@]} suites launched; waiting for completion..."
wait
echo "[run-matrix] DONE — per-suite status in /tmp/run-matrix.log, results under results/run-*/"
cat /tmp/run-matrix.log
