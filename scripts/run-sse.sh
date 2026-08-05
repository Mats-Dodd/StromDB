#!/usr/bin/env bash
# run-sse.sh — SSE (fan-out delivery latency) for the 5 configs, on ONE cluster,
# reusing gke-bench.sh's proven SSE machinery (multi-fanout, subscriber sweep,
# delivery-p99). Guaranteed teardown + done marker. Writes results/sse-comparison.*
# and copies the curated data into results/final/sse/. Touches no other report.
#
# Configs (gke-bench SYSTEMS): durable:walnew (new binary, cache off),
# durable:walnew-cache (cache on), ursula:memory, ursula:disk, s2:_.
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)"
REPO_ROOT="$(pwd)"

CLUSTER="${CLUSTER:-bench-sse}"; ZONE="${ZONE:-europe-west4-a}"
DONE_MARKER="${DONE_MARKER:-$REPO_ROOT/.bench-state/sse.done}"
TIMEOUT_BIN="$(command -v gtimeout || command -v timeout || true)"
export DS_TARGET=remote PROJECT="${PROJECT:-$(gcloud config get-value project 2>/dev/null)}" PULL_POLICY="${PULL_POLICY:-IfNotPresent}"
# IMG_SERVER defaults to the standard durable-streams image (target-env derives it
# from the registry). Override to benchmark a specific server build.
export AR_LOCATION="${AR_LOCATION:-europe-west1}" AR_REPO="${AR_REPO:-ds-bench}"
export IMG_SERVER="${IMG_SERVER:-${AR_LOCATION}-docker.pkg.dev/${PROJECT}/${AR_REPO}/durable-streams:dev}"
log() { echo "[$(date -u +%H:%M:%S)] $*"; }

rm -f "$DONE_MARKER"
log "run-sse start — cluster=$CLUSTER zone=$ZONE IMG_SERVER=$IMG_SERVER"

# 1. Cluster: server + a small client pool (SSE uses ONE 12-CPU pod).
log "cluster-up $CLUSTER"
CLUSTER="$CLUSTER" ZONE="$ZONE" SERVER_MACHINE=c4d-standard-16-lssd \
  CLIENT_MACHINE=n2d-standard-16 CLIENT_NODES=2 DS_TARGET=remote bash scripts/cluster-up.sh \
  || { log "cluster-up FAILED"; }

# 2. SSE matrix via gke-bench.sh (WORKLOADS=sse only).
log "running gke-bench SSE"
if [ -n "$TIMEOUT_BIN" ]; then TO=("$TIMEOUT_BIN" 7200); else TO=(); fi
PROJECT="$PROJECT" ZONE="$ZONE" CLUSTER="$CLUSTER" DS_TARGET=remote PULL_POLICY=IfNotPresent \
  IMG_SERVER="$IMG_SERVER" SERVER_CPUS=4 \
  WORKLOADS=sse SYSTEMS="${SSE_SYSTEMS:-durable:walnew durable:walnew-cache ursula:memory ursula:disk s2:_}" \
  SSE_STREAMS=1 SSE_TOTAL_SUBS="1 10 100 1000" SSE_FLEET_CPU=12 SSE_REPS=1 \
  DURATION=20 WARMUP_SECS=10 SETTLE_SECS=5 \
  "${TO[@]}" bash scripts/gke-bench.sh || log "WARN: gke-bench sse exited non-zero/timeout"

# 3. Teardown (retry through RECONCILING).
log "teardown $CLUSTER"
ok=0
for i in $(seq 1 30); do
  gcloud container clusters describe "$CLUSTER" --zone "$ZONE" >/dev/null 2>&1 || { ok=1; break; }
  gcloud container clusters delete "$CLUSTER" --zone "$ZONE" --quiet >/dev/null 2>&1 && { ok=1; break; }
  log "  teardown retry $i (RECONCILING?)"; sleep 20
done
[ "$ok" = 1 ] || log "WARN: $CLUSTER still present — watchdog will catch it"

# 4. Report from the latest gke-bench summary.tsv.
SUM="$(ls -t results/bench/bench-*/summary.tsv 2>/dev/null | head -1)"
if [ -n "$SUM" ]; then
  RUNDIR="$(dirname "$SUM")"
  log "parsing $RUNDIR (merged.json, p50) -> sse report"
  python3 scripts/sse_report.py "$RUNDIR" sse-comparison || log "WARN: sse report failed"
  mkdir -p results/final/sse
  cp -f results/sse-comparison.md results/sse-comparison.csv results/final/sse/ 2>/dev/null || true
else
  log "WARN: no summary.tsv found"
fi

touch "$DONE_MARKER"
log "run-sse DONE — marker: $DONE_MARKER"
