#!/usr/bin/env bash
# verify-accuracy-cell.sh — run ONE pool write cell on the local kind cluster and
# immediately cross-check the fleet's client-observed counts against server-side
# stream offsets (scripts/verify-write-accuracy.sh), while the cell's state is
# still live on the server. Standalone: deploys the server itself, shuts it down
# after. Usage:
#   scripts/verify-accuracy-cell.sh [streams] [pods] [server-args]
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

STREAMS="${1:-2000}"
PODS="${2:-2}"
export WAL_SERVER_ARGS="${3:-}"

export DS_TARGET=local CLUSTER="${KIND_CLUSTER:-ds-bench}" KCTX="kind-${KIND_CLUSTER:-ds-bench}"
export SERVER_CPUS="${SERVER_CPUS:-2}" SERVER_MEM="${SERVER_MEM:-2Gi}"
export SERVER_CPU="${SERVER_CPUS%% *}"
export FLEET_CPU="${FLEET_CPU:-0.25}" WARMUP_SECS="${WARMUP_SECS:-5}" SETTLE_SECS="${SETTLE_SECS:-3}"
export MEASURE_SECS="${MEASURE_SECS:-10}" SETUP_CONCURRENCY="${SETUP_CONCURRENCY:-32}"
export PAYLOAD_BYTES="${PAYLOAD_BYTES:-256}" CONNS_PER_POD="${CONNS_PER_POD:-64}" BATCH_PER_POD=1

export SUITE_FILE="suites/write-accuracy-local.json"
export SWEEP_RUN_ID="verify-cell"
export RESULTS_ROOT="results/_verify-cell" SAT_RESULT_ROOT="results/_verify-cell/cells"
mkdir -p "$RESULTS_ROOT"

. scripts/lib-saturate.sh

ensure_metrics_configmap >&2 || true
shutdown_local_servers >&2 || true
trap 'shutdown_local_servers >&2 || true' EXIT

echo "== deploy (wal mode, args='${WAL_SERVER_ARGS}') =="
deploy_mode wal || { echo "deploy failed"; exit 1; }

echo "== run one cell: n=${STREAMS} pods=${PODS} conns=${CONNS_PER_POD} =="
export SAT_MODE=wal SAT_SC="$STREAMS" SAT_REP=1
read -r cpu thr aligned < <(measure_pods "$PODS")
echo "cell result: cpu=${cpu}% thr=${thr} aligned=${aligned:-1}"

echo "== verify against live server state =="
KCTX="$KCTX" scripts/verify-write-accuracy.sh \
  "${SWEEP_RUN_ID}-wal-write-n${STREAMS}-p${PODS}-r1-p${PODS}" "$STREAMS" "$PAYLOAD_BYTES"
