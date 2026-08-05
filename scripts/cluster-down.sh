#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# cluster-down.sh — tear down the cluster for DS_TARGET.
#   local  → kind delete cluster (instant, frees the Docker containers).
#   remote → gcloud delete cluster --quiet, VERIFY it is gone (no billing), unset
#            the kube context. This is the STRICT teardown for cloud runs.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
# shellcheck source=scripts/target-env.sh
. scripts/target-env.sh

if [ "$DS_TARGET" = "local" ]; then
  echo "=== kind delete cluster --name ${KIND_CLUSTER} ==="
  kind delete cluster --name "$KIND_CLUSTER"
  echo "✓ local cluster deleted"
else
  echo "=== gcloud delete cluster ${CLUSTER} ==="
  gcloud container clusters delete "$CLUSTER" --zone "$ZONE" --project "$PROJECT" --quiet
  if gcloud container clusters list --project "$PROJECT" --format="value(name)" 2>/dev/null | grep -qx "$CLUSTER"; then
    echo "!! WARNING: cluster ${CLUSTER} STILL EXISTS — re-run teardown" >&2
    exit 1
  fi
  kubectl config unset "contexts.${KCTX}" >/dev/null 2>&1 || true
  echo "✓ remote cluster gone (no nodes, no billing); context unset"
fi
