#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# cluster-up.sh — bring up the cluster + namespace + metrics ConfigMap + MinIO
# for DS_TARGET. Idempotent: re-running on an existing cluster just re-applies.
#   local  → kind create cluster (single node).
#   remote → gcloud create: c4d-standard-16-lssd role=server (Titanium NVMe) +
#            clients pool (n2d-standard-16 ×2, role=client) on the `benchmarking` VPC.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
# shellcheck source=scripts/lib-bench.sh
. scripts/lib-bench.sh   # sources target-env.sh; gives K() + ensure_metrics_configmap

if [ "$DS_TARGET" = "local" ]; then
  if kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
    echo "kind cluster '${KIND_CLUSTER}' already exists"
  else
    echo "=== kind create cluster --name ${KIND_CLUSTER} ==="
    kind create cluster --name "$KIND_CLUSTER"
  fi
else
  if gcloud container clusters describe "$CLUSTER" --zone "$ZONE" --project "$PROJECT" >/dev/null 2>&1; then
    echo "GKE cluster '${CLUSTER}' already exists"
    # An interrupted create can leave the cluster WITHOUT its clients pool (the
    # cluster create was submitted, the pool create never was). Fleet pods then
    # sit Pending forever on the role=client selector. Make adoption idempotent:
    # ensure the pool exists before proceeding.
    if ! gcloud container node-pools describe clients --cluster "$CLUSTER" --zone "$ZONE" --project "$PROJECT" >/dev/null 2>&1; then
      echo "=== clients pool missing on existing cluster — creating ==="
      SPOT_FLAG=()
      [ "${SPOT_CLIENTS:-1}" = "1" ] && SPOT_FLAG=(--spot)
      gcloud container node-pools create clients --cluster "$CLUSTER" --zone "$ZONE" --project "$PROJECT" \
        --machine-type "${CLIENT_MACHINE:-n2d-standard-16}" --num-nodes "${CLIENT_NODES:-2}" \
        --node-labels=role=client "${SPOT_FLAG[@]}"
    fi
  else
    echo "=== gcloud create cluster ${CLUSTER} (+ clients pool) ==="
    # c4d-8-lssd and c4d-16-lssd bundle the SAME single Titanium NVMe, so the
    # disk (the thing that matters for durability) is identical. The durable
    # server reserves only 500m (bursts to its 4-CPU limit), so 8 vCPU fits it +
    # MinIO + system. Pairs with ~64-pod fleets (MAX_FLEET_PODS). Override to
    # c4d-standard-16-lssd for 200-pod fleets / maximum measure isolation.
    SERVER_MACHINE="${SERVER_MACHINE:-c4d-standard-8-lssd}"
    # 4th-gen Titanium "-lssd" machines bundle a fixed Local SSD (the count is set
    # by the machine type — gcloud rejects an explicit count). Older N2D-style
    # types let you stripe N×375 GB devices via LOCAL_SSD_COUNT.
    #
    # SERVER_LOCAL_SSD_BLOCK=1 (default OFF) switches the server pool to RAW-BLOCK
    # local NVMe (--local-nvme-ssd-block) INSTEAD of ephemeral-storage. Rationale:
    # --ephemeral-storage-local-ssd RAID0-stripes ALL local SSDs into ONE
    # filesystem = ONE fsync barrier (single fsync lane), so per-shard fdatasync
    # can't scale. Raw block keeps each physical NVMe device a separate /dev node,
    # and gke/durable-streams-multilane.yaml (MULTILANE=1) mkfs+mounts one device
    # per WAL shard dir = one independent fsync lane per shard. On 3rd/4th-gen
    # (C3/C4/C4D "-lssd") the count is FIXED by the machine type, so the raw-block
    # flag takes NO count (see MULTILANE_SETUP.md). Existing behavior is preserved
    # when the env is unset.
    if [ "${SERVER_LOCAL_SSD_BLOCK:-0}" = "1" ]; then
      case "$SERVER_MACHINE" in
        *-lssd) LSSD_FLAG=(--local-nvme-ssd-block) ;;
        *)      LSSD_FLAG=(--local-nvme-ssd-block "count=${LOCAL_SSD_COUNT:-1}") ;;
      esac
    else
      case "$SERVER_MACHINE" in
        *-lssd) LSSD_FLAG=(--ephemeral-storage-local-ssd) ;;
        *)      LSSD_FLAG=(--ephemeral-storage-local-ssd "count=${LOCAL_SSD_COUNT:-1}") ;;
      esac
    fi
    # The server pool holds state, so it is on-demand by DEFAULT (a Spot
    # preemption mid-run kills the stateful server and invalidates that cell).
    # SPOT_SERVER=1 opts the server node into Spot too (cheapest; accept that a
    # preemption forces a re-run of the affected cells — the suite is resumable).
    SPOT_SERVER_FLAG=()
    [ "${SPOT_SERVER:-0}" = "1" ] && SPOT_SERVER_FLAG=(--spot)
    # STATIC_CPU=1: server pool kubelet runs cpuManagerPolicy=static, so a
    # GUARANTEED pod (integer CPU, requests==limits) gets EXCLUSIVE pinned cores
    # (true CPU binding; pairs with gke/durable-streams-splitlane-guaranteed.yaml).
    # Default off = shared cores (existing behavior preserved).
    STATIC_CPU_FLAG=()
    if [ "${STATIC_CPU:-0}" = "1" ]; then
      # NOTE: X's must be TRAILING — BSD/macOS mktemp doesn't substitute a
      # template with a suffix ("ds-syscfg-XXXXXX.yaml" is taken literally, so a
      # second run collides with "File exists" and the create never happens).
      _SYSCFG="$(mktemp /tmp/ds-syscfg-XXXXXX)" || { echo "FATAL: mktemp for kubelet system config failed" >&2; exit 1; }
      printf 'kubeletConfig:\n  cpuManagerPolicy: static\n' > "$_SYSCFG"
      STATIC_CPU_FLAG=(--system-config-from-file "$_SYSCFG")
    fi
    # Fail HARD if the create fails: continuing hands every later kubectl a
    # stale kubeconfig from a previous same-name cluster (dead IP), and the
    # harness's transient-error tolerance then burns a whole run against it.
    gcloud container clusters create "$CLUSTER" --zone "$ZONE" --project "$PROJECT" --num-nodes 1 \
      --machine-type "$SERVER_MACHINE" "${LSSD_FLAG[@]}" "${SPOT_SERVER_FLAG[@]}" "${STATIC_CPU_FLAG[@]}" \
      --node-labels=role=server --network benchmarking --subnetwork benchmarking \
      --enable-ip-alias --release-channel regular \
      || { echo "FATAL: cluster create failed for $CLUSTER" >&2; exit 1; }
    # The client fleet is disposable + fault-tolerant (the bench tolerates pod
    # failures), so run it on Spot VMs by default (~60-80% cheaper). The SERVER
    # pool stays on-demand (it holds state). SPOT_CLIENTS=0 forces on-demand.
    SPOT_FLAG=()
    [ "${SPOT_CLIENTS:-1}" = "1" ] && SPOT_FLAG=(--spot)
    gcloud container node-pools create clients --cluster "$CLUSTER" --zone "$ZONE" --project "$PROJECT" \
      --machine-type "${CLIENT_MACHINE:-n2d-standard-16}" --num-nodes "${CLIENT_NODES:-2}" \
      --node-labels=role=client "${SPOT_FLAG[@]}"
  fi
  gcloud container clusters get-credentials "$CLUSTER" --zone "$ZONE" --project "$PROJECT"
  gcloud auth configure-docker "${AR_LOCATION:-europe-west1}-docker.pkg.dev" -q || true
fi

echo "=== namespace + metrics ConfigMap + MinIO (context=${KCTX}) ==="
kubectl --context "$KCTX" create namespace ds-bench --dry-run=client -o yaml | kubectl --context "$KCTX" apply -f -
ensure_metrics_configmap
envsubst "$MANIFEST_VARS" < gke/minio.yaml | K apply -f -
K wait --for=condition=available deploy/minio --timeout=180s 2>/dev/null \
  || echo "WARN: minio not yet available — check 'K get pods'"
echo "✓ cluster-up complete (target=${DS_TARGET}, context=${KCTX})"
