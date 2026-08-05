#!/usr/bin/env bash
# Sourced by the benchmark runners + cluster/build helpers to select where the
# benchmark runs: a LOCAL kind cluster (fast dev loop, no cloud, no billing) or a
# REMOTE GKE cluster (measurement-grade, multi-node isolation).
#
#   DS_TARGET=local   (default) -> kind cluster `kind-<KIND_CLUSTER>`, locally
#                                  built images loaded via `kind load`, single
#                                  node (no role pools), IfNotPresent pulls.
#   DS_TARGET=remote            -> GKE context, Artifact Registry images, role
#                                  node pools, Always pulls.
#
# It exports the values the manifests need; the runners envsubst these into
# gke/*.yaml (image refs, pull policy, node selectors) and use $KCTX for kubectl.
# This file only sets variables — it never touches a cluster.

DS_TARGET="${DS_TARGET:-local}"

case "$DS_TARGET" in
  local)
    KIND_CLUSTER="${KIND_CLUSTER:-ds-bench}"
    KCTX="kind-${KIND_CLUSTER}"
    # Images are built locally (native arch) and `kind load`ed — no registry.
    # IMG_SERVER is overridable so several server implementations (reference,
    # io-uring, …) can be compared by pointing at distinct image tags.
    IMG_SERVER="${IMG_SERVER:-durable-streams:dev}"
    IMG_NODE="${IMG_NODE:-durable-node:dev}"   # Node.js reference server (kind-loaded)
    IMG_URSULA="${IMG_URSULA:-ghcr.io/tonbo-io/ursula:v0.2.0}"   # upstream published image
    IMG_DSBENCH="ds-bench:dev"
    IMG_METRICS="ds-bench:dev"        # ds-bench:dev carries bash+curl+procps for the sidecar
    PULL_POLICY="IfNotPresent"        # use the kind-loaded image; never reach for a registry
    NODESEL_SERVER="{}"               # single-node kind: schedule anywhere
    NODESEL_CLIENT="{}"
    SERVER_MACHINE="${SERVER_MACHINE:-kind}"   # server node machine type
    ;;
  remote)
    PROJECT="${PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
    ZONE="${ZONE:-europe-west1-b}"
    CLUSTER="${CLUSTER:-ds-bench}"
    KCTX="gke_${PROJECT}_${ZONE}_${CLUSTER}"
    # Artifact Registry: ${AR_LOCATION}-docker.pkg.dev/${PROJECT}/${AR_REPO}. Override
    # AR_LOCATION / AR_REPO (and PROJECT / ZONE) for your own project + region.
    AR_LOCATION="${AR_LOCATION:-europe-west1}"; AR_REPO="${AR_REPO:-ds-bench}"
    REG="${AR_LOCATION}-docker.pkg.dev/${PROJECT}/${AR_REPO}"
    IMG_SERVER="${IMG_SERVER:-${REG}/durable-streams:dev}"
    IMG_NODE="${IMG_NODE:-${REG}/durable-node:dev}"   # Node.js reference server
    IMG_URSULA="${IMG_URSULA:-ghcr.io/tonbo-io/ursula:v0.2.0}"   # upstream published image
    IMG_DSBENCH="${REG}/ds-bench:dev"
    IMG_METRICS="${REG}/ds-bench:dev"   # ds-bench:dev carries bash+procps for the sidecar
    # Default Always (a runner pushes a fresh :dev each session). Overridable to
    # IfNotPresent for long saturation runs where the image is FIXED for the whole
    # run: the server restarts once per ladder rung, and re-pulling from AR every
    # time adds ~2 min/restart — IfNotPresent reuses the node-cached image (it still
    # pulls if absent, e.g. on a scaled-up node, so it stays correct).
    PULL_POLICY="${PULL_POLICY:-Always}"
    NODESEL_SERVER='{ "role": "server" }'
    NODESEL_CLIENT='{ "role": "client" }'
    # Server node machine type — also the calibration-key component. This is the
    # EFFECTIVE default: target-env.sh is sourced into cluster-up.sh before its own
    # fallback, so the value here wins. c4d-8-lssd: same single Titanium NVMe as the
    # 16 (identical disk), durable server is 4-CPU-pinned so node size doesn't change
    # its numbers, ~½ the cost. Override to c4d-standard-16-lssd for 200-pod fleets
    # (MinIO burst headroom) / maximum measure isolation.
    SERVER_MACHINE="${SERVER_MACHINE:-c4d-standard-8-lssd}"
    ;;
  *)
    echo "DS_TARGET must be 'local' or 'remote' (got: '$DS_TARGET')" >&2
    return 2 2>/dev/null || exit 2
    ;;
esac

# Server pod memory limit (the cgroup OOM ceiling — drives fan-out subscriber capacity).
SERVER_MEM="${SERVER_MEM:-16Gi}"

# Per-fleet-pod cpu RESERVATION (scheduling only; no cpu limit in bench-job.yaml).
# Default 0.5: gke-bench's methodology is MANY LIGHT pods so the SERVER is the
# bottleneck. At 2, a 64-pod fleet reserves 128 cores = an entire 8-node client
# pool with no headroom → pod scheduling races → some reps run the server on ~1
# core (cpu ~100%) and under-report throughput. 0.5 keeps the fleet schedulable
# with headroom so every rep drives the server fully.
FLEET_CPU="${FLEET_CPU:-0.5}"

# Ursula Raft WAL backend: "disk" (durable, default) | "memory" (no disk WAL =
# Ursula's analog of durable-streams' fast/non-durable mode).
URSULA_WAL="${URSULA_WAL:-disk}"

# Vars referenced by envsubst in the manifests + by the runners.
export DS_TARGET KCTX IMG_SERVER IMG_NODE IMG_URSULA IMG_DSBENCH IMG_METRICS PULL_POLICY \
       NODESEL_SERVER NODESEL_CLIENT PROJECT ZONE CLUSTER KIND_CLUSTER REG \
       AR_LOCATION AR_REPO SERVER_MEM SERVER_MACHINE FLEET_CPU URSULA_WAL

# Every manifest envsubst must whitelist these so the image/policy/selector
# placeholders resolve. Runners reference $MANIFEST_VARS in their envsubst calls.
MANIFEST_VARS='${IMG_SERVER} ${IMG_NODE} ${IMG_URSULA} ${IMG_DSBENCH} ${IMG_METRICS} ${PULL_POLICY} ${NODESEL_SERVER} ${NODESEL_CLIENT} ${SERVER_MEM} ${FLEET_CPU} ${URSULA_WAL}'
export MANIFEST_VARS
