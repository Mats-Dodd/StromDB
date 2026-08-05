#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# build-images.sh — build the ds-bench + durable-streams + durable-node images.
#   local  → native `docker build` (arm64 on Apple Silicon, no QEMU) + `kind load`
#            (no registry — fast dev loop).
#   remote → Cloud Build → Artifact Registry (scripts/gke-push-images.sh, amd64).
#
# Source repos (override to point at a different checkout, then re-run):
#   DS_RUST_REPO — the Rust server monorepo; the image builds from its
#                  packages/server-rust crate dir. Default ../electric-ds-rust.
#   DS_NODE_REPO — the Node reference server monorepo (packages/server). The build
#                  context is the repo root (pnpm workspace). Default ../durable-streams.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
# shellcheck source=scripts/target-env.sh
. scripts/target-env.sh

DS_RUST_REPO="${DS_RUST_REPO:-../electric-ds-rust}"
DS_NODE_REPO="${DS_NODE_REPO:-../durable-streams}"
# The server crate dir inside DS_RUST_REPO. The electric monorepo names it
# packages/durable-streams-rust; older checkouts used packages/server-rust.
# Auto-detect, overridable via DS_RUST_CRATE (absolute or repo-relative).
if [ -n "${DS_RUST_CRATE:-}" ]; then
  case "$DS_RUST_CRATE" in
    /*) RUST_CTX="$DS_RUST_CRATE" ;;
    *)  RUST_CTX="${DS_RUST_REPO}/${DS_RUST_CRATE}" ;;
  esac
elif [ -d "${DS_RUST_REPO}/packages/durable-streams-rust" ]; then
  RUST_CTX="${DS_RUST_REPO}/packages/durable-streams-rust"
else
  RUST_CTX="${DS_RUST_REPO}/packages/server-rust"
fi

if [ "$DS_TARGET" = "remote" ]; then
  echo "=== remote: Cloud Build → Artifact Registry ==="
  exec scripts/gke-push-images.sh "${PROJECT}"
fi

# ── local: native docker build + kind load ───────────────────────────────────
build_one() {  # tag dockerfile context
  local tag="$1" dockerfile="$2" context="$3" rc
  echo "=== docker build ${tag} (native) — context ${context} ==="
  cp "$dockerfile" "${context}/Dockerfile"
  printf 'target/\n.git/\nnode_modules/\n**/target/\n**/node_modules/\ndist/\n**/dist/\n' > "${context}/.dockerignore"
  if docker build -t "${tag}" "${context}"; then rc=0; else rc=$?; fi
  rm -f "${context}/Dockerfile" "${context}/.dockerignore"
  [ "$rc" = 0 ] || { echo "build ${tag} FAILED (rc=$rc)" >&2; exit "$rc"; }
}

build_one "ds-bench:dev"         dockerfiles/ds-bench.Dockerfile         ds-bench
build_one "durable-streams:dev"  dockerfiles/durable-streams.Dockerfile  "$RUST_CTX"
# Node.js reference server (BUILD_NODE=0 to skip when iterating only on the Rust server).
loaded="durable-streams:dev ds-bench:dev"
if [ "${BUILD_NODE:-1}" = 1 ]; then
  build_one "durable-node:dev" dockerfiles/durable-node.Dockerfile "$DS_NODE_REPO"
  loaded="$loaded durable-node:dev"
fi

echo "=== kind load docker-image → ${KIND_CLUSTER} ==="
kind load docker-image $loaded --name "$KIND_CLUSTER"
echo "✓ images built + loaded (rust=$(git -C "$DS_RUST_REPO" rev-parse --short HEAD 2>/dev/null) node=$(git -C "$DS_NODE_REPO" rev-parse --short HEAD 2>/dev/null))"
