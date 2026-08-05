#!/usr/bin/env bash
# Build + push the amd64 ds-bench and durable-streams images to Artifact
# Registry using Google Cloud Build (native amd64 in GCP). This avoids QEMU
# emulation on the arm64 mac host — durable-streams' Rust LTO release build
# would take ~1h under `buildx --platform linux/amd64`; Cloud Build is fast
# and pushes straight to AR.
#
# The Dockerfiles live in dockerfiles/ but their build CONTEXTS differ, so we copy
# each Dockerfile into its context root, submit that context, then remove the copy:
#   ds-bench        -> ds-bench/
#   durable-streams -> $DS_RUST_REPO/packages/server-rust  (standalone crate dir)
#   durable-node    -> $DS_NODE_REPO                        (pnpm workspace root)
set -euo pipefail
PROJECT="${1:-$(gcloud config get-value project)}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AR_LOCATION="${AR_LOCATION:-europe-west1}"; AR_REPO="${AR_REPO:-ds-bench}"
REG="${AR_LOCATION}-docker.pkg.dev/$PROJECT/$AR_REPO"
cd "$REPO_ROOT"
DS_RUST_REPO="${DS_RUST_REPO:-../electric-ds-rust}"
DS_NODE_REPO="${DS_NODE_REPO:-../durable-streams}"
# Server crate dir: the electric monorepo names it packages/durable-streams-rust;
# older checkouts used packages/server-rust. Auto-detect; DS_RUST_CRATE overrides.
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

cleanup() {
  rm -f ds-bench/Dockerfile "$RUST_CTX/Dockerfile" "$RUST_CTX/.dockerignore" \
        "$DS_NODE_REPO/Dockerfile" "$DS_NODE_REPO/.dockerignore"
}
trap cleanup EXIT

# --- ds-bench (context: ds-bench/) ---
echo "=== Cloud Build: ds-bench -> $REG/ds-bench:dev ==="
cp dockerfiles/ds-bench.Dockerfile ds-bench/Dockerfile
gcloud builds submit ds-bench --project "$PROJECT" --tag "$REG/ds-bench:dev"
rm -f ds-bench/Dockerfile

# --- durable-streams (context: the server-rust crate dir) ---
echo "=== Cloud Build: durable-streams -> $REG/durable-streams:dev ==="
cp dockerfiles/durable-streams.Dockerfile "$RUST_CTX/Dockerfile"
printf 'target/\n.git/\n' > "$RUST_CTX/.dockerignore"
gcloud builds submit "$RUST_CTX" --project "$PROJECT" --tag "$REG/durable-streams:dev"
rm -f "$RUST_CTX/Dockerfile" "$RUST_CTX/.dockerignore"

# --- durable-node (Node.js reference; context: the node monorepo; BUILD_NODE=0 to skip) ---
if [ "${BUILD_NODE:-1}" = 1 ]; then
  echo "=== Cloud Build: durable-node -> $REG/durable-node:dev ==="
  cp dockerfiles/durable-node.Dockerfile "$DS_NODE_REPO/Dockerfile"
  printf 'node_modules/\n.git/\ntarget/\n**/node_modules/\n**/target/\ndist/\n**/dist/\n' > "$DS_NODE_REPO/.dockerignore"
  gcloud builds submit "$DS_NODE_REPO" --project "$PROJECT" --tag "$REG/durable-node:dev"
  rm -f "$DS_NODE_REPO/Dockerfile" "$DS_NODE_REPO/.dockerignore"
fi

echo "pushed: $REG/ds-bench:dev  $REG/durable-streams:dev  $REG/durable-node:dev"
