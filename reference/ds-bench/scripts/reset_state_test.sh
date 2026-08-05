#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/lib-bench.sh
export RESET_DRYRUN=1 KCTX=test-ctx

out="$(reset_state wal)"
echo "$out" | grep -q "rollout restart" || { echo "FAIL wal: no rollout restart"; exit 1; }

out="$(reset_state s2)"
echo "$out" | grep -q "mc rm" || { echo "FAIL s2: no bucket wipe"; exit 1; }
echo "$out" | grep -q "rollout restart" || { echo "FAIL s2: no rollout restart"; exit 1; }

out="$(reset_state ursula)"
echo "$out" | grep -q "rollout restart" || { echo "FAIL ursula: no rollout restart"; exit 1; }

echo "PASS reset_state"
