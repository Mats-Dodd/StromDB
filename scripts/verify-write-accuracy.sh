#!/usr/bin/env bash
# verify-write-accuracy.sh — cross-check the fleet's client-observed append
# counts against server-side truth for the MOST RECENT fleet run still on the
# server (state is wiped per rung, so run this right after a cell/rep finishes
# while its streams are still live).
#
#   1. Downloads the run's per-pod JSONs from in-cluster MinIO and sums
#      ok_total_all_phases (client truth, all phases) + lazy_creates + checks
#      the pod slices tile the whole domain disjointly.
#   2. Runs `ds-bench verify-offsets` in-cluster: HEADs every stream in the
#      global domain and sums stream-next-offset (server truth).
#   3. PASS iff server records == client records, slices tile [0, N), and
#      lazy_creates == 0.
#
# Usage: KCTX=kind-ds-bench scripts/verify-write-accuracy.sh <run-id> <streams> [payload_bytes]
#   <run-id>  MinIO prefix under bench-results/ (e.g. write-accuracy-local-wal-…-r1-p2)
#   list runs: scripts/verify-write-accuracy.sh --list
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KCTX="${KCTX:-kind-ds-bench}"
NS="${NS:-ds-bench}"
K() { kubectl --context "$KCTX" -n "$NS" "$@"; }

_mc() { K exec deploy/minio -- sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; $*"; }

if [ "${1:-}" = "--list" ]; then
  _mc "mc ls local/bench-results/" | awk '{print $NF}'
  exit 0
fi

RUN_ID="${1:?usage: verify-write-accuracy.sh <run-id> <streams> [payload_bytes]}"
STREAMS="${2:?streams (global domain size)}"
PAYLOAD="${3:-256}"
IMG="${IMG_DSBENCH:-ds-bench:dev}"

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT

echo "== 1. client truth: per-pod JSONs from bench-results/${RUN_ID}/ =="
pods="$(_mc "mc ls local/bench-results/${RUN_ID}/" | awk '{print $NF}' | grep -E '\.json$' || true)"
[ -n "$pods" ] || { echo "FAIL: no per-pod JSONs under ${RUN_ID}"; exit 1; }
for p in $pods; do
  _mc "mc cat local/bench-results/${RUN_ID}/${p}" > "$tmp/$p"
done

CLIENT_SUMMARY="$(python3 - "$tmp" "$STREAMS" <<'PY'
import json, sys, glob, os
d, n = sys.argv[1], int(sys.argv[2])
tot_all = tot_measured = lazy = errs = bp = 0
slices = []
for f in sorted(glob.glob(os.path.join(d, "*.json"))):
    j = json.load(open(f))
    tot_all += j.get("ok_total_all_phases", 0)
    tot_measured += j.get("counts", {}).get("ok", 0)
    errs += j.get("counts", {}).get("other_err", 0)
    bp += j.get("counts", {}).get("backpressure", 0)
    lazy += j.get("lazy_creates", 0)
    if "pod_slice_lo" in j:
        slices.append((j["pod_slice_lo"], j["pod_slice_hi"]))
slices.sort()
tiled = bool(slices) and slices[0][0] == 0 and slices[-1][1] == n and \
    all(slices[i][1] == slices[i+1][0] for i in range(len(slices)-1))
print(json.dumps({"pods": len(slices), "ok_total_all_phases": tot_all,
                  "ok_measured": tot_measured, "other_err": errs, "backpressure": bp,
                  "lazy_creates": lazy, "slices_tile_domain": tiled}))
PY
)"
echo "$CLIENT_SUMMARY"

echo "== 2. server truth: verify-offsets over ${STREAMS} streams =="
K delete pod verify-offsets --ignore-not-found >/dev/null 2>&1 || true
K run verify-offsets --image="$IMG" --restart=Never --image-pull-policy=IfNotPresent \
  --command -- ds-bench verify-offsets \
  --target http://durable-streams:4438 --streams "$STREAMS" --payload-bytes "$PAYLOAD" >/dev/null
K wait --for=jsonpath='{.status.phase}'=Succeeded pod/verify-offsets --timeout=300s >/dev/null
SERVER_JSON="$(K logs verify-offsets)"
K delete pod verify-offsets --ignore-not-found >/dev/null 2>&1 || true
echo "$SERVER_JSON"

echo "== 3. verdict =="
python3 - "$CLIENT_SUMMARY" "$SERVER_JSON" <<'PY'
import json, sys
c, s = json.loads(sys.argv[1]), json.loads(sys.argv[2])
checks = {
    "server_records == client ok_total_all_phases":
        s["total_records"] == c["ok_total_all_phases"],
    "pod slices tile the domain disjointly": c["slices_tile_domain"],
    "lazy_creates == 0 (setup owned all creation)": c["lazy_creates"] == 0,
    "no missing streams (full key-space coverage)": s["streams_missing"] == 0,
    "offsets divide payload exactly (uniform appends)": s["bytes_divide_exactly"],
    "no HEAD errors": s["head_errors"] == 0,
}
for k, v in checks.items():
    print(("PASS  " if v else "FAIL  ") + k)
delta = s["total_records"] - c["ok_total_all_phases"]
print(f"      server={s['total_records']} client={c['ok_total_all_phases']} delta={delta}")
sys.exit(0 if all(checks.values()) else 1)
PY
