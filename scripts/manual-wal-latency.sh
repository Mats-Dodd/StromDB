#!/usr/bin/env bash
# manual-wal-latency.sh — independent verification of the wal @100k numbers.
# Assumes the bench-verify cluster exists (cluster-up.sh done). Deploys the wal
# server (4 vCPU, 4 shards), then:
#   1. curl-based single-request append latency (NOT ds-bench code): 300 POSTs
#      on one connection, %{time_total} percentiles.
#   2. ds-bench single pod, fixed 100k-stream domain, concurrency ramp
#      64 → 256 → 1024 → 4096: throughput + p50/p99 per step.
# Confirms or refutes: ceiling ~60k ops/s; p50 at 4096-in-flight ≈ 64 ms is
# queueing (Little's law), while low-concurrency latency is ~1-3 ms.
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
export KUBECONFIG="$PWD/.bench-state/kc-bench-verify"
export DS_TARGET=remote PROJECT="${PROJECT:-vaxine}" CLUSTER=bench-verify ZONE=europe-west4-a
export KCTX="gke_${PROJECT}_${ZONE}_${CLUSTER}"
export SERVER_CPUS=4 SERVER_CPU=4 SERVER_MEM=16Gi
export WAL_SERVER_ARGS="--wal-shards 4 --worker-threads 4"

. scripts/lib-bench.sh

IMG="${IMG_DSBENCH:?target-env should set IMG_DSBENCH}"
STREAMS=100000

ensure_metrics_configmap >&2 || true
K exec deploy/minio -- true >/dev/null 2>&1 || { # minio not required, but ns must exist
  kubectl --context "$KCTX" create namespace ds-bench --dry-run=client -o yaml | kubectl --context "$KCTX" apply -f - >/dev/null; }

echo "== deploy wal server (4 vCPU, ${WAL_SERVER_ARGS}) =="
deploy_mode wal || { echo "deploy failed"; exit 1; }

echo "== seed: create ${STREAMS} streams + smoke load (single ds-bench pod, 64 conns, 15 s) =="
K delete pod manual-seed --ignore-not-found >/dev/null 2>&1
K run manual-seed --image="$IMG" --restart=Never --image-pull-policy=Always \
  --overrides='{"spec":{"nodeSelector":{"role":"client"}}}' \
  --command -- ds-bench multi-stream --target http://durable-streams:4438 --api-style durable \
  --streams "$STREAMS" --connections 64 --duration-secs 15 --warmup-secs 5 \
  --payload-bytes 256 --setup-concurrency 256 >/dev/null
K wait --for=jsonpath='{.status.phase}'=Succeeded pod/manual-seed --timeout=1200s >/dev/null || {
  echo "seed pod did not complete:"; K get pod manual-seed; K logs manual-seed --tail=20; exit 1; }
K logs manual-seed | python3 -c "
import json,sys
j=json.loads(sys.stdin.read())
print('  seed: ops/s=%.0f p50=%.2fms p99=%.2fms err=%d'%(j['aggregate_ops_per_sec'],j['latency_ms']['p50'],j['latency_ms']['p99'],j['counts']['other_err']))"
K delete pod manual-seed >/dev/null 2>&1

echo "== 1. curl single-request latency (300 sequential POSTs, one connection) =="
K delete pod manual-curl --ignore-not-found >/dev/null 2>&1
K run manual-curl --image=curlimages/curl --restart=Never \
  --overrides='{"spec":{"nodeSelector":{"role":"client"}}}' \
  --command -- sh -c '
    head -c 256 /dev/zero > /tmp/body;
    for i in $(seq 1 300); do
      curl -s -o /dev/null -w "%{time_total}\n" -X POST \
        -H "content-type: application/octet-stream" --data-binary @/tmp/body \
        http://durable-streams:4438/v1/stream/s00000042;
    done' >/dev/null
K wait --for=jsonpath='{.status.phase}'=Succeeded pod/manual-curl --timeout=300s >/dev/null
K logs manual-curl | python3 -c "
import sys
v=sorted(float(x)*1000 for x in sys.stdin if x.strip())
n=len(v)
print('  curl(1 conn): n=%d p50=%.2fms p90=%.2fms p99=%.2fms max=%.2fms'%(n,v[n//2],v[int(n*.9)],v[int(n*.99)],v[-1]))"
K delete pod manual-curl >/dev/null 2>&1

echo "== 2. ds-bench single-pod concurrency ramp over the SAME 100k domain =="
for C in 64 256 1024 4096; do
  K delete pod manual-ramp --ignore-not-found >/dev/null 2>&1
  K run manual-ramp --image="$IMG" --restart=Never --image-pull-policy=IfNotPresent \
    --overrides='{"spec":{"nodeSelector":{"role":"client"}}}' \
    --command -- ds-bench multi-stream --target http://durable-streams:4438 --api-style durable \
    --streams "$STREAMS" --connections "$C" --duration-secs 20 --warmup-secs 8 \
    --payload-bytes 256 --setup-concurrency 256 >/dev/null
  K wait --for=jsonpath='{.status.phase}'=Succeeded pod/manual-ramp --timeout=900s >/dev/null || {
    echo "  C=$C: pod did not complete"; K logs manual-ramp --tail=5; continue; }
  K logs manual-ramp | python3 -c "
import json,sys
j=json.loads(sys.stdin.read())
l=j['latency_ms']
print('  C=%-5d ops/s=%-8.0f p50=%-8.2f p90=%-8.2f p99=%-8.2f err=%d lazy=%d'%(
  ${C}, j['aggregate_ops_per_sec'], l['p50'], l['p90'], l['p99'],
  j['counts']['other_err'], j.get('lazy_creates',0)))"
done
K delete pod manual-ramp >/dev/null 2>&1
echo "done — server left running for follow-ups (teardown separately)"
