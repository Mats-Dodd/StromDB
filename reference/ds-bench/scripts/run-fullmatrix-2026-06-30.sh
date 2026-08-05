#!/usr/bin/env bash
# Full-matrix benchmark on the PR #4662 HEAD durable build (durable-streams:dev =
# commit 3754d64cb, image sha256:80b8abdc), saved to results-2026-06-30/ with
# commit provenance. Clean slate first; bulletproof teardown + watchdog at the end.
set -uo pipefail
cd /Users/vbalegas/workspace/ds-bench
export DS_TARGET=remote PROJECT=vaxine
DATE=2026-06-30
OUT="results-$DATE"
DONE_MARKER="$(pwd)/.bench-state/run-all.done"
mkdir -p .bench-state
rm -f "$DONE_MARKER"

WRITE_SUITES="run-durable run-ursula run-s2 run-node"
READ_SUITES="reads-catchup reads-longpoll reads-sse-remote"

teardown_all() {
  echo "===== FULL TEARDOWN $(date -u) ====="
  for s in $WRITE_SUITES $READ_SUITES; do
    BENCH_KEEP_CLUSTER=0 scripts/bench "suites/$s.json" teardown >/dev/null 2>&1 || true
  done
  for line in $(gcloud container clusters list --project vaxine --format='value(name,location)' 2>/dev/null | grep -i bench | tr '\t' ':'); do
    c="${line%%:*}"; z="${line##*:}"; echo "sweeping $c ($z)"
    gcloud container clusters delete "$c" --zone "$z" --project vaxine --quiet || true
  done
  touch "$DONE_MARKER"   # tell the watchdog we cleaned up
  echo "----- remaining bench clusters -----"
  gcloud container clusters list --project vaxine --format='value(name,status)' 2>/dev/null | grep -i bench || echo "(none)"
  echo "===== TEARDOWN DONE $(date -u) ====="
}
trap teardown_all EXIT INT TERM

# ---- Phase A: clean slate (delete existing bench clusters, waiting out PROVISIONING) ----
echo "===== PHASE A: clean slate $(date -u) ====="
for attempt in $(seq 1 90); do
  busy=0; any=0
  while IFS=$'\t' read -r c z st; do
    [ -z "$c" ] && continue; any=1
    case "$st" in
      PROVISIONING|RECONCILING|STOPPING) busy=1 ;;
      *) gcloud container clusters delete "$c" --zone "$z" --project vaxine --quiet --async || true ;;
    esac
  done < <(gcloud container clusters list --project vaxine --format='value(name,location,status)' 2>/dev/null | grep -i bench)
  [ "$any" = 0 ] && { echo "no bench clusters"; break; }
  [ "$busy" = 1 ] && echo "  waiting for in-progress cluster ops to settle ($attempt)" || echo "  delete issued, waiting ($attempt)"
  sleep 20
done
echo "clean slate complete: $(gcloud container clusters list --project vaxine --format='value(name)' 2>/dev/null | grep -ic bench) bench clusters remain"

# ---- arm watchdog (7h hard deadline; stands down when $DONE_MARKER appears) ----
DEADLINE_SECS=25200 nohup bash scripts/teardown-watchdog.sh >/tmp/teardown-watchdog.log 2>&1 &
echo "watchdog armed (pid $!, 7h deadline)"

# ---- Phase B: write throughput + latency + memory (HEAD image; SKIP_BUILD keeps it) ----
echo "===== PHASE B: write matrix $(date -u) ====="
SKIP_BUILD=1 MAX_PARALLEL_CLUSTERS=3 scripts/run-matrix.sh $WRITE_SUITES > /tmp/fm-write.log 2>&1 || echo "[write matrix rc=$?]"
echo "write matrix done $(date -u)"

# ---- Phase C: SSE fan-out (delivery latency + memory vs subscribers) ----
echo "===== PHASE C: SSE fan-out $(date -u) ====="
SKIP_BUILD=1 ZONE=europe-west4-a scripts/run-sse.sh > /tmp/fm-sse.log 2>&1 || echo "[sse rc=$?]"
echo "sse done $(date -u)"

# ---- Phase D: reads (catchup / long-poll / sse), from scratch ----
echo "===== PHASE D: reads $(date -u) ====="
for s in $READ_SUITES; do
  rm -rf "results/$s"
  echo "  running $s $(date -u)"
  scripts/bench "suites/$s.json" run > "/tmp/fm-$s.log" 2>&1 || echo "[$s rc=$?]"
done
echo "reads done $(date -u)"

# ---- Phase E: assemble dated results + provenance ----
echo "===== PHASE E: assemble $OUT $(date -u) ====="
mkdir -p "$OUT"
for d in run-durable run-ursula run-s2 run-node sse sse-memory reads-catchup reads-longpoll reads-sse-remote; do
  [ -d "results/$d" ] && cp -R "results/$d" "$OUT/" && echo "  copied results/$d"
done
cat > "$OUT/PROVENANCE.md" <<EOF
# Benchmark provenance — $DATE

Full matrix run on the **PR #4662 HEAD** durable-streams build (the reactor PR), from scratch.

## Versions (commit hashes)
- **durable-streams** (PR #4662, https://github.com/electric-sql/electric/pull/4662):
  commit \`3754d64cba5694ac7e4155ac57a959386001d055\` (branch \`sse-reactor-flat-userspace\`).
  Image \`durable-streams:dev\` = \`sha256:80b8abdcebeef4875391bb66ef5caa938c0cc30fd8b30d20ccee22c8fbca99fe\`, built $DATE.
  Reactor source verified byte-identical to this commit (post-fix, includes the
  reactor shutdown-leak + write()==0 guard).
- **ds-bench**: commit \`03ba78ac746e958e292321b2b369c4140f8be1f0\` (branch \`feat/read-scalability-workload\`).
- **ursula**: \`ghcr.io/tonbo-io/ursula:v0.1.5\`
- **Node.js reference**: \`durable-node:dev\`
- **S2 (s2lite)**: \`ghcr.io/s2-streamstore/s2\`

## Workloads
- **Write** throughput / latency / memory: \`run-durable\` (wal, wal-tailcache, memory),
  \`run-ursula\` (memory, disk), \`run-node\`, \`run-s2\` — saturation pod-ladder per cardinality.
- **SSE fan-out**: \`run-sse.sh\` — 1 stream, subscribers 1/10/100/1000, delivery latency + memory.
- **Reads**: \`reads-catchup\`, \`reads-longpoll\`, \`reads-sse-remote\` (wal + ursula).

## Hardware
Server \`c4d-standard-16-lssd\` pinned to 4 CPUs; client fleet \`n2d-standard-32\` Spot. europe-west4.
EOF
echo "wrote $OUT/PROVENANCE.md"
echo "===== FULL MATRIX COMPLETE $(date -u) ====="
