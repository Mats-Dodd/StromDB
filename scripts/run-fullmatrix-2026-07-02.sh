#!/usr/bin/env bash
# Full-matrix benchmark on the perf-branch durable build (electric-ds-rust
# bench/mixed-interference-validation @ 06a8a37c5 = perf/combined-t1a-t1c-t2a head,
# PR #4679), saved to results-2026-07-02/ with commit provenance. First campaign to
# include the MIXED interference workload (Phase D2): a mixed-cal cell measures the
# remote ceiling, the mixed suites are anchored to it, then paced / hot-loop reader
# sweeps + the wal-vs-memory delivery sweep run on one persistent bench-mixed cluster.
# Clean slate first; bulletproof teardown + watchdog at the end.
set -uo pipefail
cd /Users/vbalegas/workspace/ds-bench
export DS_TARGET=remote PROJECT="${PROJECT:-vaxine}"
DATE=2026-07-02
OUT="results-$DATE"
DONE_MARKER="$(pwd)/.bench-state/run-all.done"
mkdir -p .bench-state
rm -f "$DONE_MARKER"

WRITE_SUITES="run-durable run-ursula run-s2 run-node"
READ_SUITES="reads-catchup reads-sse-remote"   # long-poll dropped per 2026-07-02 scope
MIXED_SUITES="mixed-cal mixed-writes mixed-writes-hot mixed-delivery"

REG="europe-west1-docker.pkg.dev/$PROJECT/ds-bench"
DS_RUST_CRATE="/Users/vbalegas/workspace/electric-ds-rust-1m/packages/durable-streams-rust"

teardown_all() {
  echo "===== FULL TEARDOWN $(date -u) ====="
  for s in $WRITE_SUITES $READ_SUITES $MIXED_SUITES; do
    BENCH_KEEP_CLUSTER=0 scripts/bench "suites/$s.json" teardown >/dev/null 2>&1 || true
  done
  for line in $(gcloud container clusters list --project "$PROJECT" --format='value(name,location)' 2>/dev/null | grep -i bench | tr '\t' ':'); do
    c="${line%%:*}"; z="${line##*:}"; echo "sweeping $c ($z)"
    gcloud container clusters delete "$c" --zone "$z" --project "$PROJECT" --quiet || true
  done
  touch "$DONE_MARKER"   # tell the watchdog we cleaned up
  echo "----- remaining bench clusters -----"
  gcloud container clusters list --project "$PROJECT" --format='value(name,status)' 2>/dev/null | grep -i bench || echo "(none)"
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
      *) gcloud container clusters delete "$c" --zone "$z" --project "$PROJECT" --quiet --async || true ;;
    esac
  done < <(gcloud container clusters list --project "$PROJECT" --format='value(name,location,status)' 2>/dev/null | grep -i bench)
  [ "$any" = 0 ] && { echo "no bench clusters"; break; }
  [ "$busy" = 1 ] && echo "  waiting for in-progress cluster ops to settle ($attempt)" || echo "  delete issued, waiting ($attempt)"
  sleep 20
done
echo "clean slate complete"

# ---- Phase A2: images. ds-bench (new mixed scenario) + durable from the PERF BRANCH
# crate (AGENTS.md §4: never let the matrix rebuild durable from the default source —
# build it ourselves, then SKIP_BUILD=1 everywhere). node/ursula/s2 images unchanged.
# RESUME=1 (relaunch after an interrupted run): images are already in AR and, more
# importantly, the results dirs hold completed cells the suites will skip — so
# Phase A2 (rebuild) and Phase A3 (rm -rf results) are both skipped.
if [ "${RESUME:-0}" = "1" ]; then
  echo "===== RESUME: skipping images + results wipe $(date -u) ====="
else
echo "===== PHASE A2: images $(date -u) ====="
( cd "$DS_RUST_CRATE" && [ "$(git branch --show-current)" = "bench/mixed-interference-validation" ] ) \
  || { echo "FATAL: $DS_RUST_CRATE not on bench/mixed-interference-validation"; exit 1; }
cp dockerfiles/ds-bench.Dockerfile ds-bench/Dockerfile
gcloud builds submit ds-bench --project "$PROJECT" --tag "$REG/ds-bench:dev" >/tmp/fm-build-dsbench.log 2>&1 || { echo "ds-bench build FAILED"; exit 1; }
rm -f ds-bench/Dockerfile
cp dockerfiles/durable-streams.Dockerfile "$DS_RUST_CRATE/Dockerfile.bench"
( cd "$DS_RUST_CRATE" && mv Dockerfile Dockerfile.orig && mv Dockerfile.bench Dockerfile && \
  printf 'target/\n.git/\nnode_modules/\n' > .dockerignore && \
  gcloud builds submit . --project "$PROJECT" --tag "$REG/durable-streams:dev" >/tmp/fm-build-durable.log 2>&1; rc=$?; \
  mv Dockerfile.orig Dockerfile 2>/dev/null; rm -f .dockerignore; exit $rc ) || { echo "durable build FAILED"; exit 1; }
echo "images pushed"

# ---- Phase A3: force true re-runs (resume digest is tag-based, not content-based) ----
for s in $WRITE_SUITES $READ_SUITES $MIXED_SUITES; do rm -rf "results/$s"; done
fi   # end RESUME skip

# ---- arm watchdog (8h hard deadline; stands down when $DONE_MARKER appears) ----
DEADLINE_SECS=28800 nohup bash scripts/teardown-watchdog.sh >/tmp/teardown-watchdog.log 2>&1 &
echo "watchdog armed (pid $!, 8h deadline)"

# ---- Phase B: write throughput + latency + memory ----
echo "===== PHASE B: write matrix $(date -u) ====="
SKIP_BUILD=1 MAX_PARALLEL_CLUSTERS=3 scripts/run-matrix.sh $WRITE_SUITES > /tmp/fm-write.log 2>&1 || echo "[write matrix rc=$?]"
echo "write matrix done $(date -u)"

# ---- Phase C: SSE fan-out (delivery latency + memory vs subscribers) ----
# No tailcache variant: single-stream high-fan-out SSE is a micro-benchmark; the
# spread-subscriber fan-out story lives in mixed-delivery (Phase D2).
echo "===== PHASE C: SSE fan-out $(date -u) ====="
SSE_SYSTEMS="durable:walnew ursula:memory ursula:disk s2:_" \
  SKIP_BUILD=1 ZONE=europe-west4-a scripts/run-sse.sh > /tmp/fm-sse.log 2>&1 || echo "[sse rc=$?]"
echo "sse done $(date -u)"

# ---- Phase D: reads (catchup / long-poll / sse) ----
echo "===== PHASE D: reads $(date -u) ====="
for s in $READ_SUITES; do
  echo "  running $s $(date -u)"
  scripts/bench "suites/$s.json" run > "/tmp/fm-$s.log" 2>&1 || echo "[$s rc=$?]"
done
echo "reads done $(date -u)"

# ---- Phase D2: mixed interference (one persistent bench-mixed cluster) ----
# 1. mixed-cal measures the remote mixed-shape ceiling (50 unthrottled writers).
# 2. Anchor the sweep suites: writes pin writers at 60% of ceiling; delivery levels
#    at ~5/20/50/80/100% of it.
# 3. Paced + hot reader sweeps, then the wal-vs-memory delivery sweep.
echo "===== PHASE D2: mixed $(date -u) ====="
BENCH_KEEP_CLUSTER=1 scripts/bench suites/mixed-cal.json run > /tmp/fm-mixed-cal.log 2>&1 || echo "[mixed-cal rc=$?]"
CEIL="$(python3 -c "import json;print(round(json.load(open('results/mixed-cal/aggregate.json'))[0]['write_ops_per_sec'] or 0))" 2>/dev/null || echo 0)"
if [ "${CEIL:-0}" -gt 0 ] 2>/dev/null; then
  echo "  mixed ceiling: ${CEIL} ops/s — anchoring sweep suites"
  python3 - "$CEIL" <<'PY'
import json, sys
c = int(sys.argv[1])
def patch(path, fn):
    d = json.load(open(path)); fn(d)
    json.dump(d, open(path, "w"), indent=2); open(path, "a").write("\n")
def writers(path):
    # writers_per_stream is 1 in all mixed suites, so writer count = streams.
    return json.load(open(path))["stream_counts"][0]
for p in ("suites/mixed-writes.json", "suites/mixed-writes-hot.json"):
    pin = max(1, round(0.6 * c / writers(p)))
    patch(p, lambda d, pin=pin: d["mixed"].update(writer_rate=pin))
    print(f"  {p}: writer_rate pin={pin}/writer over {writers(p)} writers")
w = writers("suites/mixed-delivery.json")
levels = sorted(set(max(1, round(f * c / w)) for f in (0.05, 0.2, 0.5, 0.8))) + [0]
patch("suites/mixed-delivery.json", lambda d: d["mixed"].update(levels=levels))
print(f"  delivery levels={levels} over {w} writers")
PY
  for s in mixed-writes mixed-writes-hot mixed-delivery; do
    echo "  running $s $(date -u)"
    BENCH_KEEP_CLUSTER=1 scripts/bench "suites/$s.json" run > "/tmp/fm-$s.log" 2>&1 || echo "[$s rc=$?]"
  done
else
  echo "  mixed-cal produced no ceiling — skipping mixed sweeps (see /tmp/fm-mixed-cal.log)"
fi
scripts/bench suites/mixed-cal.json teardown >/dev/null 2>&1 || true   # bench-mixed down
echo "mixed done $(date -u)"

# ---- Phase E: assemble dated results + provenance ----
echo "===== PHASE E: assemble $OUT $(date -u) ====="
mkdir -p "$OUT"
for d in $WRITE_SUITES sse sse-memory $READ_SUITES $MIXED_SUITES; do
  [ -d "results/$d" ] && cp -R "results/$d" "$OUT/" && echo "  copied results/$d"
done
find "$OUT" -type d -name cells -exec rm -rf {} + 2>/dev/null
DURABLE_SHA="$(cd "$DS_RUST_CRATE" && git rev-parse HEAD)"
DSBENCH_SHA="$(git rev-parse HEAD)"
cat > "$OUT/PROVENANCE.md" <<EOF
# Benchmark provenance — $DATE

Full matrix on the **perf-branch** durable build, from scratch. First campaign to
include the mixed read/write interference workload (PR electric#4679 has the local
validation this extends).

## Versions (commit hashes)
- **durable-streams**: \`$DURABLE_SHA\`
  (branch \`bench/mixed-interference-validation\` = \`perf/combined-t1a-t1c-t2a\` head, PR #4679).
  Image \`$REG/durable-streams:dev\`, built $DATE via Cloud Build from the crate checkout.
- **ds-bench**: \`$DSBENCH_SHA\` (mixed workload harness).
- **ursula**: \`ghcr.io/tonbo-io/ursula:v0.1.5\` · **Node.js reference**: \`durable-node:dev\` · **S2**: \`ghcr.io/s2-streamstore/s2\`

## Workloads
- **Write** saturation: \`run-durable\` (wal, wal-tailcache, memory — streams up to
  **500k**), \`run-ursula\` (memory, disk), \`run-node\`, \`run-s2\`.
- **SSE fan-out**: \`run-sse.sh\` — subscribers 1/10/100/1000, **no tailcache variant**
  (single-stream fan-out is a micro-benchmark; spread fan-out is mixed-delivery's job).
- **Reads**: \`reads-catchup\`, \`reads-sse-remote\` (wal + ursula; long-poll dropped this run).
- **Mixed interference** (NEW): \`mixed-cal\` (ceiling anchor), \`mixed-writes\`
  (readers 0→**100k**, one staggered replay/30s each, vs a 60%-pinned write load over
  10k streams), \`mixed-writes-hot\` (unpaced adversarial), \`mixed-delivery\`
  (2000 SSE subscribers over 2000 streams vs write-rate ladder, wal + memory).

## Hardware
Server \`c4d-standard-16-lssd\` pinned to 4 CPUs; client fleet \`n2d-standard-32\` Spot. europe-west4.
EOF
echo "wrote $OUT/PROVENANCE.md"
echo "===== FULL MATRIX COMPLETE $(date -u) ====="
