#!/usr/bin/env bash
# ============================================================================
# gke-bench.sh — the ONE durable-streams benchmark matrix runner.
#
# Runs a reproducible benchmark matrix across streaming systems under a single,
# documented methodology on a GKE cluster (1 server node + a client fleet pool).
#
#   SYSTEMS    durable:strict  durable:wal   (our Rust server; modes: strict|wal)
#              ursula:disk     ursula:memory               (Raft; memory = best case)
#              s2:_                                         (S2-lite, object-store)
#   WORKLOADS  write     — multi-stream append throughput (ops/s + p99)
#              sse       — single-/multi-stream fan-out delivery latency (p99)
#              replay    — catch-up / mass reconnect (p99 + snapshot bytes)
#              sustained — steady low rate over a LONG window → server RSS drift /
#                          latency stability over time (durable-only)
#
#   METHODOLOGY (applied to every cell):
#     • CLEAN deploy   — a fresh server (empty data dir) per cell; no cross-cell
#                        stream accumulation.
#     • WARM-UP + WAIT — the client drives load for WARMUP_SECS (uncounted,
#                        warming caches/WAL/consensus), idles SETTLE_SECS, then
#                        measures only the steady window. (write+sse; replay is a
#                        one-shot reconnect → clean-deploy + settle instead.)
#     • REPS           — REPEATS measured runs per cell, reported as mean.
#     • CLIENT-UNBOUND — many light fleet pods (low FLEET_CPU) so the SERVER is
#                        the bottleneck, not the load generator.
#
# This is a SINGLE-CONFIG comparison in OUR environment (local-NVMe server,
# matched cgroup budget), best-case for each system. It is NOT a multi-node /
# replicated comparison — e.g. Ursula here is single-node, not a 3-voter quorum.
#
# KNOWN LIMITATION: the cpu_pct AND mem_peak_mb/mem_drift_mb columns (server CPU%
# and RSS, from a metrics sidecar) are instrumented for durable-streams ONLY.
# Ursula and S2 are not instrumented and report 0 — by design; we don't read
# their server CPU/memory.
#
# Prereqs:  a cluster from scripts/cluster-up.sh (CLIENT_NODES sized for the
#           fan-out) + images pushed (scripts/gke-push-images.sh). Does NOT
#           create or tear down the cluster.
#
# Usage:    PROJECT=<your-project> ZONE=.. CLUSTER=.. [knobs below] scripts/gke-bench.sh
# Output:   results/bench/bench-<ts>/summary.tsv  (+ per-cell merged/samples/hdr)
# ============================================================================
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
REPO_ROOT="$(pwd)"; export REPO_ROOT
export DS_TARGET="${DS_TARGET:-remote}" PROJECT="${PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
# ZONE/CLUSTER only required for remote target
[ "${DS_TARGET}" = "remote" ] && { export ZONE="${ZONE:?set ZONE}" CLUSTER="${CLUSTER:?set CLUSTER}"; } || { export ZONE="${ZONE:-local}" CLUSTER="${CLUSTER:-ds-bench}"; }
export SERVER_CPUS="${SERVER_CPUS:-4}" SERVER_MEM="${SERVER_MEM:-16Gi}"
# shellcheck source=scripts/lib-bench.sh
. scripts/lib-bench.sh
SERVER_CPU="${SERVER_CPUS%% *}"

# ── knobs (all overridable) ─────────────────────────────────────────────────
# SYSTEMS run in ORDER. List the PRIMARY system (our Rust durable-streams) FIRST
# so its stable headline numbers are collected and parked before the secondary /
# comparison systems (Ursula, S2, base-Node), which are likelier to need re-runs
# or to fail. A failed cell/system is non-fatal — the matrix continues.
# Format: "system:variant".
SYSTEMS="${SYSTEMS:-durable:strict durable:strict-iouring durable:wal ursula:memory s2:_}"
WORKLOADS="${WORKLOADS:-write sse replay sustained}"
WRITE_CARDS="${WRITE_CARDS:-1000 10000 100000}"   # stream counts for the write sweep
# SSE fan-out is a 2-D sweep: streams (M) × TOTAL subscribers (T). Per-stream
# subscribers = T/M (e.g. 10 streams × 1000 total = 100 subs/stream). Cells where
# T<M (would be <1 sub/stream) are skipped. Each SSE cell runs on ONE fleet pod so
# the M×T fan-out is exact (the fleet replicates per-pod, so >1 pod would multiply it).
# SSE = Ursula's published methodology: ONE stream, sweep the subscriber count.
SSE_STREAMS="${SSE_STREAMS:-1}"
SSE_TOTAL_SUBS="${SSE_TOTAL_SUBS:-1 10 100 1000}"
# SSE runs on ONE well-provisioned client pod (FLEET_CPU=SSE_FLEET_CPU): the writer
# and all subscribers share a single process, so delivery latency is measured against
# ONE wall clock — no cross-pod NTP skew — matching Ursula's published fan-out bench.
# Throughput here is writer-paced, so a single pod doesn't cap it. (multi_fanout still
# supports DS_BENCH_SHARDS sharding for a future SSE-throughput-ceiling test.)
SSE_FLEET_CPU="${SSE_FLEET_CPU:-12}"   # ≈ a full n2d-16 client node for the single SSE pod
SSE_REPS="${SSE_REPS:-1}"              # SSE delivery p99 is stable → 1 rep (no repetition)
REPLAY_CONF="${REPLAY_CONF:-1000:200}"            # clients:pre_events for catch-up
# Sustained: steady low per-stream rate over a LONG window (RSS drift / latency
# stability over time), swept over stream counts. Durable-only (memory is the point,
# and only durable is sidecar-instrumented). Its own long DURATION, separate from
# the short write/sse window above.
SUSTAINED_CARDS="${SUSTAINED_CARDS:-10 50 100 150}"   # stream counts
SUSTAINED_RATE="${SUSTAINED_RATE:-10}"                # per-stream ops/sec
SUSTAINED_DURATION="${SUSTAINED_DURATION:-90}"        # long, so the RSS sidecar captures drift
export REPEATS="${REPEATS:-2}"
BENCH_REPS="$REPEATS"   # this matrix runs its own rep loop; keep our own count.
export WARMUP_SECS="${WARMUP_SECS:-10}" SETTLE_SECS="${SETTLE_SECS:-5}" DURATION="${DURATION:-20}"
export FLEET_CPU="${FLEET_CPU:-0.5}"
PER_POD="${PER_POD:-250}"                          # target streams/pod (client-unbound, well-provisioned); pods=ceil(N/PER_POD), capped
# 64 keeps the load generator client-unbound for a 4-CPU server while not swamping
# the single MinIO result-store at collection time (200 simultaneous HDR uploads →
# dial timeouts → corrupted high-cardinality cells). Pairs with the c4d-8-lssd server
# node. Raise to 200 only with a c4d-16-lssd server (MinIO burst headroom).
MAX_FLEET_PODS="${MAX_FLEET_PODS:-64}"
export FLEET_TIMEOUT="${FLEET_TIMEOUT:-360}" COORD_TIMEOUT="${COORD_TIMEOUT:-180}"
# run_cell runs each cell at exactly INIT_PARALLELISM pods (set per cell by run_one).

TS="$(date +%s)"
export SWEEP_RUN_ID="bench-${TS}"
export RESULTS_ROOT="results/bench/bench-${TS}"
mkdir -p "$RESULTS_ROOT"
SUM="$RESULTS_ROOT/summary.tsv"
printf 'system\tvariant\tworkload\tparams\tpods\trep\tthr_or_evps\tp99_ms\tcpu_pct\tmem_peak_mb\tmem_drift_mb\n' > "$SUM"

echo "═══ gke-bench → $RESULTS_ROOT ═══"
echo "  systems  : $SYSTEMS"
echo "  workloads: $WORKLOADS"
echo "  method   : clean-deploy + warmup ${WARMUP_SECS}s + settle ${SETTLE_SECS}s + measure ${DURATION}s × ${REPEATS} reps, fleet_cpu=${FLEET_CPU}"
ensure_metrics_configmap
# cold-tier buckets (ursula/s2 write to object store)
K exec deploy/minio -- sh -c 'mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; mc mb -p local/ursula local/s2-bench local/durable-streams local/bench-results >/dev/null 2>&1; true' >/dev/null 2>&1 || true

# ── per-system deploy (fresh) + addressing ──────────────────────────────────
# Sets globals: T_TARGET T_API T_PROBE T_NS (namespace flag). Returns 1 on failure.
deploy_system() {  # system variant
  local sys="$1" var="$2"
  K delete deploy/durable-streams ursula s2lite --ignore-not-found --wait=true >/dev/null 2>&1 || true
  case "$sys" in
    durable)
      local args
      # walnew / walnew-cache: the SIMPLIFIED (wal-v2 / zero-copy) server binary —
      # WAL-only (no --durability) and no --splice-appends (both flags were dropped);
      # only --wal-shards (+ optional --tail-cache-bytes). Point IMG_SERVER at the new
      # image. Used for SSE on the new binary. Handled here so the legacy flag path
      # (which would inject rejected flags) is skipped entirely.
      case "$var" in
        walnew)       SERVER_KIND=durable SERVER_EXTRA_ARGS="--wal-shards ${WAL_SHARDS:-4}" PROBE_HOSTPORT="durable-streams:4438" deploy_server "$SERVER_CPU" >&2 || return 1
                      T_TARGET="http://durable-streams:4438"; T_API="durable"; T_PROBE="durable-streams:4438"; T_NS=""; return 0 ;;
        walnew-cache) SERVER_KIND=durable SERVER_EXTRA_ARGS="--wal-shards ${WAL_SHARDS:-4} --tail-cache-bytes ${TAIL_CACHE_BYTES:-65536}" PROBE_HOSTPORT="durable-streams:4438" deploy_server "$SERVER_CPU" >&2 || return 1
                      T_TARGET="http://durable-streams:4438"; T_API="durable"; T_PROBE="durable-streams:4438"; T_NS=""; return 0 ;;
      esac
      args="--durability ${var}"
      [ "$var" = strict-iouring ] && args="--durability strict --strict-io-uring"
      [ "$var" = wal ] && args="--durability wal --wal-shards ${WAL_SHARDS:-4}"
      # wal-cache: wal mode with the resident tail cache ON (64 KiB) — for the
      # tail-cache vs always-sendfile comparison (vs plain `wal` = cache off on Linux).
      [ "$var" = wal-cache ] && args="--durability wal --wal-shards ${WAL_SHARDS:-4} --tail-cache-bytes ${TAIL_CACHE_BYTES:-65536}"
      # strict-cache: strict durability + the resident tail cache ON. --tail-cache-bytes
      # is a standalone READ-path flag (store::set_tail_cache_bytes), independent of the
      # durability mode — the read path is identical across strict/wal/fast, so the cache
      # delta is the same whichever mode it rides on; this variant just labels it strict.
      [ "$var" = strict-cache ] && args="--durability strict --tail-cache-bytes ${TAIL_CACHE_BYTES:-65536}"
      # Linux-optimal: zero-copy splice(2) for the binary (octet-stream) bench
      # appends — a CPU lever (~½–⅓ append CPU); applies to every durable variant.
      args="$args --splice-appends"
      SERVER_KIND=durable SERVER_EXTRA_ARGS="$args" PROBE_HOSTPORT="durable-streams:4438" deploy_server "$SERVER_CPU" >&2 || return 1
      T_TARGET="http://durable-streams:4438"; T_API="durable"; T_PROBE="durable-streams:4438"; T_NS="" ;;
    ursula)
      SERVER_KIND=ursula URSULA_WAL="$var" PROBE_HOSTPORT="ursula:4437" deploy_server "$SERVER_CPU" >&2 || return 1
      T_TARGET="http://ursula:4437"; T_API="ursula"; T_PROBE="ursula:4437"; T_NS="--bucket benchmark" ;;
    s2)
      K apply -f gke/s2lite.yaml >&2 && K rollout status deploy/s2lite --timeout=600s >&2 || return 1
      T_TARGET="http://s2lite:80"; T_API="s2"; T_PROBE="s2lite:80"; T_NS="--basin benchmark" ;;
    *) echo "unknown system '$sys'" >&2; return 1 ;;
  esac
}

# Does this system support this workload? (S2-lite has no catch-up/replay.)
supports() {
  local sys="$1" wl="$2"
  [ "$sys" = s2 ] && [ "$wl" = replay ] && return 1
  # sustained measures server MEMORY stability — only durable is sidecar-instrumented.
  [ "$wl" = sustained ] && [ "$sys" != durable ] && return 1
  return 0
}

# Run one cell: fresh deploy + (warmup/settle baked into bench_cmd) + REPEATS reps.
# Records mean throughput/ev-s + p99 + cpu to the summary.
run_one() {  # sys var workload params bench_cmd_fn pods label
  local sys="$1" var="$2" wl="$3" params="$4" pods="$5" cell="$6" bench_cmd="$7" merge="$8"
  local rep cd thr p99 cpu cmd mem_peak mem_drift
  for rep in $(seq 1 "$BENCH_REPS"); do
    deploy_system "$sys" "$var" || { echo "[$cell] deploy failed (rep $rep) — skipping (system may be unbuilt-to-scale)"; continue; }
    # Substitute the target/api/namespace placeholders now that deploy set them.
    cmd="${bench_cmd//__T__/$T_TARGET}"; cmd="${cmd//__A__/$T_API}"; cmd="${cmd//__NS__/$T_NS}"
    local rcell="${cell}-r${rep}"
    INIT_PARALLELISM="$pods" MAX_PODS="$pods" \
      run_cell "$rcell" "$cmd" "$wl" "$merge" "$SERVER_CPU" >&2 || true
    cd="$RESULTS_ROOT/$rcell/rep1"
    cpu="$(compute_server_cpu_pct "$cd/samples.csv" 2>/dev/null || echo 0)"
    read -r mem_peak mem_drift < <(compute_server_mem_mb "$cd/samples.csv" 2>/dev/null || echo "0 0")
    thr="$(python3 scripts/saturation.py --merged "$cd/merged.json" --prev-thr 0 --cpu "$cpu" --cores 1 2>/dev/null | awk '{print $2}')"
    p99="$(grep -oE '"p99_ms"[: ]*[0-9.]+' "$cd/merged.json" 2>/dev/null | grep -oE '[0-9.]+$' | head -1)"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$sys" "$var" "$wl" "$params" "$pods" "$rep" "${thr:-0}" "${p99:-NA}" "${cpu:-0}" "${mem_peak:-0}" "${mem_drift:-0}" | tee -a "$SUM"
  done
}

clamp_pods() { local n="$1" p; p=$(( (n + PER_POD - 1) / PER_POD )); [ "$p" -lt 2 ] && p=2; [ "$p" -gt "$MAX_FLEET_PODS" ] && p="$MAX_FLEET_PODS"; echo "$p"; }

# ── the matrix ──────────────────────────────────────────────────────────────
for sysvar in $SYSTEMS; do
  sys="${sysvar%%:*}"; var="${sysvar#*:}"
  echo "════════════ SYSTEM ${sys}:${var} ════════════"
  for wl in $WORKLOADS; do
    supports "$sys" "$wl" || { echo "  (skip $wl — unsupported on $sys)"; continue; }
    # Durability mode only changes the WRITE path; SSE/replay are read paths and are
    # byte-identical across modes, so run reads on ONE durable config (wal) — the
    # other durable variants would just repeat the same read result. EXCEPT the
    # *-cache variants (strict-cache/wal-cache): the resident tail read-cache DOES
    # change the read path, so they MUST run reads (that IS the cache A/B). write
    # and sustained are write-path workloads → run on every durable variant.
    # walnew (new binary, cache off) and walnew-cache (cache on) are read-relevant
    # too — keep them OUT of the skip so SSE/replay actually run for the new binary.
    [ "$sys" = durable ] && [ "$wl" != write ] && [ "$wl" != sustained ] \
      && [ "$var" != wal ] && [ "$var" != strict-cache ] && [ "$var" != wal-cache ] \
      && [ "$var" != walnew ] && [ "$var" != walnew-cache ] && \
      { echo "  (skip $wl on durable:$var — reads are mode-independent; covered by durable:wal)"; continue; }
    case "$wl" in
      write)
        for n in $WRITE_CARDS; do
          pods="$(clamp_pods "$n")"; perpod=$(( (n + pods - 1) / pods ))
          # NS resolved at deploy; reference via $T_NS inside bench (deploy runs first per rep).
          run_one "$sys" "$var" write "n=$n" "$pods" "${sys}-${var}-write-n${n}" \
            "multi-stream --target __T__ --api-style __A__ __NS__ --streams ${perpod} --duration-secs ${DURATION} --payload-bytes 256 --setup-concurrency 256 --warmup-secs ${WARMUP_SECS} --settle-secs ${SETTLE_SECS}" \
            "ds-bench hdr-merge --hdr-dir /merge --results-dir /merge --label-prefix multi-stream-"
        done ;;
      sse)
        for m in $SSE_STREAMS; do
          for t in $SSE_TOTAL_SUBS; do
            [ "$t" -lt "$m" ] && continue          # need ≥1 subscriber per stream
            s=$(( t / m ))                          # subscribers per stream
            # ONE pod: writer + all subscribers co-located → single-clock latency.
            BENCH_REPS="$SSE_REPS" FLEET_CPU="$SSE_FLEET_CPU" run_one "$sys" "$var" sse "streams=$m,subs_per=$s,total=$t" 1 "${sys}-${var}-sse-m${m}t${t}" \
              "multi-fanout --target __T__ --api-style __A__ __NS__ --streams ${m} --subscribers-per-stream ${s} --writer-rate 50 --duration-secs ${DURATION} --warmup-secs ${WARMUP_SECS} --settle-secs ${SETTLE_SECS}" \
              "ds-bench hdr-merge --hdr-dir /merge --results-dir /merge --label-prefix multi-fanout-"
          done
        done ;;
      replay)
        cl="${REPLAY_CONF%%:*}"; ev="${REPLAY_CONF##*:}"; pods="$(clamp_pods "$cl")"
        run_one "$sys" "$var" replay "clients=$cl,events=$ev" "$pods" "${sys}-${var}-replay-c${cl}" \
          "catch-up --target __T__ --api-style __A__ __NS__ --clients ${cl} --pre-events ${ev} --event-bytes 1024 --setup-concurrency 256" \
          "ds-bench hdr-merge --hdr-dir /merge --results-dir /merge --label-prefix catch-up-" ;;
      sustained)
        for n in $SUSTAINED_CARDS; do
          pods="$(clamp_pods "$n")"; perpod=$(( (n + pods - 1) / pods ))
          run_one "$sys" "$var" sustained "n=$n,rate=$SUSTAINED_RATE" "$pods" "${sys}-${var}-sustained-n${n}" \
            "sustained --target __T__ --api-style __A__ __NS__ --streams ${perpod} --rate-per-stream ${SUSTAINED_RATE} --duration-secs ${SUSTAINED_DURATION} --snapshot-secs 5 --setup-concurrency 256" \
            "ds-bench hdr-merge --hdr-dir /merge --results-dir /merge --label-prefix sustained-"
        done ;;
    esac
  done
done

echo ""; echo "═══════ DONE → $SUM ═══════"; column -t "$SUM" 2>/dev/null || cat "$SUM"
echo "(cluster $CLUSTER still up — tear down when finished)"
