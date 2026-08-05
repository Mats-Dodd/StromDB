#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# lib-bench.sh — shared benchmark engine for the DS-rust suite.
#
# Sourced by the phase runners (gke-rawpower.sh = phase 1, gke-scaleout.sh =
# phase 2, gke-sustained.sh = phase 3). It is PURE definitions + target
# selection — sourcing it touches no cluster. It in turn sources target-env.sh
# (DS_TARGET=local kind | remote GKE) and defines K() + the deploy / fleet /
# coordinator / headroom / collect engine.
#
#   Config a caller sets before calling run_cell / _run_cell_one:
#     SERVER_CPUS REPEATS INIT_PARALLELISM
#     SWEEP_RUN_ID RESULTS_ROOT TARGET API_STYLE [PROBE_HOSTPORT]
#
# Contracts:
#   SWEEP_RUN_ID — stable per-run id. Matrix bench_cmds MUST name streams
#                  "${cell}-${SWEEP_RUN_ID}". run_cell re-sets $RUN_ID per attempt
#                  (the MinIO results prefix) so streams must NOT use $RUN_ID, or
#                  each cell would inherit the previous cell's id (malformed names).
#   RESULTS_ROOT — per-cell rep dirs (merged.json / samples.csv / verdict.txt) land
#                  under ${RESULTS_ROOT}/<cell>/rep<N>/.
# ─────────────────────────────────────────────────────────────────────────────

_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/target-env.sh
. "${_LIB_DIR}/target-env.sh"

PROBE_HOSTPORT="${PROBE_HOSTPORT:-durable-streams:4438}"

# ALL kubectl calls go through K() — scoped to the selected context + ds-bench ns.
K() { kubectl --context "$KCTX" -n ds-bench "$@"; }

# ensure_metrics_configmap — poller.sh ConfigMap (idempotent; must exist before
# the server, whose metrics sidecar mounts it).
ensure_metrics_configmap() {
  echo "--- ensuring metrics-poller ConfigMap..."
  K create configmap metrics-poller \
    --from-file=poller.sh=deploy/metrics/poller.sh \
    --dry-run=client -o yaml | K apply -f -
}

# shutdown_local_servers — on the shared local (kind) cluster every suite uses ONE
# cluster, so remove ALL server deployments (durable-streams / ursula / s2lite) + their
# pods to free the node. Lets a local suite self-clean on exit and pre-clear leftovers
# from a prior run, so suites never stack up / contend for the single node. MinIO and
# the metrics ConfigMap (shared infra) stay. No-op on remote (each mode has its own cluster).
shutdown_local_servers() {
  K delete deploy durable-streams durable-node ursula s2lite --ignore-not-found --cascade=foreground >&2 2>/dev/null || true
}

# ── deploy_server SERVER_CPU [extra_args...] ─────────────────────────────────
#   Applies durable-streams.yaml (envsubst MANIFEST_VARS + SERVER_CPU), optionally
#   patching server args. Two injection modes selected from extra_args:
#     "--tier local"  -> REPLACE the S3 tier block with a local cold-tier block.
#     other flags     -> APPEND each as a YAML list item after "--tier-allow-http".
#   Both use `sed r <tmpfile>` / range-delete (BSD-sed + GNU-sed safe).
deploy_server() {
  local cpu="$1"; shift
  # SERVER_EXTRA_ARGS (env) is prepended to any per-call flags so a comparison can
  # add server flags (e.g. "--wal --wal-sync strict --wal-fsync-events 1") to every
  # deploy without threading them through each runner. xargs normalizes whitespace
  # so the empty case stays empty (preserves the no-injection fast path).
  local extra_args; extra_args="$(echo "${SERVER_EXTRA_ARGS:-} ${*:-}" | xargs)"
  export SERVER_CPU="$cpu"

  # ── ursula variant ─────────────────────────────────────────────────────────
  # A different server (separate manifest, no --wal/--splice/--tier injection).
  # Matched node + cgroup budget (SERVER_CPU/SERVER_MEM) for a fair comparison.
  # Caller sets SERVER_KIND=ursula and TARGET/API_STYLE/PROBE_HOSTPORT=ursula:4437.
  if [ "${SERVER_KIND:-durable}" = "ursula" ]; then
    echo "    deploying ursula server: cpu=${cpu} mem=${SERVER_MEM} (${DS_TARGET})..."
    envsubst "${MANIFEST_VARS} \${SERVER_CPU} \${PROJECT}" < gke/ursula.yaml | K apply -f -
    K rollout status deploy/ursula --timeout=600s
    echo "    ursula available."
    echo "    probing ursula (need 3 consecutive HTTP answers)..."
    K run "ursula-probe-$$" --rm --attach --restart=Never --image=curlimages/curl:latest \
      --overrides="{\"spec\":{\"nodeSelector\":${NODESEL_CLIENT}}}" --command -- \
      /bin/sh -c "ok=0; for i in \$(seq 1 90); do code=\$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://${PROBE_HOSTPORT}/ 2>/dev/null); case \"\$code\" in 2??|3??|4??|5??) ok=\$((ok+1));; *) ok=0;; esac; if [ \"\$ok\" -ge 3 ]; then echo \"ursula ready (HTTP \$code, 3x)\"; exit 0; fi; sleep 1; done; echo 'ursula never ready'; exit 1" </dev/null \
      && echo "    probe ok" \
      || echo "    WARN: probe pod non-zero (kubectl --rm attach is flaky); continuing"
    return 0
  fi

  echo "    deploying durable-streams server: cpu=${cpu} extra='${extra_args}' (${DS_TARGET})..."

  # MULTILANE=1 deploys the multi-lane variant (one NVMe device per WAL shard dir;
  # see gke/durable-streams-multilane.yaml + MULTILANE_SETUP.md). It REQUIRES a
  # server pool created with SERVER_LOCAL_SSD_BLOCK=1 (raw-block local NVMe).
  # Default = the single-filesystem manifest (existing behavior preserved). The
  # variant is arg-compatible, so the same tier/injection sed paths below apply.
  local server_manifest="gke/durable-streams.yaml"
  [ "${MULTILANE:-0}" = "1" ] && server_manifest="gke/durable-streams-multilane.yaml"
  # SPLITLANE=1: device 0 = stream-data lane, devices 1..5 = WAL shard lanes
  # (see gke/durable-streams-splitlane.yaml). Server args must use
  # --data-dir /data/wal/0 and --wal-shards <= 5. Takes precedence over MULTILANE.
  [ "${SPLITLANE:-0}" = "1" ] && server_manifest="gke/durable-streams-splitlane.yaml"
  # GUARANTEED=1 (with SPLITLANE=1): Guaranteed-QoS variant — requests==limits on
  # every container, integer server CPU. On a STATIC_CPU=1 node pool the server
  # gets exclusive pinned cores (CPU-binding experiment).
  [ "${GUARANTEED:-0}" = "1" ] && server_manifest="gke/durable-streams-splitlane-guaranteed.yaml"
  # SERVER_MANIFEST: explicit override for one-off topologies (e.g. the 3x3
  # stream-lane/WAL-lane variant). Wins over all the flags above.
  [ -n "${SERVER_MANIFEST:-}" ] && server_manifest="$SERVER_MANIFEST"

  if [ -z "$extra_args" ]; then
    envsubst "${MANIFEST_VARS} \${SERVER_CPU}" < "$server_manifest" | K apply -f -

  elif echo "$extra_args" | grep -q -- "--tier local"; then
    local tmp_tier
    tmp_tier="$(mktemp /tmp/ds-tier-XXXXXX)"
    printf '            - "--tier"\n'               >> "$tmp_tier"
    printf '            - "local"\n'                >> "$tmp_tier"
    printf '            - "--tier-local-dir"\n'     >> "$tmp_tier"
    printf '            - "/data/cold"\n'           >> "$tmp_tier"
    printf '            - "--tier-segment-bytes"\n' >> "$tmp_tier"
    printf '            - "1048576"\n'              >> "$tmp_tier"
    envsubst "${MANIFEST_VARS} \${SERVER_CPU}" < "$server_manifest" \
      | sed \
          -e '/- "--tier"$/,/- "--tier-allow-http"$/d' \
          -e "/- \"\/data\"/r ${tmp_tier}" \
      | K apply -f -
    rm -f "$tmp_tier"

  else
    local tmp_inject
    tmp_inject="$(mktemp /tmp/ds-inject-XXXXXX)"
    for flag in $extra_args; do
      printf '            - "%s"\n' "$flag" >> "$tmp_inject"
    done
    envsubst "${MANIFEST_VARS} \${SERVER_CPU}" < "$server_manifest" \
      | sed "/--tier-allow-http/r ${tmp_inject}" \
      | K apply -f -
    rm -f "$tmp_inject"
  fi

  K wait --for=condition=available deploy/durable-streams --timeout=600s
  echo "    server available."

  # 3× consecutive HTTP readiness probe. nodeSelector via ${NODESEL_CLIENT} so it
  # schedules on a client node (remote) or anywhere (single-node kind, local).
  echo "    probing server (need 3 consecutive HTTP answers)..."
  K run "server-probe-$$" --rm --attach --restart=Never --image=curlimages/curl:latest \
    --overrides="{\"spec\":{\"nodeSelector\":${NODESEL_CLIENT}}}" --command -- \
    /bin/sh -c "ok=0; for i in \$(seq 1 90); do code=\$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://${PROBE_HOSTPORT}/ 2>/dev/null); case \"\$code\" in 2??|3??|4??|5??) ok=\$((ok+1));; *) ok=0;; esac; if [ \"\$ok\" -ge 3 ]; then echo \"server ready (HTTP \$code, 3x)\"; exit 0; fi; sleep 1; done; echo 'server never ready'; exit 1" </dev/null \
    && echo "    probe ok" \
    || echo "    WARN: probe pod non-zero (kubectl --rm attach is flaky); continuing"
}

# server_label — pod selector for the server under test (durable-streams|ursula|s2lite|durable-node),
# so the metrics sidecar helpers find the right pod regardless of SERVER_KIND.
server_label() {
  case "${SERVER_KIND:-durable}" in
    ursula) echo "app=ursula" ;;
    s2)     echo "app=s2lite" ;;
    node)   echo "app=durable-node" ;;
    *)      echo "app=durable-streams" ;;
  esac
}

# reset_sidecar_samples — truncate the server sidecar's samples.csv before a cell.
reset_sidecar_samples() {
  local pod
  pod="$( { K get pod -l "$(server_label)" -o name 2>/dev/null || true; } | head -1 | sed 's|pod/||')"
  if [ -n "$pod" ]; then
    echo "    resetting samples.csv on pod ${pod}..."
    K exec "$pod" -c metrics -- sh -c 'echo "ts_ms,rss_bytes,cpu_ticks,write_bytes,pod_ws_bytes" > /metrics/samples.csv' || true
  fi
}

# collect_sidecar DEST_DIR — copy the server sidecar's samples.csv out.
collect_sidecar() {
  local dest="$1"
  local pod
  pod="$( { K get pod -l "$(server_label)" -o name 2>/dev/null || true; } | head -1 | sed 's|pod/||')"
  if [ -n "$pod" ]; then
    K cp "ds-bench/${pod}:/metrics/samples.csv" "${dest}/samples.csv" -c metrics \
      && echo "    saved samples.csv → ${dest}/samples.csv" \
      || echo "    WARN: could not copy samples.csv"
  else
    echo "    WARN: no server pod found for sidecar collection"
  fi
}

# clean_jobs — delete bench-fleet + bench-coordinator synchronously.
clean_jobs() {
  # --cascade=foreground: block until the Jobs' PODS are gone too (default background
  # cascade deletes the Job object but reaps pods async -> they linger into the next
  # rung, which reused the fixed job names -> stale-pod log reads + "already exists").
  K delete job bench-fleet bench-coordinator --ignore-not-found --cascade=foreground --wait=true >/dev/null 2>&1 || true
  for _ in $(seq 1 45); do
    j=$( { K get jobs bench-fleet bench-coordinator --no-headers 2>/dev/null || true; } | wc -l | tr -d ' ')
    p=$( { K get pods -l 'job-name in (bench-fleet,bench-coordinator)' --no-headers 2>/dev/null || true; } | wc -l | tr -d ' ')
    if [ "$j" = "0" ] && [ "$p" = "0" ]; then break; fi
    sleep 2
  done
}

# _capture_choke_diagnostics — when a fleet didn't fully Complete, log WHY (to stderr,
# i.e. the run log) so a "creation_choke" is backed by a concrete cause. Distinguishes:
#   * server CRASH/OOM  -> server pod restarts>0 / lastTerminated reason OOMKilled|Error
#   * server CRASHLOOP   -> state=waiting reason=CrashLoopBackOff (e.g. s2lite)
#   * TIMEOUT (heavy)    -> server Running, 0 restarts; client pod still Running/slow
# plus a non-Completed client pod's last log lines (the actual client-side error).
_capture_choke_diagnostics() {
  echo "    ── choke diagnostics ──────────────────────────────────────────" >&2
  K get pods -l 'app in (durable-streams,ursula,s2lite)' -o jsonpath='{range .items[*]}      SERVER {.metadata.name} phase={.status.phase} restarts={.status.containerStatuses[0].restartCount} state={.status.containerStatuses[0].state} lastTerminated={.status.containerStatuses[0].lastState.terminated.reason}:{.status.containerStatuses[0].lastState.terminated.exitCode}{"\n"}{end}' 2>/dev/null >&2 || true
  local fp; fp="$(K get pods -l job-name=bench-fleet --no-headers 2>/dev/null | awk '$3!="Completed"{print $1; exit}')"
  if [ -n "$fp" ]; then
    echo "      CLIENT pod $fp ($(K get pod "$fp" -o jsonpath='{.status.phase}' 2>/dev/null)) — log tail:" >&2
    K logs "$fp" --tail=12 2>&1 | sed 's/^/        /' >&2 || true
  fi
  echo "    ───────────────────────────────────────────────────────────────" >&2
}

# ── Fleet start barrier (leader side) ─────────────────────────────────────────
# When BARRIER_DIR is non-empty, every fleet pod holds after its setup phase until
# a shared go time is published (see gke/bench-job.yaml + src/barrier.rs). The
# host is the leader: wait for PARALLELISM ready markers in MinIO, then publish
# `go` = now + BARRIER_GO_HEADROOM_SECS (unix ms). On BARRIER_SETUP_TIMEOUT_SECS
# the fleet is released anyway — the merged windows_aligned verdict judges the
# cell rather than a hung barrier wedging the walk.

# _barrier_mc <shell-with-mc> — run an mc pipeline inside the minio pod (it has
# the server-local alias). Overridable in tests.
_barrier_mc() {
  K exec deploy/minio -- sh -c \
    "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; $*" 2>/dev/null
}

# _barrier_reset — clear the run's WHOLE MinIO prefix BEFORE the fleet launches.
# RUN_IDs are deterministic per cell but do NOT include the server-config label,
# so two hazards share one fix:
#   * stale barrier state — a re-run (resume of an error cell) otherwise finds
#     the previous pass's ready markers and go file: pods grab the stale go (a
#     time in the past) and start immediately, staggering the fleet — exactly
#     the misalignment the barrier exists to prevent;
#   * stale RESULTS — a later config label (e.g. memory after wal) reuses the
#     same prefix; if one of its pods failed to upload, the coordinator would
#     silently merge the PREVIOUS label's same-named .hdr/.json into this
#     label's cell. Clearing the prefix turns that into an honest missing-pod
#     merge instead of cross-label contamination.
_barrier_reset() {
  _barrier_mc "mc rm --recursive --force local/bench-results/${RUN_ID}/ 2>/dev/null; true"
}

# _barrier_release_fleet <want-ready-count> — block until every pod is at the
# barrier (or timeout), then publish the go time.
_barrier_release_fleet() {
  local want="$1" poll="${BARRIER_POLL_SECS:-2}" n=0
  local deadline=$(( $(date +%s) + ${BARRIER_SETUP_TIMEOUT_SECS:-900} ))
  echo "    barrier: waiting for ${want} ready markers (timeout ${BARRIER_SETUP_TIMEOUT_SECS:-900}s)..."
  while :; do
    n="$(_barrier_mc "mc ls local/bench-results/${RUN_ID}/barrier/" | grep -c 'ready-' || true)"
    n="${n:-0}"
    [ "$n" -ge "$want" ] && break
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "    barrier: TIMEOUT with ${n}/${want} ready — releasing anyway (windows_aligned judges the cell)"
      break
    fi
    sleep "$poll"
  done
  local go_ms=$(( ( $(date +%s) + ${BARRIER_GO_HEADROOM_SECS:-5} ) * 1000 ))
  _barrier_mc "echo ${go_ms} | mc pipe local/bench-results/${RUN_ID}/barrier/go"
  echo "    barrier: released ${n}/${want} pods (go=${go_ms})"
}

# run_fleet_and_coordinator — expects: RUN_ID PARALLELISM BENCH_CMD OUT_PREFIX MERGE_CMD
run_fleet_and_coordinator() {
  export RUN_ID PARALLELISM BENCH_CMD OUT_PREFIX MERGE_CMD
  export BARRIER_DIR="${BARRIER_DIR:-}"

  clean_jobs
  # Stale barrier state from a previous pass of this same RUN_ID must be gone
  # BEFORE any pod can poll it.
  if [ -n "${BARRIER_DIR}" ]; then
    _barrier_reset
  fi

  echo "    launching fleet (${PARALLELISM} pods)..."
  # A fresh GKE control plane can refuse connections for minutes (resize) — a
  # failed apply here used to mean a fleet that never existed followed by a
  # full 900s barrier wait for it. Retry the apply through the outage; if it
  # still fails, skip the barrier wait so the rung fails FAST (no merged data →
  # thr 0 → the walker records an honest error cell).
  local _apply_ok=0 _try
  for _try in 1 2 3 4 5; do
    if envsubst "${MANIFEST_VARS} \${RUN_ID} \${PARALLELISM} \${BENCH_CMD} \${OUT_PREFIX} \${BARRIER_DIR}" \
        < gke/bench-job.yaml | K apply -f -; then
      _apply_ok=1; break
    fi
    echo "    fleet apply failed (attempt ${_try}/5) — API server unreachable? retrying in 30s..."
    sleep 30
  done
  if [ -n "${BARRIER_DIR}" ] && [ "${_apply_ok}" = 1 ]; then
    _barrier_release_fleet "${PARALLELISM}"
  elif [ "${_apply_ok}" != 1 ]; then
    echo "    fleet apply NEVER succeeded — skipping barrier wait; rung will record as error"
  fi
  # Tolerant: a hung/saturated server makes some pods fail → the Job never reaches
  # `complete`. Wait for complete OR failed, then proceed — the coordinator merges
  # whatever HDRs the surviving pods uploaded, instead of aborting under `set -e`.
  K wait --for=condition=complete job/bench-fleet --timeout="${FLEET_TIMEOUT:-180}s" 2>/dev/null \
    || K wait --for=condition=failed job/bench-fleet --timeout=5s 2>/dev/null \
    || true
  echo "    fleet pods: $(K get pods -l job-name=bench-fleet --no-headers 2>/dev/null | awk '{print $3}' | sort | uniq -c | tr '\n' ' ')"
  K get pods -l job-name=bench-fleet -o wide 2>/dev/null || true
  # If any fleet pod did NOT Complete, the cell will choke — capture WHY now (the
  # server pod's crash/OOM/restart state + a failed client pod's log tail), so the
  # generic "creation_choke" label is backed by a concrete reason in the run log.
  if K get pods -l job-name=bench-fleet --no-headers 2>/dev/null | awk '$3!="Completed"{f=1} END{exit !f}'; then
    _capture_choke_diagnostics
  fi

  echo "    launching coordinator..."
  K delete job bench-coordinator --ignore-not-found --cascade=foreground --wait=true >/dev/null 2>&1 || true
  for _ in $(seq 1 30); do
    c=$( { K get job bench-coordinator --no-headers 2>/dev/null || true; } | wc -l | tr -d ' ')
    if [ "$c" = "0" ]; then break; fi
    sleep 2
  done
  envsubst "${MANIFEST_VARS} \${RUN_ID} \${MERGE_CMD}" < gke/coordinator-job.yaml | K apply -f -
  # Tolerant: if the fleet all-errored there are no HDRs to merge and the
  # coordinator may never `complete` — don't abort the whole matrix.
  K wait --for=condition=complete job/bench-coordinator --timeout="${COORD_TIMEOUT:-90}s" 2>/dev/null \
    || K wait --for=condition=failed job/bench-coordinator --timeout=5s 2>/dev/null \
    || true
}

# fetch_coordinator_merged DEST — pull coordinator stdout into DEST. Retries while
# the pod is still ContainerCreating (kubectl logs returns BadRequest then). Never
# fatal: on persistent failure writes an error marker so `set -e` can't abort.
fetch_coordinator_merged() {
  local dest="$1" i out cpod
  for i in $(seq 1 30); do
    # Read the NEWEST coordinator pod specifically (not `job/bench-coordinator`,
    # which can match a lingering previous-rung pod and return STALE merged data).
    cpod="$(K get pods -l job-name=bench-coordinator --sort-by=.metadata.creationTimestamp \
            -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null)"
    if [ -n "$cpod" ] && out="$(K logs "$cpod" 2>/dev/null)" && [ -n "$out" ]; then
      printf '%s\n' "$out" > "$dest"
      return 0
    fi
    sleep 2
  done
  echo '{"error":"coordinator logs unavailable after retries"}' > "$dest"
  echo "    WARN: coordinator logs unavailable after retries → wrote error marker"
}

# compute_server_cpu_pct SAMPLES_CSV [START_MS END_MS] — cpu_pct =
# (Δticks/CLK_TCK)/Δs ×100. CLK_TCK=100. ts_ms (col 1) is unix wall-clock ms.
# With START_MS/END_MS (>0), only samples inside [START_MS, END_MS] count, so the
# figure reflects the LOADED measure window (pass the fleet's concurrent measure
# interval) instead of the whole cell diluted by idle setup/warmup/settle/upload —
# which understated "server CPU% at saturation". Omit the bounds for whole-cell.
compute_server_cpu_pct() {
  local csv="$1" start_ms="${2:-0}" end_ms="${3:-0}"
  awk -F',' -v start="$start_ms" -v end="$end_ms" '
    NR==1 { next }
    {
      ts=$1; ticks=$3
      if (start>0 && (ts<start || ts>end)) next    # scope to the measure window
      if (t0=="") { t0=ts; c0=ticks }              # first sample in scope
      t1=ts; c1=ticks                              # last sample in scope
    }
    END {
      if (t0=="" || t0==t1) { print "0"; exit }
      elapsed_s = (t1 - t0) / 1000.0
      delta_ticks = c1 - c0
      clk_tck = 100
      printf "%.1f\n", (delta_ticks / clk_tck) / elapsed_s * 100
    }
  ' "$csv"
}

# compute_server_mem_mb SAMPLES_CSV — prints "PEAK_MB DRIFT_MB" from rss_bytes (col 2).
#   peak  = max(rss)               (high-water mark, MiB)
#   drift = rss(last) - rss(first) (growth over the window, MiB; may be negative)
# Sidecar-instrumented for durable only; "0 0" when there are no data rows.
compute_server_mem_mb() {
  awk -F',' '
    NR==1 { next }
    NR==2 { r0=$2; rl=$2; peak=$2; next }
    { rl=$2; if ($2>peak) peak=$2 }
    END {
      if (r0=="") { print "0 0"; exit }
      drift = (rl - r0) / 1048576
      if (drift > -0.5 && drift < 0.5) drift = 0
      printf "%.0f %.0f\n", peak/1048576, drift
    }' "$1"
}

# compute_server_podmem_mb SAMPLES_CSV — peak POD working-set memory (col 5
# pod_ws_bytes = memory.current − inactive_file), MiB. Process-agnostic, so it is
# comparable across implementations (anon + active page cache). "0" if no data /
# the column is absent (older samples.csv) / the pod cgroup was not found.
compute_server_podmem_mb() {
  awk -F',' '
    NR==1 { next }
    { if (NF>=5 && $5+0 > peak) peak=$5+0 }
    END { printf "%.0f\n", peak/1048576 }' "$1"
}

# _run_cell_one — one measurement at a fixed pod count: deploy the fleet + coordinator,
# fetch the merged HDR + sidecar samples into <cell_dir>, echo "<cpu_pct> <thr>" (the
# only stdout line — all progress goes to stderr).
_run_cell_one() {
  local cell_name="$1" bench_cmd="$2" out_prefix="$3" merge_cmd="$4" pods="$5" repeat="$6" cell_dir="$7"
  RUN_ID="${SWEEP_RUN_ID}-${cell_name}-r${repeat}-p${pods}"
  export PARALLELISM="$pods"
  BENCH_CMD="$bench_cmd"; OUT_PREFIX="$out_prefix"; MERGE_CMD="$merge_cmd"
  { reset_sidecar_samples
    run_fleet_and_coordinator
    fetch_coordinator_merged "${cell_dir}/merged.json"
    collect_sidecar "$cell_dir"
  } >&2
  local cpu_pct="0"
  if [ -f "${cell_dir}/samples.csv" ]; then
    # Scope CPU% to the fleet's CONCURRENT measure window when the merge exposes it
    # (measure_overlap_{start,end}_unix_ms); else fall back to whole-cell. Keeps
    # "server CPU% at saturation" from being diluted by idle setup/warmup/upload
    # samples in the sidecar CSV.
    local cpu_lo cpu_hi
    cpu_lo="$(grep -oE '"measure_overlap_start_unix_ms"[: ]*[0-9]+' "${cell_dir}/merged.json" 2>/dev/null | grep -oE '[0-9]+$' | head -1)"
    cpu_hi="$(grep -oE '"measure_overlap_end_unix_ms"[: ]*[0-9]+' "${cell_dir}/merged.json" 2>/dev/null | grep -oE '[0-9]+$' | head -1)"
    if [ -n "$cpu_lo" ] && [ -n "$cpu_hi" ]; then
      cpu_pct="$(compute_server_cpu_pct "${cell_dir}/samples.csv" "$cpu_lo" "$cpu_hi")"
    else
      cpu_pct="$(compute_server_cpu_pct "${cell_dir}/samples.csv")"
    fi
  fi
  # saturation.py prints "<reason> <thr> <aligned> <p50> <p99>"; aligned=0 means
  # the fleet's measure windows didn't overlap (thr is already forced to 0 in
  # that case) — pass it through so the walker can label the rung
  # misaligned_windows. p50/p99 are the rung's merged latency (ms, "None" when
  # absent) so the walk can locate the pre-saturation knee.
  # --expect-pods = PARALLELISM: a merge from fewer pods (Spot preemption / OOM
  # dropped a pod that never uploaded) UNDER-counts the summed throughput, so
  # saturation.py forces thr=0 and the rung records as an error rather than a
  # deflated (falsely-plateaued) number. stderr carries the concrete reason.
  local thr aligned p50 p99
  read -r thr aligned p50 p99 < <(python3 "${REPO_ROOT}/scripts/saturation.py" --merged "${cell_dir}/merged.json" \
          --prev-thr 0 --cpu "$cpu_pct" --cores 1 --expect-pods "${PARALLELISM:-1}" 2>/dev/null | awk '{print $2, $3, $4, $5}')
  echo "${cpu_pct} ${thr:-0} ${aligned:-1} ${p50:-None} ${p99:-None}"
}

# run_cell CELL_NAME BENCH_CMD OUT_PREFIX MERGE_CMD SERVER_CPU_CORES — run a cell at a
# FIXED pod count (INIT_PARALLELISM), REPEATS times, via _run_cell_one. Collects
# merged.json/samples.csv/verdict.txt per rep under ${RESULTS_ROOT}/<cell>/rep<N>/.
run_cell() {
  local cell_name="$1" bench_cmd="$2" out_prefix="$3" merge_cmd="$4" cpu_cores="$5"
  local pods="${INIT_PARALLELISM:-2}"
  echo ""
  echo "=== cell: ${cell_name}  cpu=${cpu_cores}  pods=${pods}  repeats=${REPEATS:-1} ==="
  local repeat
  for repeat in $(seq 1 "${REPEATS:-1}"); do
    local cell_dir="${RESULTS_ROOT}/${cell_name}/rep${repeat}"
    mkdir -p "$cell_dir"
    local cpu_pct thr _aligned
    read -r cpu_pct thr _aligned < <(_run_cell_one "$cell_name" "$bench_cmd" "$out_prefix" "$merge_cmd" "$pods" "$repeat" "$cell_dir")
    { echo "cell=${cell_name}"; echo "parallelism=${pods}";
      echo "server_cpu_cores=${cpu_cores}"; echo "server_cpu_pct=${cpu_pct}"; } > "${cell_dir}/verdict.txt"
    echo "  ${cell_name}: pods=${pods} cpu%=${cpu_pct} thr=${thr}"
  done
}

# reset_state <mode> — clear server state between cells WITHOUT recreating the
# cluster. Pod restart wipes the emptyDir (+ the wipe-data initContainer guarantees
# the data-dir is empty even on NVMe). s2 keeps state in MinIO, so its bucket is
# emptied too. Deployment/bucket names: wal=durable-streams, ursula=ursula,
# s2=s2lite (bucket s2-bench). RESET_DRYRUN=1 echoes commands instead of running
# them (for tests).
# _wait_server_http <hostport> — block until the server returns 3 CONSECUTIVE HTTP
# responses through its Service (any 2xx–5xx = listener serving), so a fleet never
# races a freshly-restarted server whose old pod is still terminating / whose
# Service endpoints haven't re-pointed yet (the GKE creation_choke). rollout status
# only proves TCP readiness; this proves the server actually serves. Best-effort:
# the throwaway curl pod's --rm/--attach is flaky on exit, but its in-pod loop still
# performs the readiness WAIT regardless of the attach exit code.
_wait_server_http() {
  local hostport="$1"
  K run "reset-probe-$$-${RANDOM}" --rm --attach --restart=Never \
    --image=curlimages/curl:latest --overrides="{\"spec\":{\"nodeSelector\":${NODESEL_CLIENT}}}" --command -- \
    /bin/sh -c "ok=0; for i in \$(seq 1 60); do code=\$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://${hostport}/ 2>/dev/null); case \"\$code\" in 2??|3??|4??|5??) ok=\$((ok+1));; *) ok=0;; esac; [ \"\$ok\" -ge 3 ] && exit 0; sleep 1; done; exit 1" </dev/null >/dev/null 2>&1 || true
}

reset_state() {
  local mode="$1" run
  if [ "${RESET_DRYRUN:-0}" = "1" ]; then run="echo"; else run=""; fi
  case "$mode" in
    s2)
      # mc's alias lives in the minio pod's ~/.mc config; set it inline (as the
      # rest of the harness does) so the rm works in a fresh exec session.
      $run kubectl --context "$KCTX" -n ds-bench exec deploy/minio -- sh -c \
        'mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; mc rm --recursive --force local/s2-bench/ 2>/dev/null; true' || true
      $run kubectl --context "$KCTX" -n ds-bench rollout restart deploy/s2lite
      if [ -z "$run" ]; then kubectl --context "$KCTX" -n ds-bench rollout status deploy/s2lite --timeout=120s; _wait_server_http s2lite:80; fi
      ;;
    wal)
      $run kubectl --context "$KCTX" -n ds-bench rollout restart deploy/durable-streams
      if [ -z "$run" ]; then kubectl --context "$KCTX" -n ds-bench rollout status deploy/durable-streams --timeout=120s; _wait_server_http durable-streams:4438; fi
      ;;
    ursula)
      $run kubectl --context "$KCTX" -n ds-bench rollout restart deploy/ursula
      if [ -z "$run" ]; then kubectl --context "$KCTX" -n ds-bench rollout status deploy/ursula --timeout=120s; _wait_server_http ursula:4437; fi
      ;;
    node)
      # in-memory server: a rollout restart gives a fresh (empty) process
      $run kubectl --context "$KCTX" -n ds-bench rollout restart deploy/durable-node
      if [ -z "$run" ]; then kubectl --context "$KCTX" -n ds-bench rollout status deploy/durable-node --timeout=120s; _wait_server_http durable-node:4438; fi
      ;;
    *) echo "reset_state: unknown mode '$mode'" >&2; return 2 ;;
  esac
}

# deploy_mode <mode> — deploy ONE server for a persistent-cluster cell run and set
# the addressing globals (T_TARGET/T_API/T_NS) the walker's measure_pods consumes.
# Mirrors gke-bench.sh deploy_system's per-mode switch, but WITHOUT the per-cell
# `K delete` (the cluster persists; reset_state clears state between cells). Modes:
#   wal    -> durable-streams, --durability wal --wal-shards N --splice-appends
#   ursula -> ursula (URSULA_WAL backend from env; cold tier -> MinIO `ursula`)
#   s2     -> s2lite (cold tier -> MinIO `s2-bench`)
deploy_mode() {
  local mode="$1"
  case "$mode" in
    wal)
      # The (WAL-only) reference server needs NO durability flag: it always WALs,
      # auto-picks wal-shards = core count on a fresh data-dir (reset_state wipes it
      # each cell), and defaults tail-cache to 0 (off) on Linux. So the default is
      # empty. WAL_SERVER_ARGS injects server knobs to SWEEP (e.g. "--wal-shards 8"
      # or "--tail-cache-bytes 65536") when finding the optimal config.
      local args="${WAL_SERVER_ARGS:-}"
      SERVER_KIND=durable SERVER_EXTRA_ARGS="$args" PROBE_HOSTPORT="durable-streams:4438" \
        deploy_server "$SERVER_CPU" >&2 || return 1
      T_TARGET="http://durable-streams:4438"; T_API="durable"; T_NS=""; export SERVER_KIND=durable ;;
    ursula)
      # Backend (memory|disk) per server_config: the config's args (exported as
      # WAL_SERVER_ARGS by run_mode) pick it, so a suite can run ursula-memory +
      # ursula-disk as two labeled configs in ONE cluster. Falls back to the
      # URSULA_WAL env, then disk.
      local backend="${WAL_SERVER_ARGS:-${URSULA_WAL:-disk}}"
      SERVER_KIND=ursula URSULA_WAL="$backend" PROBE_HOSTPORT="ursula:4437" \
        deploy_server "$SERVER_CPU" >&2 || return 1
      T_TARGET="http://ursula:4437"; T_API="ursula"; T_NS="--bucket benchmark"; export SERVER_KIND=ursula ;;
    s2)
      envsubst "${MANIFEST_VARS}" < gke/s2lite.yaml | K apply -f - >&2 \
        && K rollout status deploy/s2lite --timeout=600s >&2 || return 1
      T_TARGET="http://s2lite:80"; T_API="s2"; T_NS="--basin benchmark"; export SERVER_KIND=s2 ;;
    node)
      # Node.js reference server — same protocol as the Rust server (api-style durable),
      # in-memory storage (no MinIO; a restart in reset_state gives fresh state).
      envsubst "${MANIFEST_VARS} \${SERVER_CPU}" < gke/durable-node.yaml | K apply -f - >&2 \
        && K rollout status deploy/durable-node --timeout=600s >&2 || return 1
      T_TARGET="http://durable-node:4438"; T_API="durable"; T_NS=""; export SERVER_KIND=node ;;
    *) echo "deploy_mode: unknown mode '$mode'" >&2; return 1 ;;
  esac
  export T_TARGET T_API T_NS
}

# server_image_digest <mode> [config_args] — echo a 12-char digest of the deployed
# server image (+ the config's server args, so a changed image OR a changed config
# re-runs that cell; see cells.status_of). Deployment per mode: wal=durable-streams,
# ursula=ursula, s2=s2lite.
server_image_digest() {
  local dep; case "$1" in wal) dep=durable-streams;; ursula) dep=ursula;; s2) dep=s2lite;; esac
  local img; img="$(K get "deploy/$dep" -o jsonpath='{.spec.template.spec.containers[0].image}')"
  printf '%s\n%s\n' "$img" "${2:-}" | sha256sum | cut -c1-12
}
