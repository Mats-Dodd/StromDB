#!/usr/bin/env bash
# teardown-watchdog.sh — hard-deadline safety net. Independently force-deletes
# ALL bench-* GKE clusters at a deadline UNLESS run-all signalled clean completion
# (DONE_MARKER) first. Meant to be launched detached (nohup ... & disown) so it
# survives even if the orchestration / session dies — guaranteeing clusters never
# bill indefinitely because of a hang or bug that skips auto-teardown.
#
# Env:
#   DEADLINE_SECS   seconds from launch until the hard teardown fires (default 28800 = 8h)
#   DONE_MARKER     if this file appears, run-all already cleaned up -> exit quietly
#   POLL_SECS       how often to check (default 60)
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)"

DEADLINE_SECS="${DEADLINE_SECS:-28800}"
DONE_MARKER="${DONE_MARKER:-$(pwd)/.bench-state/run-all.done}"
POLL_SECS="${POLL_SECS:-60}"
# Which clusters this watchdog may delete (grep -E pattern). Default ALL bench-*;
# scope it (e.g. '^bench-ursula') when other bench runs share the project so a
# parallel run's cluster is never touched.
CLUSTER_FILTER="${CLUSTER_FILTER:-^bench-}"
START="$(date +%s)"
DEADLINE=$(( START + DEADLINE_SECS ))

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] watchdog: $*"; }
log "armed — deadline in ${DEADLINE_SECS}s; done-marker=$DONE_MARKER"

# List clusters matching the filter (name<TAB>zone), or nothing.
_matching() { gcloud container clusters list --format='value(name,zone)' 2>/dev/null | grep -E "$CLUSTER_FILTER"; }
# Delete matching clusters; returns 0 only if NONE remain afterwards (a delete can
# fail transiently while a cluster is PROVISIONING/RECONCILING — caller retries).
_sweep() {
  _matching | while read -r name zone; do
    log "deleting $name ($zone)"
    gcloud container clusters delete "$name" --zone "$zone" --quiet >/dev/null 2>&1 \
      && log "deleted $name" || log "WARN: delete $name failed (likely RECONCILING) — will retry"
  done
  [ -z "$(_matching)" ]
}

while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  if [ -f "$DONE_MARKER" ]; then
    # Don't blindly trust the marker: a run can write DONE even if its own teardown
    # failed (e.g. cluster was RECONCILING), which would orphan a billing cluster.
    # Verify nothing matching remains; if it does, delete it (retrying each poll
    # through RECONCILING) and only stand down once truly clean.
    if [ -z "$(_matching)" ]; then
      log "DONE marker present and no '${CLUSTER_FILTER}' clusters remain — standing down."
      exit 0
    fi
    log "DONE marker present but leftover '${CLUSTER_FILTER}' cluster(s) exist — sweeping."
    _sweep && { log "leftover swept — standing down."; exit 0; }
  fi
  sleep "$POLL_SECS"
done

log "DEADLINE REACHED without completion — force-deleting clusters matching '${CLUSTER_FILTER}'."
until _sweep; do log "retrying sweep (clusters still present)…"; sleep "$POLL_SECS"; done
log "watchdog teardown sweep complete."
