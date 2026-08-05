#!/usr/bin/env bash
set -uo pipefail
OUT="${METRICS_OUT:-/metrics/samples.csv}"
PROC_NAME="${SERVER_PROC:-durable-streams-server}"
INTERVAL="${METRICS_INTERVAL_S:-1}"
DATA_DIR="${DATA_DIR:-/data}"

# ── Pod memory (cross-implementation) ────────────────────────────────────────
# Per-process RSS (below) only sees the server thread's anonymous memory, which
# differs wildly by implementation: a resident/residency cache shows as process
# RSS, whereas an OS-paging design keeps hot data in the page cache (charged to
# the cgroup, NOT the process). To compare memory fairly across implementations
# we sample the POD cgroup's working set = memory.current − inactive_file (the
# same definition the kubelet reports), which counts anon + active page cache.
#
# cgroupns is private in the sidecar (/proc/self/cgroup is "0::/"), so we can't
# read the server's cgroup via our own /sys/fs/cgroup. Instead the sidecar mounts
# the host cgroupfs (CGROUP_HOST, default /host/cgroup) read-only and locates the
# pod slice by POD_UID (downward API) — robust across cgroup drivers / QoS slices.
CGROUP_HOST="${CGROUP_HOST:-/host/cgroup}"
POD_CG=""
if [ -n "${POD_UID:-}" ] && [ -d "$CGROUP_HOST" ]; then
  u1="$POD_UID"; u2="$(echo "$POD_UID" | tr '-' '_')"
  POD_CG="$(find "$CGROUP_HOST" -maxdepth 6 -type d \( -name "*pod${u2}*" -o -name "*pod${u1}*" \) 2>/dev/null | head -1)"
fi
[ -n "$POD_CG" ] && echo "poller: pod cgroup ${POD_CG}" >&2 \
                 || echo "poller: pod cgroup not found (POD_UID=${POD_UID:-unset}); pod_ws_bytes=0" >&2

# Working set = memory.current − inactive_file (cgroup v2); fall back to v1.
pod_ws_bytes() {
  [ -z "$POD_CG" ] && { echo 0; return; }
  local cur inact
  if [ -r "$POD_CG/memory.current" ]; then
    cur="$(cat "$POD_CG/memory.current" 2>/dev/null || echo 0)"
    inact="$(awk '/^inactive_file /{print $2; exit}' "$POD_CG/memory.stat" 2>/dev/null || echo 0)"
  else
    cur="$(cat "$POD_CG/memory.usage_in_bytes" 2>/dev/null || echo 0)"
    inact="$(awk '/^total_inactive_file /{print $2; exit}' "$POD_CG/memory.stat" 2>/dev/null || echo 0)"
  fi
  : "${cur:=0}"; : "${inact:=0}"
  [ "$cur" -gt "$inact" ] 2>/dev/null && echo $(( cur - inact )) || echo "$cur"
}

# ── Disk write throughput (device-wide) ──────────────────────────────────────
# Device-wide "sectors written" from /proc/diskstats counts ALL writeback to the
# disk, including the kernel flusher that drains low-rate buffered writes — which
# the per-pid /proc/<pid>/io counter misses. The server node is dedicated, so the
# server is the only material writer to this device.
DEV="$(df "$DATA_DIR" 2>/dev/null | awk '$1 ~ /^\/dev\// {sub(/^\/dev\//,"",$1); print $1; exit}')"
[ -z "$DEV" ] && DEV="md0"   # fallback: GKE striped local-SSD RAID0

# field 10 of /proc/diskstats = sectors written; 512 bytes/sector.
disk_write_bytes() {
  local sec
  sec=$(awk -v d="$DEV" '$3==d{print $10; exit}' /proc/diskstats 2>/dev/null || echo 0)
  : "${sec:=0}"
  echo $(( sec * 512 ))
}

echo "ts_ms,rss_bytes,cpu_ticks,write_bytes,pod_ws_bytes" > "$OUT"
echo "poller: ${DATA_DIR} backed by device '${DEV}' (device-wide disk write_bytes)" >&2
while true; do
  # Per-process RSS/CPU (best-effort: 0 when the process name does not match — e.g.
  # a server whose binary name we don't track; pod memory still samples uniformly).
  # -f matches the full cmdline (not the 15-char-truncated comm); exclude self.
  rss=0; cpu=0
  pid="$(pgrep -f "$PROC_NAME" | grep -vx "$$" | head -1 || true)"
  if [ -n "$pid" ] && [ -r "/proc/$pid/stat" ]; then
    rss_pages=$(awk '{print $24}' "/proc/$pid/stat" 2>/dev/null || echo 0)   # field 24 = rss in pages
    rss=$(( rss_pages * $(getconf PAGE_SIZE) ))
    cpu=$(awk '{s=$0; sub(/^.*\) /,"",s); split(s,f," "); print f[12]+f[13]}' "/proc/$pid/stat" 2>/dev/null || echo 0)
  fi
  wbytes="$(disk_write_bytes)"; : "${wbytes:=0}"
  ws="$(pod_ws_bytes)"; : "${ws:=0}"
  ts=$(( $(date +%s%N) / 1000000 ))
  echo "${ts},${rss},${cpu},${wbytes},${ws}" >> "$OUT"
  sleep "$INTERVAL"
done
