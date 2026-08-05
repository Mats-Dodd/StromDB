# Multi-lane local-NVMe WAL benchmark setup

Purpose: run the durable-streams server on a GKE node that has **multiple,
physically-separate** local NVMe devices, with **one device per WAL shard
directory**, so each shard's `fdatasync` hits an independent device queue
(an independent *fsync lane*). This re-tests whether WAL throughput scales
with `--wal-shards` when the disk is genuinely multi-lane.

The single-device `c4d-standard-16-lssd` runs we did before could NOT show
shard scaling: `--ephemeral-storage-local-ssd` makes GKE **RAID0-stripe all
local SSDs into ONE filesystem**, so every shard's fsync serialises behind the
same single md/dm write barrier. That is a single-lane artifact, not a real
ceiling. To break it we must attach the NVMe as **raw block** (one `/dev` node
per physical device) and lay one filesystem per device under each shard dir.

---

## 1. Machine + provisioning command

Machine: **`c4d-standard-64-lssd`** — 64 vCPU, 6 physically-attached Titanium
local NVMe devices (each device is its own NVMe controller = independent queue).

### The raw-block flag (researched)

For **3rd/4th-generation** machine series (C3, C3D, C4, **C4D**, …) the local
SSD count is **fixed by the machine type**. gcloud therefore rejects an explicit
count — you pass `--local-nvme-ssd-block` **with NO `count=` field**, and GKE
provisions exactly the number of devices that come attached to the VM shape
(6 for `c4d-standard-64-lssd`):

> "If you use a machine type from a third or fourth generation machine series,
> use the `--local-nvme-ssd-block` option, without a count field, to create a
> cluster." — GKE docs, *Provision and use Local SSD-backed raw block storage*.

(By contrast, `--local-nvme-ssd-block=count=N` is only valid for 1st/2nd-gen
series such as N1/N2/N2D, where you choose N.)

This REPLACES `--ephemeral-storage-local-ssd` (which would RAID0-stripe the
6 devices into one filesystem). Raw block leaves the 6 devices as separate
`/dev` nodes so we can put one filesystem per device.

### Exact command (what `cluster-up.sh` now emits with the env gates set)

```bash
SERVER_MACHINE=c4d-standard-64-lssd \
SERVER_LOCAL_SSD_BLOCK=1 \
SPOT_SERVER=1 \
gcloud container clusters create bench-multilane \
  --zone europe-west4-b --project "$PROJECT" --num-nodes 1 \
  --machine-type c4d-standard-64-lssd \
  --local-nvme-ssd-block \
  --spot \
  --node-labels=role=server \
  --network benchmarking --subnetwork benchmarking \
  --enable-ip-alias --release-channel regular
```

(The clients pool `n2d-standard-32 ×3` is created by `cluster-up.sh` exactly as
today; only the SERVER pool gains the raw-block flag.)

### Resulting device paths on the GKE COS node

The COS host exposes the raw-block local NVMe devices under stable symlinks:

- Ordinal symlinks (**use these** — stable per boot):
  `/dev/disk/by-id/google-local-ssd-block0`,
  `/dev/disk/by-id/google-local-ssd-block1`, … `google-local-ssd-block5`
- UUID symlinks (generated, not stable across recreate):
  `/dev/disk/by-uuid/google-local-ssds-nvme-block/local-ssd-<UUID>`
- Underlying raw nodes: `/dev/nvme0n1 … /dev/nvme5n1` (ordering NOT guaranteed —
  do not hard-code these; resolve via the `by-id` symlink instead).

Source: GKE docs — *Provision and use Local SSD-backed raw block storage*
(`https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/local-ssd-raw`)
and *About Local SSD for GKE*
(`https://cloud.google.com/kubernetes-engine/docs/concepts/local-ssd`).

`gke/durable-streams-multilane.yaml` iterates the `google-local-ssd-block*`
glob, so it adapts to whatever count the machine actually exposes.

---

## 2. Mount approach (chosen)

A single **privileged initContainer** (`mount-shards`) runs before the server:

For each device `/dev/disk/by-id/google-local-ssd-blockI` (I = 0..5):

1. `mkfs.ext4 -F -q <device>` — **only if the device is not already ext4**
   (checked with `blkid`), so a pod restart that re-uses the same node's
   devices does not needlessly reformat.
2. `mkdir -p /data/wal/<I>`
3. `mount <device> /data/wal/<I>`
4. wipe the freshly-mounted dir (`rm -rf` its contents; `lost+found` is left).

This gives **one filesystem per physical NVMe device**, mounted at the exact
shard directory the server uses: the server opens `<data-dir>/wal/<i>/` per shard
(confirmed in `src/wal/walset.rs`: `wal_dir = data_dir.join("wal")`, each shard
at `wal_dir.join(i.to_string())`, persisted-N file at `<data-dir>/wal/shards`).
So **no server code change** is needed — `--data-dir /data` stays as is; shard `i`
just happens to land on device `i`. The tiny `<data-dir>/wal/shards` metadata
file lives on the base `data` volume (not on any NVMe device) — that is fine, it
is written once at creation and never fsync-hot.

**Shard count MUST be ≤ device count (6).** With `--wal-shards N`, the server
uses dirs `wal/0 … wal/N-1`; the init container mounts all 6 devices, so any
N ≤ 6 maps every shard onto its own device. `--wal-shards 7+` would put two
shards on the base volume / same device and defeat the experiment — the suite
caps the sweep at s6.

### Mount-propagation requirement (the crux)

A mount made inside a container is normally invisible to sibling containers. To
make the init container's `/data/wal/*` mounts visible to the server:

- The **init container** mounts the shared `data` volume with
  `mountPropagation: Bidirectional`. Bidirectional = the mount is propagated OUT
  to the host mount namespace (rshared), so it **survives the init container
  exiting** and is visible to later containers of the same pod.
- The **server container** mounts the same `data` volume with
  `mountPropagation: HostToContainer`, so it **receives** those propagated
  submounts.

> DESIGN NOTE / deliberate deviation from the task text: the task asked for
> `Bidirectional` on BOTH containers. Kubernetes requires a container using
> `Bidirectional` to be **`privileged: true`**. We do NOT want the server
> running privileged. `HostToContainer` on the server is sufficient (it only
> needs to *receive* the mounts, not create/propagate any) and keeps the server
> unprivileged. Only the short-lived `mount-shards` init container is
> privileged. If you truly want Bidirectional on the server, you must also set
> `securityContext.privileged: true` on it.

Kubernetes only honours these propagation modes when the kubelet's mount is
rshared (COS default: yes).

---

## VERIFY AT PROVISION TIME

1. **Device symlink name.** Docs give `/dev/disk/by-id/google-local-ssd-block0..5`
   for raw block. (The task text guessed `google-local-nvme-ssd-0..5`; that is
   the *ephemeral-storage* naming, NOT raw-block.) Confirm on the node:
   `ls -l /dev/disk/by-id/ | grep local-ssd` and, if the names differ, update the
   `DEV_GLOB` in `gke/durable-streams-multilane.yaml`.
2. **Device count = 6.** Confirm `c4d-standard-64-lssd` exposes 6 devices:
   `ls /dev/disk/by-id/google-local-ssd-block*` on the node (or
   `gcloud compute machine-types describe c4d-standard-64-lssd`). Keep the shard
   sweep ≤ device count.
3. **mkfs.ext4 / mount / blkid availability.** The `mount-shards` init container
   uses image `mirror.gcr.io/library/debian:12-slim` and `apt-get install -y
   e2fsprogs util-linux` at runtime, because **the server image (`${IMG_SERVER}`)
   and metrics image (`${IMG_METRICS}` = `ds-bench:dev`) are NOT known to ship
   `mkfs.ext4`/`mount`/`blkid`**. VERIFY the node has egress to
   `mirror.gcr.io` (GKE nodes do by default) and to the Debian apt mirrors. If
   the node is egress-restricted, bake e2fsprogs+util-linux into a pinned image
   in Artifact Registry and swap the `image:` field instead of apt-get at
   runtime.
4. **Bidirectional mount survives init-container exit.** This relies on the
   documented rshared propagation-to-host behavior. VERIFY after deploy:
   `kubectl exec deploy/durable-streams -c durable-streams -- mount | grep /data/wal`
   should list 6 ext4 mounts. If empty, the propagation did not survive — fall
   back to doing the mkfs+mount in a **sidecar that stays alive** (not an init
   container) OR set `privileged: true` + `Bidirectional` on the server too.
5. **hostPath `/dev` access.** The init container mounts hostPath `/dev` at
   `/dev` and runs `privileged: true` so the raw `by-id` symlinks (which point
   into `/dev/nvmeXn1`) resolve inside the container. VERIFY the COS node allows
   privileged pods (GKE Standard does; Autopilot does NOT — this needs a
   Standard cluster, which `cluster-up.sh` creates).
6. **Pod Security / seccomp.** The server pod already sets
   `seccompProfile.type: Unconfined` (for io_uring). The privileged init
   container is additionally exempt from restricted PSS. VERIFY no namespace
   `PodSecurity: restricted` label blocks it (the `ds-bench` ns is unlabeled
   today).
7. **Metrics sidecar diskstats.** The sidecar `df`s `/data` to find ONE backing
   device for `/proc/diskstats` sectors-written. With 6 sub-mounts the top-level
   `/data` still resolves to the base emptyDir device, so device-wide write bytes
   now under-count the per-shard NVMe traffic. This does not affect the WAL
   throughput/latency verdict (measured client-side); only the sidecar's
   write-bytes column is affected. Left as-is; flagged for awareness.
