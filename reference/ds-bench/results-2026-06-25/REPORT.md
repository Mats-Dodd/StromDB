# Durable-streams benchmarks — write scalability + memory (2026-06-25)

A single-node run focused on **write throughput**, **server memory**, and **SSE fan-out**, across
the currently-supported implementations on their latest versions, **including the Node.js reference
server**. Each workload is a declarative suite under `suites/`; see *Reproducing* at the end.

**Setup.** One server node (`c4d-standard-16-lssd`), server pinned to **4 CPUs**; a Kubernetes
client fleet (`n2d-standard-32`, Spot) drives load. 256-byte binary appends. Throughput is the
**saturation ceiling** — a per-cardinality pod ladder ramped until per-server throughput plateaus.
Latency is fleet-wide p50/p99 from merged HDR histograms. **Memory is the pod cgroup working set**
(`memory.current − inactive_file` = anon + active page cache), sampled each second at the pinned
rung; reported as **peak** (high-water) / **p50** (median = steady state).

**Systems (latest versions).**
- **durable-streams** (Rust, `electric/packages/server-rust` @ 2026-06-25): `wal` (default,
  group-commit fsync), `wal-tailcache` (`--tail-cache-bytes 65536`, resident read cache on), and
  `memory` (`--durability memory`, no WAL — zero-copy socket→file splice, page-cache ack). All tier
  cold segments to in-cluster MinIO.
- **ursula** (`ghcr.io/tonbo-io/ursula:v0.1.5`): single-node Raft; `memory` and `disk` WAL backends.
  Cold tier → MinIO.
- **Node.js reference** (`@durable-streams/server`, `durable-streams/packages/server`): the
  protocol's reference implementation; same wire protocol (`api-style durable`), in-memory storage.
- **S2 (s2lite)** (`ghcr.io/s2-streamstore/s2`): object-store-backed (writes through to MinIO).

> `memory + tail cache` is not a configuration — `--durability memory` force-disables the resident
> tail cache (it serves reads from the page cache), so durable has three real write configs.

---

## 1. Write throughput at saturation (ops/s)

| streams | durable wal | durable wal-tc | durable memory | ursula-mem | ursula-disk | node | s2 |
|---|---|---|---|---|---|---|---|
| 100 | 520k | 528k | 427k | 48k | 4.6k | 55k | 2.0k |
| 1 000 | 650k | 607k | 488k | 91k | 4.8k | 76k | — |
| 10 000 | 572k | 560k | 479k | 89k | 8.7k | 63k | — |
| 100 000 | **860k** | 887k | **786k** | — | — | — | — |

**Peak:** durable ~**0.79–0.89M append/s** (wal 860k @ 80 pods; memory 786k @ ~100 pods; wal-tailcache ≥887k).
durable is **~7–9× ursula-in-memory** (~90k), **~70–100× ursula-disk** (~5–9k), and **~10× node**
(~55–76k). S2 does not scale past ~100 streams (1000 chokes on stream creation). ursula/node/s2 cap
at 10k/1k/100 streams respectively (higher cardinalities crash or don't saturate within budget).

## 2. Write append latency at saturation (p50 / p99, ms)

| streams | durable wal | durable memory | ursula-mem | ursula-disk | node | s2 |
|---|---|---|---|---|---|---|
| 100 | 0.26 / 0.46 | 0.29 / 0.48 | 0.67 / 39 | 26 / 59 | 1.6 / 3.7 | 51 / 52 |
| 1 000 | 1.26 / 6.4 | 1.60 / 7.9 | 4.2 / 103 | 197 / 503 | 18 / 27 | — |
| 10 000 | 1.47 / 203 | 1.81 / 125 | 101 / 368 | 1001 / 3195 | 29 / 68 | — |

durable stays **sub-ms→2 ms median** throughout (p99 grows with the backlog at the ceiling);
ursula-in-memory tens→hundreds of ms; ursula-disk **hundreds of ms→seconds** (Raft-log fsync per
commit); s2 ~50 ms (object-store hot path).

## 3. Write pod memory — peak / p50 (MiB)

| streams | durable wal | durable wal-tc | durable memory | ursula-mem | ursula-disk | node |
|---|---|---|---|---|---|---|
| 100 | 103 / 45 | 110 / 36 | 93 / 51 | 3693 / 2644 | 1031 / 948 | 488 / 279 |
| 1 000 | 52 / 41 | 74 / 57 | 61 / 42 | 2245 / 1817 | 1986 / 1719 | 214 / 159 |
| 10 000 | 202 / 177 | 203 / 183 | 167 / 134 | 5058 / 4286 | 2982 / 2561 | 1052 / 793 |
| 100 000 | 950 / 515 | 977 / 655† | **769 / 496** | — | — | — |

The headline result. **durable stays in tens–hundreds of MiB** even at 100k streams (`memory` mode
is the lightest — no WAL buffers); **ursula sits at 1–5 GB** throughout. And the *shape* differs:
- **durable: p50 ≪ peak** (e.g. wal @ 100k 515 / 950; wal @ 100 45 / 103) — it holds very little
  steadily; the peak is a transient WAL group-commit / page-cache spike during the write burst.
- **ursula: p50 ≈ peak** (e.g. disk @ 10k 2561 / 2982; mem @ 10k 4286 / 5058) — it holds gigabytes
  *resident the whole time*.

**Why** (verified in the source). durable is **one file per stream**: each stream is a lean
`StreamState` in a `DashMap` (no in-memory payload, no offset index, lean appender) plus the OS cost
of its open file (fd + inode/dentry + active tail page cache). So memory tracks the **number of
streams**, not bytes — and the data itself lives on disk / in the page cache / tiered to MinIO,
never resident. ursula keeps the **full Raft log + state machine in RAM** (the `disk` WAL only adds
durability, not a smaller footprint — its memory ≈ the in-memory variant), so memory tracks **bytes
resident**. The Node reference sits between (V8 heap; 0.2–1.1 GB).

---

## 4. SSE fan-out — delivery latency (p50, ms)

1 stream, 1 writer @ 50 ev/s, swept total subscribers; one well-provisioned client pod. Writer-paced
→ the metric is per-event end-to-end delivery latency.

| config \ subscribers | 1 | 10 | 100 | 1000 |
|---|---|---|---|---|
| durable wal (cache off) | 1.00 | 1.10 | 1.46 | 4.14 |
| durable wal (cache on) | 1.00 | 1.09 | 1.44 | 3.72 |
| ursula in-memory | 0.99 | 1.10 | 1.42 | 3.28 |
| ursula disk | 1.89 | 2.36 | 2.60 | 4.23 |

All competitive — sub-ms→~4 ms median across the fan-out; ursula-in-memory edges ahead at 1000 subs,
ursula-disk carries a higher baseline.

## 5. SSE fan-out — pod memory vs subscribers — peak / p50 (MiB)

| config \ subscribers | 1 | 10 | 100 | 1000 |
|---|---|---|---|---|
| durable wal (cache off) | 6 / 5 | 7 / 6 | 11 / 10 | 27 / 23 |
| durable wal (cache on) | 6 / 5 | 6 / 6 | 8 / 7 | 26 / 21 |
| ursula in-memory | 15 / 15 | 15 / 15 | 15 / 15 | 16 / 15 |
| ursula disk | 15 / 15 | 15 / 15 | 15 / 15 | 16 / 15 |

**Neither duplicates the stream data per subscriber — the fan-out buffer is shared in both.**
- **ursula is essentially flat** (15→15 MiB over 1→1000 subscribers): it reads its in-memory tail
  once and serves all subscribers; near-zero per-subscriber cost.
- **durable grows modestly** (~5→23 MiB, **~18 KB/subscriber**): the stream data is shared (one
  resident tail / `sendfile`, not copied), so the growth is per-*connection* overhead (each SSE
  subscriber is its own HTTP/1.1 connection with a kernel socket buffer), not duplicated payload.

durable's absolute footprint is tiny at low fan-out (5 MiB vs ursula's 15) but its per-connection
cost crosses above ursula by 1000 subscribers (23 vs 15). Both scale with *connection count*, not data.

---

## Findings

1. **Write throughput:** durable peaks at **~0.79–0.89M append/s** at 4 CPUs — ~7–9× ursula-in-memory,
   ~70–100× ursula-disk, ~10× the Node reference, and ~400× S2.
2. **Write memory is the architectural divider.** durable's footprint tracks **stream count** (per-file
   bookkeeping; tens–hundreds of MiB; p50 ≪ peak) because it keeps no payload resident. ursula's tracks
   **bytes resident** (1–5 GB; p50 ≈ peak) because Raft keeps the whole log + state machine in RAM, disk
   WAL or not. `memory` mode is durable's lightest (769 MiB @ 100k vs 950 for wal).
3. **SSE fan-out memory is shared, not per-subscriber**, in both — ursula flat, durable +~18 KB/subscriber
   (socket overhead). Delivery latency stays sub-ms→~4 ms for all.

## Caveats
- Single-node only (no replication) — these are single-node numbers on equal hardware, not reused  
  any system's published multi-node results. ursula's headline feature (Raft replication) is deliberately
  not exercised.
- Throughput is a saturation ceiling per the pod ladder; some durable cells are mildly non-monotonic
  (run-to-run variance at the ceiling). Memory figures are server-side and stable.
- **100k re-collected with a robust result upload.** At 100k streams + 100+ fleet pods, the per-pod
  `mc cp → MinIO` result uploads stormed MinIO; some timed out, dropping those pods' HDRs and
  *undercounting* throughput (the apparent "collapse" past ~80 pods). The bench-job now retries the
  upload with per-pod-staggered backoff; durable wal/memory @ 100k were re-collected clean (860k /
  786k, up from 771k / 670k). wal degrading past 80 pods (96 → 561k) is then **real** server
  contention at 4 CPU, not lost data.
- ursula/node/s2 are capped at the cardinality where they stop saturating / crash (10k / 1k / 100).

## Reproducing

Per-system suites under `suites/run-*.json` (write) + `scripts/run-sse.sh` (SSE), orchestrated 3
clusters at a time by `scripts/run-matrix.sh`:

    scripts/run-matrix.sh run-durable run-ursula run-s2 run-node     # write + memory
    SSE_SYSTEMS="durable:walnew durable:walnew-cache ursula:memory ursula:disk" \
      ZONE=europe-west4-c scripts/run-sse.sh                          # SSE + memory

Each cell records throughput, p50/p99, and peak/p50 pod memory (`scripts/podmem.py`, run automatically
by `scripts/bench … report`). Raw per-cell data + merged HDRs land under `results/<suite>/`. See
`README.md` for image build (`DS_RUST_REPO` / `DS_NODE_REPO`) and cluster setup.

## Tests

    cd scripts && for t in *_test.py; do python3 "$t"; done
    for t in scripts/*_test.sh; do bash "$t"; done
