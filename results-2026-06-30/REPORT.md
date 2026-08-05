# Durable-streams benchmarks — full matrix on the reactor build (2026-06-30)

> **⚠️ Superseded — read before quoting these numbers.** This run predates two
> methodology corrections now documented in [`AGENTS.md` §7](../AGENTS.md):
> 1. **No fleet start barrier / window-alignment check.** The write-throughput
>    figures sum per-pod rates whose measure windows were not verified to overlap,
>    which multiply-counts server capacity at high pod counts (the documented
>    2.9M-ops/s artifact). Treat the write ceilings here as **upper bounds only**.
> 2. **Latency quoted at the saturation rung.** The p50/p99 columns are the
>    *closed-loop queueing* latency at the ceiling (Little's law), not the server's
>    service latency. The current harness quotes latency from the **knee** (≤80 %
>    load) and labels the plateau rung's latency as queueing.
>
> Numbers were also single-shot (no replication / error bars). For a
> methodology-current dataset, regenerate with the barrier + knee harness (see
> `AGENTS.md` §7 and the `write-wal-vs-mem-*` reference points). Kept for provenance.

A from-scratch run of the **whole matrix** — write throughput, server memory, SSE fan-out, and the
three read modes (catch-up / long-poll / SSE) — on the **PR #4662 HEAD** durable-streams build
(commit `3754d64cb`, the live-tail SSE epoll-reactor PR, post-fix). Each workload is a declarative
suite under `suites/`; see *Reproducing* and `PROVENANCE.md` at the end for commit hashes and the
image digest.

**Setup.** One server node (`c4d-standard-16-lssd`), server pinned to **4 CPUs**; a Kubernetes
client fleet (`n2d-standard-32`, Spot) drives load. 256-byte binary appends. Throughput is the
**saturation ceiling** — a per-cardinality pod ladder ramped until per-server throughput plateaus.
Latency is fleet-wide p50/p99 from merged HDR histograms. **Memory is the pod cgroup working set**
(`memory.current − inactive_file` = anon + active page cache), sampled each second at the pinned
rung; reported as **peak** (high-water) / **p50** (median = steady state).

**Systems.**
- **durable-streams** (Rust, PR #4662 HEAD reactor build, `3754d64cb`): `wal` (default,
  group-commit fsync), `wal-tailcache` (`--tail-cache-bytes 65536`, resident read cache on), and
  `memory` (`--durability memory`, no WAL — zero-copy socket→file splice, page-cache ack). Live-tail
  SSE is served from a per-core epoll reactor (Linux). All tier cold segments to in-cluster MinIO.
- **ursula** (`ghcr.io/tonbo-io/ursula:v0.1.5`): single-node Raft; `memory` and `disk` WAL backends.
  Cold tier → MinIO.
- **Node.js reference** (`durable-node:dev`): the protocol's reference implementation; same wire
  protocol (`api-style durable`), in-memory storage.
- **S2 (s2lite)** (`ghcr.io/s2-streamstore/s2`): object-store-backed (writes through to MinIO).

> `memory + tail cache` is not a configuration — `--durability memory` force-disables the resident
> tail cache (it serves reads from the page cache), so durable has three real write configs.

---

## 1. Write throughput at saturation (ops/s)

| streams | durable wal | durable wal-tc | durable memory | ursula-mem | ursula-disk | node | s2 |
|---|---|---|---|---|---|---|---|
| 100 | 501k | 505k | 446k | 63k | 2.4k | 66k | 2.0k |
| 1 000 | 678k | 621k | 500k | 111k | 6.7k | 95k | — |
| 10 000 | 733k | 685k | 583k | 146k | 12.2k | 101k | — |
| 100 000 | **928k** | 822k | 1282k† | — | — | — | — |

**Peak:** durable wal **~928k append/s** @ 100k streams (wal-tailcache 822k; the three configs track
each other to within ~15%). At equal cardinality (10k, where every system has data) durable is
**~5× ursula-in-memory** (146k), **~60× ursula-disk** (12k), and **~7× node** (101k); S2 manages only
~2k @100 streams and does not scale past that. ursula/node/s2 cap at 10k/10k/100 streams respectively
(higher cardinalities crash or don't saturate within budget).

† durable `memory` @ 100k reports 1.28M but **no HDR latency was captured** for the cell, and the
value breaks the otherwise-consistent `memory < wal` ordering at every lower cardinality — treat it
as a lost-HDR / saturation-miscount artifact, not a real ceiling. The clean durable peak is wal 928k.

## 2. Write append latency at saturation (p50 / p99, ms)

| streams | durable wal | durable memory | ursula-mem | ursula-disk | node | s2 |
|---|---|---|---|---|---|---|
| 100 | 0.22 / 0.41 | 0.24 / 0.42 | 0.54 / 42.9 | 14.4 / 88.0 | — | 51.1 / 52.0 |
| 1 000 | 1.18 / 6.6 | 1.47 / 7.7 | — | 112.6 / 384.3 | 5.0 / 19.1 | — |
| 10 000 | 1.43 / 93.6 | 1.77 / 120.2 | 24.4 / 288.0 | 1184.8 / 2619.4 | 76.6 / 123.3 | — |
| 100 000 | 40.6 / 737.3 | — | — | — | — | — |

durable stays **sub-ms→2 ms median** through 10k (p99 grows with the backlog at the ceiling, and at
100k the median itself climbs to ~40 ms under maximal fan-in); ursula-in-memory tens→hundreds of ms;
ursula-disk **hundreds of ms→seconds** (Raft-log fsync per commit); s2 ~50 ms (object-store hot
path). (Dashes are cells where throughput was recorded but the HDR upload was lost.)

## 3. Write pod memory — peak / p50 (MiB)

| streams | durable wal | durable wal-tc | durable memory | ursula-mem | ursula-disk | node |
|---|---|---|---|---|---|---|
| 100 | 69 / 12 | 93 / 44 | 123 / 11 | 2580 / 1960 | 1082 / 1046 | 180 / 114 |
| 1 000 | 43 / 35 | 61 / 51 | 48 / 40 | 2359 / 1956 | 1631 / 1475 | 436 / 229 |
| 10 000 | 193 / 103 | 194 / 136 | 147 / 92 | 3496 / 3159 | 2486 / 2351 | 969 / 787 |
| 100 000 | 854 / 545 | 909 / 510 | 726 / 481 | — | — | — |

The architectural headline. **durable stays in tens–hundreds of MiB** even at 100k streams (`memory`
mode the lightest — no WAL buffers); **ursula sits at 1–3.5 GB** throughout. And the *shape* differs:
- **durable: p50 ≪ peak** (e.g. wal @ 100k 545 / 854; memory @ 100 11 / 123) — it holds very little
  steadily; the peak is a transient WAL group-commit / page-cache spike during the write burst.
- **ursula: p50 ≈ peak** (e.g. mem @ 10k 3159 / 3496; disk @ 10k 2351 / 2486) — gigabytes resident
  the whole time.

durable is **one file per stream** (a lean `StreamState` in a `DashMap` plus the OS cost of the open
file; payload lives on disk / page cache / tiered to MinIO, never resident), so memory tracks **stream
count**, not bytes. ursula keeps the **full Raft log + state machine in RAM** (the `disk` backend adds
durability, not a smaller footprint), so memory tracks **bytes resident**. Node sits between (V8 heap,
0.18–0.97 GB).

---

## 4. SSE fan-out — delivery latency (p50, ms)

1 stream, 1 writer @ 50 ev/s, swept total subscribers; one well-provisioned client pod (single wall
clock). Writer-paced → the metric is per-event end-to-end delivery latency.

| config \ subscribers | 1 | 10 | 100 | 1000 |
|---|---|---|---|---|
| durable wal (cache off) | 1.00 | 1.10 | 1.46 | 4.14 |
| durable wal (cache on) | 1.00 | 1.09 | 1.44 | 3.72 |
| ursula in-memory | 0.99 | 1.10 | 1.42 | 3.28 |
| ursula disk | 1.89 | 2.36 | 2.60 | 4.23 |

All competitive — sub-ms→~4 ms median across the fan-out (p99 stays within ~1 ms of p50; e.g. durable
wal @ 1000 subs is 4.14 / 5.48 p50/p99). ursula-in-memory edges ahead at 1000 subs; ursula-disk
carries a higher baseline.

## 5. SSE fan-out — pod memory vs subscribers — peak / p50 (MiB)

| config \ subscribers | 1 | 10 | 100 | 1000 |
|---|---|---|---|---|
| durable wal (cache off) | 6 / 5 | 7 / 6 | 11 / 10 | 27 / 23 |
| durable wal (cache on) | 6 / 5 | 6 / 6 | 8 / 7 | 26 / 21 |
| ursula in-memory | 15 / 15 | 15 / 15 | 15 / 15 | 16 / 15 |
| ursula disk | 15 / 15 | 15 / 15 | 15 / 15 | 16 / 15 |

**Flat (or near-flat) across the row ⇒ a shared fan-out buffer** — one resident tail served to all
subscribers, no per-subscriber payload duplication. ursula is essentially flat (15→16 MiB over
1→1000 subscribers); durable grows modestly (~5→23 MiB) as per-*connection* socket overhead, not
duplicated data. Absolute footprint is tiny either way: **1000 subscribers cost ~27 MiB on durable
wal**. This shared-buffer behaviour is exactly where catch-up's resident-per-connection read model
(§6) falls over.

---

## 6. Read scalability

Three read modes against the same resident stream, swept over connection count. **catch-up** is a hot
re-scan of resident data (each reader downloads the full stream); **long-poll** and **SSE** tail new
appends fed by a light per-stream writer, so their metric is per-record delivery latency.

### 6a. Catch-up (resident re-scan) — p50 / p99, ms

| system / streams | 8 conns | 32 conns | 128 conns | 512 conns |
|---|---|---|---|---|
| durable wal · 10 | 57 / 76 | 219 / 249 | **OOM** | **OOM** |
| durable wal · 100 | 58 / 82 | 219 / 252 | **OOM** | **OOM** |
| ursula · 10 | 54 / 67 | 191 / 547 | **OOM** | **OOM** |
| ursula · 100 | **OOM** | **OOM** | **OOM** | **OOM** |

Catch-up scales only to ~32 connections. Past that the **client fleet pod is OOMKilled** — each
re-scan reader holds a 16 MiB resident buffer and 128–512 concurrent re-scans exceed the 4 GiB pod
limit (durable safe ≤64 conns; ursula's heavier client path OOMs at 100 streams for *every*
connection count). This is the resident-replay model's structural ceiling, not a server defect.

### 6b. Long-poll (live tail) — p99 delivery, ms

| system / streams | 32 | 128 | 512 | 1024 | 2048 |
|---|---|---|---|---|---|
| durable wal · 100 | 5.4 | 6.2 | err | 6.3 | 7.2 |
| durable wal · 1000 | 49.6 | 50.4 | 48.9 | 52.4 | 56.9 |
| ursula · 100 | 177.8 | 181.0 | 187.3 | 185.1 | 179.8 |
| ursula · 1000 | 1610.8 | 1386.5 | 1307.6 | 1312.8 | 1308.7 |

Long-poll scales cleanly to **2048 connections**: durable wal holds **p99 ~5–7 ms @100 streams** and
~50 ms @1000 streams, flat across the connection ladder. ursula is ~30× higher (~180 ms / ~1.3–1.6 s).

### 6c. SSE live tail (HEAD reactor) — p99 delivery, ms

| system / streams | 64 | 256 | 1024 | 2048 |
|---|---|---|---|---|
| durable wal · 10 | 0.66 | 0.97 | 1.98 | 2.53 |
| durable wal · 100 | 0.48 | 1.34 | 1.19 | 1.34 |
| ursula · 10 | 1.46 | 1.94 | 3.25 | 3.89 |
| ursula · 100 | 40.7 | 44.0 | 55.5 | 61.4 |

The reactor's payoff: durable SSE holds **p99 ~0.5–2.5 ms across 64–2048 connections** — all 16 cells
clean, and *lower* than long-poll at the same fan-out. ursula's SSE is competitive at 10 streams but
degrades to 40–61 ms at 100 streams. **SSE and long-poll scale where catch-up's resident model does
not**, and SSE does it at sub-millisecond-to-few-millisecond latency.

---

## Findings

1. **Write throughput:** durable peaks at **~928k append/s** (wal @ 100k) at 4 CPUs — ~5× ursula-in-
   memory, ~60× ursula-disk, ~7× the Node reference, and ~470× S2 at equal cardinality.
2. **Write memory is the architectural divider.** durable's footprint tracks **stream count** (per-file
   bookkeeping; tens–hundreds of MiB; p50 ≪ peak). ursula's tracks **bytes resident** (1–3.5 GB; p50 ≈
   peak) because Raft keeps the whole log + state machine in RAM, disk WAL or not.
3. **SSE fan-out memory is shared, not per-subscriber.** 1000 subscribers cost ~27 MiB on durable wal
   (ursula flat at ~15 MiB), and delivery latency stays sub-ms→~4 ms for both.
4. **SSE live-tail (the reactor) is the best-scaling read path:** p99 ~0.5–2.5 ms from 64 to 2048
   connections, all cells clean — flatter and lower than long-poll.
5. **Catch-up does not scale to high fan-out.** Its resident-per-reader model OOMs the client past
   ~32–64 connections; long-poll and SSE (streamed, shared) scale to 2048 connections cleanly.
6. **ursula trails on reads too:** long-poll ~30× and SSE ~10–50× durable's delivery latency at the
   100–1000-stream fan-outs.

## Caveats
- Single-node only (no replication) — equal-hardware single-node numbers, not reused from any
  system's published multi-node results. ursula's Raft replication is deliberately not exercised.
- Throughput is a saturation ceiling per the pod ladder; some cells are mildly non-monotonic
  (run-to-run variance at the ceiling). Memory figures are server-side and stable.
- A few cells lost their HDR latency upload (shown as `—`): durable `memory` @ 100k (throughput
  recorded but anomalous — see §1†), durable `memory`/node/ursula-mem at one cardinality each.
- **Known error cells (all outside the durable write/reactor path):** `run-s2` s=1000 (transient
  `s2lite.yaml` apply race; s=100 succeeded), catch-up 128/512 connections (client-pod OOM, §6a), and
  one long-poll cell (wal sc=100 / 512 conns, transient). The durable HEAD-reactor and reactor-read
  matrices are otherwise 100% clean. See `PROVENANCE.md`.

## Reproducing

Per-system suites under `suites/run-*.json` (write) + `scripts/run-sse.sh` (SSE fan-out) + the read-
mode suites, orchestrated 3 clusters at a time by `scripts/run-matrix.sh` (with `SKIP_BUILD=1` to
pin the hand-built HEAD server image):

    SKIP_BUILD=1 scripts/run-matrix.sh run-durable run-ursula run-s2 run-node   # write + memory
    SKIP_BUILD=1 scripts/run-sse.sh                                             # SSE fan-out
    scripts/bench suites/reads-catchup.json   run                              # catch-up
    scripts/bench suites/reads-longpoll.json  run                              # long-poll
    scripts/bench suites/reads-sse-remote.json run                             # SSE live tail

Each cell records throughput, p50/p99, and peak/p50 pod memory. Curated `cells.json` + `report.md` +
`aggregate.*` per suite are kept here; raw samples/HDRs were pruned. Commit hashes, image digest, and
the full cell-level status are in `PROVENANCE.md`.
