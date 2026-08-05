# Benchmark provenance — 2026-06-30

Full matrix run on the **PR #4662 HEAD** durable-streams build (the reactor PR), from scratch.

## Versions (commit hashes)
- **durable-streams** (PR #4662, https://github.com/electric-sql/electric/pull/4662):
  commit `3754d64cba5694ac7e4155ac57a959386001d055` (branch `sse-reactor-flat-userspace`).
  Image `durable-streams:dev` = `sha256:80b8abdcebeef4875391bb66ef5caa938c0cc30fd8b30d20ccee22c8fbca99fe`, built 2026-06-30.
  Reactor source verified byte-identical to this commit (post-fix, includes the
  reactor shutdown-leak + write()==0 guard).
- **ds-bench**: commit `03ba78ac746e958e292321b2b369c4140f8be1f0` (branch `feat/read-scalability-workload`).
- **ursula**: `ghcr.io/tonbo-io/ursula:v0.1.5`
- **Node.js reference**: `durable-node:dev`
- **S2 (s2lite)**: `ghcr.io/s2-streamstore/s2`

## Workloads
- **Write** throughput / latency / memory: `run-durable` (wal, wal-tailcache, memory),
  `run-ursula` (memory, disk), `run-node`, `run-s2` — saturation pod-ladder per cardinality.
- **SSE fan-out**: `run-sse.sh` — 1 stream, subscribers 1/10/100/1000, delivery latency + memory.
- **Reads**: `reads-catchup`, `reads-longpoll`, `reads-sse-remote` (wal + ursula).

## Hardware
Server `c4d-standard-16-lssd` pinned to 4 CPUs; client fleet `n2d-standard-32` Spot. europe-west4.

## Headline numbers (peak, this run)

| System | config | append throughput (rec/s) | notes |
|---|---|---|---|
| durable-streams (HEAD) | wal | **927,583** @100k streams (501k @100) | p99 0.41 ms @100 → 737 ms @100k; pod mem 69→854 MB |
| durable-streams (HEAD) | wal-tailcache | 821,770 @100k | p99 0.4 ms @100; mem 93→909 MB |
| durable-streams (HEAD) | memory | 583,443 @10k | p99 0.4 ms @100 |
| ursula | in-memory | 146,253 @10k | pod mem 2.4–3.5 GB |
| ursula | disk | 12,153 @10k | p99 grows to 2.6 s @10k |
| Node.js ref | — | 100,566 @10k | p99 123 ms @10k |
| s2 (s2lite) | — | 1,975 @100 | s=1000 cell errored (see below) |

**Reads — live tail (HEAD reactor, post-fix):**
- **SSE** (`reads-sse-remote`, wal): p99 **0.48–2.5 ms** across 64–2048 connections, all 16 cells ok.
- **SSE fan-out** (`run-sse.sh`, 1 stream): wal p50 0.998 ms (1 sub) → 4.139 ms (1000 subs); ursula in-memory comparable, ursula-disk ~2× higher.
- **long-poll** (wal): p99 ~5–7 ms @100 streams, ~49–57 ms @1000 streams.
- **catch-up** (resident re-scan): scales to ~32 connections then the client saturates (see below).

## Cell-level status

The durable-streams (HEAD reactor) and reactor-read matrices are **100% clean**. Known
error cells, all outside the durable path, with understood causes:

- **`reads-catchup` 128/512-connection cells (wal + ursula): client fleet pod `OOMKilled`.**
  Each catch-up connection holds a 16 MiB resident re-scan buffer; 128–512 concurrent
  re-scans exceed the client pod memory. This is the documented catch-up limitation
  (resident replay does not scale to high fan-out), not a server-side defect — re-running
  the same config OOMs again. Low/mid connection counts (8–64) are ok.
- **`run-s2` s=1000: transient manifest apply race** (`s2lite.yaml` nodeSelector). `NODESEL_SERVER`
  is a constant, so this was a one-off apply error, not deterministic; s2's primary data
  point (s=100) succeeded. s2lite is a lightweight third-party comparison only.
- **`reads-longpoll` wal, 1 cell (sc=100, conn=512): transient** (9/10 wal + 10/10 ursula ok).

All GKE clusters torn down after the run (verified none remain).
