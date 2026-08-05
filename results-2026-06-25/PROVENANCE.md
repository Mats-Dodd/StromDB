# Benchmark provenance — 2026-06-25

Baseline run: **write scalability + server memory + SSE fan-out + catch-up**, across the
durable-streams Rust server and the comparison systems. This is the predecessor to
`results-2026-06-30/` (which re-measured on the PR #4662 reactor build); see `REPORT.md`
here for the full writeup.

## Versions (commit hashes)
- **durable-streams** (Rust, `electric/packages/server-rust`): **pre-reactor baseline** —
  PR #4652 "Rust Durable Streams server" (`af30a624f`). This is the server **before** the
  SSE fan-out per-subscriber memory cut (`66307c746`, 2026-06-29) and **before** the live-tail
  SSE epoll reactor (`4967b8406` / fix `3754d64cb`). Configs measured: `wal`, `wal-tailcache`
  (`--tail-cache-bytes 65536`), `memory` (`--durability memory`).
  > Note: the original 2026-06-25 run recorded only the date (`server-rust @ 2026-06-25`), not a
  > git sha; `af30a624f` is the corresponding pre-optimization server commit for that PR.
- **ds-bench**: commit `205e770` ("chore: curate results/ for publication", 2026-06-25).
- **ursula**: `ghcr.io/tonbo-io/ursula:v0.1.5`
- **Node.js reference**: `@durable-streams/server` (`durable-streams/packages/server`), in-memory.
- **S2 (s2lite)**: `ghcr.io/s2-streamstore/s2`

## Workloads
- **Write throughput / latency / memory** (`write-throughput/`): durable wal / wal-tailcache /
  memory, ursula memory / disk, node, s2 — saturation pod-ladder per cardinality.
- **Memory saturation** (`saturation/`): node, ursula-disk, ursula-memory.
- **SSE fan-out** (`sse/`): 1 stream, swept subscribers — delivery latency + shared-buffer memory.
- **Catch-up** (`catchup/`, `catchup-node/`): Ursula-methodology catch-up replay.

## Hardware
Server `c4d-standard-16-lssd` pinned to 4 CPUs; client fleet `n2d-standard-32` Spot. Single-node
(local-style) layout. 256-byte binary appends.
