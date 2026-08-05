# ds-bench — a benchmarking system for durable streams

A reproducible, single-node benchmark harness for durable-stream servers: declarative workload suites, a Kubernetes client fleet, and exact cross-fleet HDR-percentile merging, runnable on a local kind cluster or on GKE. Workloads are server-agnostic and run against any supported implementation.

**Currently supported implementations:** **durable-streams** (Rust), the **Node.js reference server** (`@durable-streams/server`), **ursula**, and **S2 (s2lite)**.

Results from a full-matrix run across them: **[`results-2026-06-30/REPORT.md`](results-2026-06-30/REPORT.md)** (an earlier baseline is archived under [`results-2026-06-25/`](results-2026-06-25/)). ⚠️ **These dated snapshots predate the fleet start barrier and the knee-vs-saturation latency methodology** ([`AGENTS.md` §7](AGENTS.md)); their write ceilings are upper bounds and their latency is saturation-rung queueing. See the `write-wal-vs-mem-*` reference points in `AGENTS.md` §7 for methodology-current numbers.

## How it works

A suite is a JSON file (`suites/*.json`) that declares the workload, the systems and configs, and the sweep. `scripts/bench` brings up a cluster, deploys each server fresh, drives a Kubernetes client fleet, merges per-pod HDR histograms into fleet-wide percentiles, records per-cell results under `results/<suite>/`, and tears down. Reports regenerate from local results, no cluster required.

Each workload is its own declarative suite:

- **Write throughput** — *append/s at saturation, plus tail latency and pod memory* — `suites/run-{durable,ursula,s2,node}.json`. Drives concurrent appends across many streams while the client fleet ramps a per-cardinality pod ladder; once server throughput stops climbing it pins the load and confirms the peak append rate, the latency at that peak, and the server's peak pod memory. This *saturation walk* finds the server's ceiling rather than assuming a pod count. The memory figure is the pod cgroup working set (anon plus active page cache), so a resident cache and an OS-paging design are compared on equal terms across every implementation.
- **Sustained load** — *latency and server-memory stability over time* — `suites/sustained.json`. Holds a fixed, modest append rate across a set of streams for a long window and watches whether latency and the server's resident memory stay flat, surfacing slow drift or leaks that a short burst would miss.
- **Catch-up / reconnect** — *per-client catch-up latency and body size* — `suites/catchup-{durable,ursula,s2,node}.json`. Pre-populates a stream, then has many clients reconnect and replay it from the beginning all at once, recording how long each client takes to catch up, how large its response is, and the aggregate replay throughput. Each implementation replays through whatever native read path it offers, whether a snapshot plus the tail since that snapshot or a full scan of the log.
- **SSE fan-out** — *per-event delivery latency and memory vs subscriber count* — `scripts/run-sse.sh`. Has a single writer publish to one stream while a growing number of subscribers stream it, measuring the per-event end-to-end delivery latency as the fan-out widens.
- **Read scalability** — *delivery/replay latency vs concurrent-connection count, across three read modes* — `suites/reads-{catchup,longpoll,sse-remote}.json`. Sweeps a growing ladder of concurrent read connections against the same stream set and compares how each read path holds up: **catch-up** is a hot resident re-scan (every reader downloads the full stream); **long-poll** and **sse** tail new appends fed by a light per-stream writer, so their metric is per-record delivery latency. This surfaces where the resident-per-reader replay model hits a ceiling and where the streamed, shared-buffer paths keep scaling. `*-local` variants (`reads-local.json`, `reads-sse-local.json`) fit a single kind node.

## Prerequisites

- `kubectl`, `python3` (3.x, stdlib only), Docker.
- **Local:** [kind](https://kind.sigs.k8s.io/).
- **Remote:** `gcloud` authenticated; an Artifact Registry repo. Override `PROJECT` (defaults to `gcloud config get-value project`), `AR_LOCATION` (default `europe-west1`), `AR_REPO` (default `ds-bench`), `ZONE`, and the machine types (`SERVER_MACHINE`, `CLIENT_MACHINE`) for your environment.
- The `durable-streams` server source checked out alongside this repo, only if you build its image yourself. ursula and S2 use upstream-published images (`ghcr.io/tonbo-io/ursula`, `ghcr.io/s2-streamstore/s2`), so there is no source to vendor.

## Quick start (local kind)

```bash
DS_TARGET=local scripts/cluster-up.sh     # kind cluster + MinIO + metrics ConfigMap
DS_TARGET=local scripts/build-images.sh   # build server + ds-bench images, load into kind

# `*-local` suites use small ladders/counts that fit a single kind node:
DS_TARGET=local scripts/bench suites/write-throughput-local.json run     # run a workload
DS_TARGET=local scripts/bench suites/catchup-local.json run              # another workload
scripts/bench suites/write-throughput-local.json report                  # (re)generate its report

DS_TARGET=local scripts/cluster-down.sh   # tear down
```

`DS_TARGET=local` runs everything against the kind cluster, with no cloud and no teardown of kind. The full `suites/*.json` are sized for a multi-node GKE cluster; for kind, use the `*-local` suites (or copy one and shrink `stream_counts` / `pod_ladder` / `clients`). `run` writes `results/<suite>/`, and re-running resumes, skipping finished cells.

## Remote (GKE)

```bash
PROJECT=my-project scripts/build-images.sh        # Cloud Build → Artifact Registry
PROJECT=my-project scripts/bench suites/run-durable.json run   # one system
PROJECT=my-project scripts/run-matrix.sh                       # all systems, ≤3 GKE clusters in parallel
```

`scripts/cluster-up.sh` (invoked by `bench`) creates a server node pool (one node, server CPU-pinned) and a Spot client pool. See `scripts/target-env.sh` for all overridable env (registry, zone, machine types, pull policy). Remote clusters are billable: they tear down on clean completion, and `scripts/teardown-watchdog.sh` is a deadline safety net.

## Supported implementations

A workload is server-agnostic: the same suite runs against any supported implementation, chosen by the suite's `modes` and the server image that gets deployed. Every server points at the same single-node MinIO, and only the system under test is running while it is measured.

- **durable-streams** (Rust) — the server this harness was built alongside. Runs WAL-backed (`--durability wal`, a sharded committer, with or without the resident tail cache) or without a WAL (`--durability memory`); `suites/run-durable.json` runs the wal / wal-tailcache / memory variants side by side.
- **Node.js reference server** (`@durable-streams/server`) — the protocol's reference implementation. It shares the wire protocol, so it reuses the `durable` API style and runs as mode `node` (in-memory storage). Being TypeScript rather than a compiled binary, its image is built from the `../durable-streams` monorepo (pnpm workspace, started under Node) — see `dockerfiles/durable-node.Dockerfile`; `build-images.sh` builds it by default (`BUILD_NODE=0` to skip).
- **ursula** — a single-node Raft server. The storage backend is chosen at deploy time via `URSULA_WAL` (`memory` keeps the log in RAM; `disk`, the default, writes a WAL and fsyncs on every commit), so `suites/run-ursula.json` covers both.
- **S2** (`s2lite`) — object-store-backed.

Adding another implementation comes down to a deployment manifest, a `ds-bench` API style for its wire protocol, and a few addressing lines in `deploy_mode` and `reset_state` in `scripts/lib-bench.sh`.

## Results & reproducing

- **Run** the write-throughput suites individually (`scripts/bench suites/run-<system>.json run`) or all at once with `scripts/run-matrix.sh` (≤3 GKE clusters in parallel); the read-scalability modes run with `scripts/bench suites/reads-<mode>.json run`.
- **Raw data:** each run writes its per-cell data under `results/<suite>/` — the `cells.json` result-and-resume store, the merged HDR histograms, and the sidecar `samples.csv`.
- **Published dataset:** the report and curated per-cell data for a run are snapshotted into a dated directory. The latest is **[`results-2026-06-30/`](results-2026-06-30/)** (full matrix on the durable-streams reactor build, with read-scalability); the **[`results-2026-06-25/`](results-2026-06-25/)** baseline is archived alongside it. Each snapshot carries a `REPORT.md` and a `PROVENANCE.md` (commit hashes, image digests, cell-level status). **⚠️ Both dated snapshots predate the current methodology** (fleet start barrier + window-alignment check, and knee-vs-saturation latency — [`AGENTS.md` §7](AGENTS.md)): their write throughput is an upper bound (unbarriered per-pod-rate sums can multiply-count capacity) and their quoted latency is saturation-rung queueing, not service latency. A methodology-current full-matrix snapshot has not yet been re-published; until then treat these as provenance, and see `AGENTS.md` §7 for corrected reference points.
- **Regenerate reports** (purely from local files, no cluster): `scripts/bench suites/<suite>.json report` for most workloads, `python3 scripts/catchup_report.py suites/catchup-*.json` for catch-up, and `scripts/run-sse.sh` for SSE.

## Tests

Framework logic is unit-tested, no cluster required:

```bash
cd scripts && for t in *_test.py; do python3 "$t"; done
for t in scripts/*_test.sh; do bash "$t"; done
```

These cover the suite loader, the per-cell result stores, the saturation classifier, the catch-up / sustained / read-scalability runners, and the report renderers.

## Fairness & disclosure

These benchmarks target **single-node deployments**. Within that scope, every run is kept on equal footing — one node per server, identical workload parameters, a shared single-node MinIO, fresh data each run, and only the system under test running while it is measured — and all numbers are generated by `ds-bench` on equal hardware, not reused from any implementation's published results.

## Future work

Extend the harness to replicated and other deployment topologies; the current workloads and suites assume a single node.

## Acknowledgements

The benchmark methodology is based on ursula's published benchmark ([ursula.tonbo.io/benchmark](https://ursula.tonbo.io/benchmark)); `ds-bench` is derived from its `ursula-bench` (Apache-2.0).
