# Provenance — canonical campaign 2026-07-23

**Single-build campaign** — every durable-streams suite ran the same image.

- **Server:** release tag `@electric-ax/durable-streams-server-rust@0.1.5` =
  electric-sql/electric `88793e765` ("chore: publish new package versions
  (#4694)", 2026-07-22). Content relative to the 2026-07-14 campaign builds:
  exactly the electric#4710 recovery-hardening squash (`012dc4acc`) + the
  version bump — i.e. the same code the #4710 write re-validation ran, now
  exercised by ALL suites. Built from a detached git worktree at the tag.
- **Images (Artifact Registry manifest digests, europe-west1/vaxine/ds-bench):**
  - `durable-streams:dev` `sha256:8f46351c1bb63bdf6ce163eff38e5aa4d268374084e5b838e1f64f4a072ebefd`
  - `ds-bench:dev` `sha256:3622a499aecf9f2f4047ebf8ade1a71ba6ddba7be7e5149e8aa6dca7d602d3c3`
  - (durable-node not built — no canonical suite uses mode `node`.)
- **Ursula:** upstream `ghcr.io/tonbo-io/ursula:v0.2.0` (memory + disk WAL arms).
- **Hardware:** `canonical-write` + the `wal-tune-*` addendum on
  `c4d-standard-64-lssd` raw-block (`STATIC_CPU=1 GUARANTEED=1
  SERVER_LOCAL_SSD_BLOCK=1 SPOT_SERVER=1`; canonical-write + wal-tune-cpu16 on
  the splitlane3x3 manifest, wal-tune-1x5 on the 1×5 splitlane-guaranteed
  manifest; server pinned to 8 cores except wal-tune-cpu16 = 16).
  ursula/reads/mixed on `c4d-standard-16-lssd` stock manifests. Clients
  `n2d-standard-32` Spot. Region europe-west4. `PULL_POLICY=IfNotPresent`
  (image fixed for the whole campaign).
- **Harness:** ds-bench `main` @ `1671f86` + the two `wal-tune-*.json` suites
  added this campaign (committed with these results).
- **Orchestration:** phase 1 in parallel (canonical-write / canonical-write-
  ursula / mixed chain, ≤3 clusters), phase 2 sequential (reads-catchup →
  reads-sse), then the mixed-writes re-run and the tuning arms. Watchdogs
  scoped per lane; **zero `bench-*` clusters at campaign end (verified)**.

## Cell-level status

- **All suites completed rc=0.** Error/artifact cells:
  - `canonical-reads-catchup` ursula n100 × {8,32,128,512} conns: ERR
    (seeding choke, ursula's historic client-OOM ceiling at this point — same
    gap as 2026-07-14). Deliberately not re-run (operator decision).
  - `canonical-mixed-writes` run 1, n10000-l100000: transient collapse
    (665 write ops/s, 105,839 + 11,902 transport-class errors). Full fresh
    re-run on a new cluster reproduced 2026-07-14 exactly (50,033 writes/s,
    4,987 reads/s, zero errors) → environment artifact, not the build. The
    published `results/canonical-mixed-writes/` is the clean re-run; run-1 raw
    results preserved off-repo (session scratchpad `mixed-writes-run1`).
  - `canonical-reads-sse` wal n100: p99 11–42 ms (was 1–3 ms) with throughput
    unchanged and ursula's own-cluster numbers identical across campaigns —
    suspected single-cluster environment artifact; flagged for re-check next
    campaign, not re-run.
- Saturation labels: wal write cells quoted at the 4-pod peak rung (8-pod rung
  declines); memory write cells `ladder_exhausted` = lower bounds.
- `wal-tune-cpu16` is a true `plateau` (4/8/12 pods within 2%).
