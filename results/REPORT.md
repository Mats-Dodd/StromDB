# ds-bench canonical campaign — 2026-07-23 (release 0.1.5)

Second run of the canonical suite set, on the first tagged release of the Rust
server (`@electric-ax/durable-streams-server-rust@0.1.5` — content: the
electric#4710 recovery-hardening squash + version bump). Provenance in
`PROVENANCE.md`; per-suite grids in each subdirectory. Includes a two-arm WAL
tuning addendum (§5).

## 1. Write saturation (`canonical-write`, `canonical-write-ursula`)

Peak append/s at saturation (256 B payloads):

| streams | wal-ideal | memory | ursula-mem | ursula-disk |
|---|---|---|---|---|
| 100  | — | — | 48.4k | 4.4k |
| 1k   | — | — | 54.6k | 7.0k |
| 10k  | 422.4k | 680.4k† | 48.9k | 8.4k |
| 100k | **388.2k** | **636.4k†** | — | — |

- **No cardinality cliff: −8% (wal) / −6% (memory) from 10k→100k.** Regression
  gates (wal@100k > 250k, memory@100k > 400k) pass with wide margin. Both
  slightly above the 2026-07-14 campaign (wal +1.5%, memory +0.8%).
- The classifier labels all four durable-streams cells `ladder_exhausted`.
  For **wal** the 4-pod rung is the peak and the 8-pod rung *declines*
  (422k→401k @10k, 388k→372k @100k), so the quoted numbers are effective
  ceilings. For **memory** (†) throughput was still climbing at the last rung
  (620k→680k, 593k→636k) — treat as lower bounds.
- Physics sanity: memory > wal everywhere.

## 3. Read scalability (`canonical-reads-catchup`, `canonical-reads-sse`)

**Catch-up** (MiB/s @ p99 ms): wal is cardinality-flat — ~2.39 GiB/s @234ms
(32 conns) to ~2.77 GiB/s (512 conns) at BOTH 10 and 100 streams, matching
2026-07-14 at every rung except the light 8-conn rung (1.4 vs 2.3 GiB/s —
under-load variance, peak unaffected). ursula matches at 10 streams
(2.9 GiB/s peak, worse p99: 10.6s vs 4.9s at 512) and errors at every level at
100 streams (recorded as a gap — its historic client-OOM ceiling, same as
2026-07-14; cells intentionally not re-run).

**SSE tail** (per-connection paced): wal throughput flat at both cardinalities
up to 2048 conns (25 MiB/s peak). p99 at 100 streams measured 11–42 ms this
campaign vs 1–3 ms on 2026-07-14, while ursula's profile is unchanged
(40–62 ms, identical to last campaign) — see Known gaps: attributed to
environment on the wal cluster, not the server, pending a re-run.

## 4. Mixed read/write interference (`canonical-mixed-*`)

- **Anchor:** 81.6k ops/s single-pod mixed-shape ceiling (was 81.7k — stable).
- **Paced readers vs pinned writes (10k streams, 50k ops/s pinned):** writes
  hold 49.9–50.0k at 0 / 1k / 10k / 100k readers, zero errors, while serving up
  to 4,987 replays/s. **The premise holds: 100k concurrent catch-up readers
  cost the write path nothing.** (Published cells are the clean re-run; the
  first pass hit a transient environment collapse at the 100k-reader level —
  see Known gaps.)
- **Delivery under write load (2000 SSE subscribers):**

| writes/s | wal del/s (p99 ms) | memory del/s (p99 ms) |
|---|---|---|
| 4k  | 4.0k (132) | 3.3k (132) |
| 16k | 16.0k (63) | 15.9k (4) |
| 40k | 33.2k (123) | 39.8k (32) |
| 66k | 65.6k (111) | 65.7k (21) |
| max | 74.4k @ 75k writes (134) | **126.3k @ 127k writes (62)** |

  memory delivery tracks writes 1:1 to 127k/s (same as 2026-07-14). wal's max
  rung landed at 75k writes / 74.4k del/s vs 84k/64k last campaign — better
  delivery ratio at a slightly lower write point (max-rung trade-off noise);
  sub-max rungs match across campaigns.

## 5. WAL tuning addendum (`wal-tune-1x5`, `wal-tune-cpu16` — non-canonical)

Question: does any configuration unlock more WAL-mode performance? @100k
streams, same c4d-standard-64-lssd raw-block hardware:

| configuration | ops/s | verdict |
|---|---|---|
| 3×3 lanes, 8 pinned cores (canonical) | 388k | baseline |
| 1×5 lanes, 8 pinned cores | 387k | tie — disk layout is NOT the bottleneck |
| 3×3 lanes, **16 pinned cores** | **537k** | **+38%, true plateau** (4/8/12 pods: 543k/537k/534k) |

At 8 cores WAL mode is CPU-bound, not disk-bound. Doubling pinned cores buys
+38% (sub-linear: the residual is WAL staging/double-write machinery — the
io_uring segment writer / batched mark-written seam). WAL @16 cores reaches
~85% of memory mode @8 cores.

## Known gaps & artifacts

- ursula catch-up @100 streams: ERR at all connection levels (historic
  client-OOM ceiling, same as 2026-07-14) — gap, not a zero. Per operator
  decision these cells were not re-run this campaign.
- `mixed-writes` first pass: the 100k-reader cell collapsed (665 write ops/s,
  105k transport-class read errors). A full fresh re-run reproduced the
  2026-07-14 numbers exactly (50k writes/s, zero errors), and the automated
  #4697-vs-0.1.5 bisection was therefore not triggered. Verdict: transient
  environment (Spot node) artifact. Run-1 raw results preserved off-repo.
- `reads-sse` wal @100 streams p99 (11–42 ms vs 1–3 ms on 2026-07-14):
  single-cluster artifact suspected (ursula's numbers on its own cluster are
  identical across campaigns; and the mixed-writes collapse on another cluster
  the same day proved transient). Not re-run; flag for the next campaign.
- SSE fan-out (single-stream subscriber ladder, `run-sse.sh`) not run this
  campaign (also skipped on 2026-07-14).
- Write cells: wal quoted at its 4-pod peak (8-pod rung declines); memory cells
  are ladder-bounded lower bounds (†).

## Pre-publication checklist

Layout verified (splitlane3x3 raw-block for canonical-write; `--data-dir
/data/wal/0` on device 0) ✓ · aligned windows ✓ · plateau-vs-ladder labeled per
cell ✓ · digests in PROVENANCE.md ✓ · physics sanity (memory > wal everywhere;
537k @16c < 636k memory) ✓ · cardinality shape reported (−8%/−6%, no cliff) ✓ ·
client-bound cells marked as gaps ✓
