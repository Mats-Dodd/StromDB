# canonical-write — write-throughput report

## Throughput at saturation (ops/s)

| streams | wal-ideal | memory |
|---|---|---|
| 10000 | 422k† | 680k† |
| 100000 | 388k† | 636k† |

† = not saturated (ladder exhausted) — treat as a lower bound.

## Pod memory at saturation — peak / p50 (MiB)

| streams | wal-ideal | memory |
|---|---|---|
| 10000 | 318 / 221 | 304 / 217 |
| 100000 | 780 / 651 | 726 / 641 |

_Pod working set = cgroup `memory.current − inactive_file` (anon + active page cache), sampled each second at the pinned rung. **peak** = high-water (catches bursts like an in-RAM Raft log filling); **p50** = median (what the server steadily holds resident). peak ≈ p50 ⇒ steadily resident; peak ≫ p50 ⇒ transient spikes._

## Latency (ms, p50 / p99)

| streams | wal-ideal @≤80% load | wal-ideal @saturation | memory @≤80% load | memory @saturation |
|---|---|---|---|---|
| 10000 | 2.3 / 4.3 (422k @4p) | — | 1.5 / 4.0 (620k @4p) | — |
| 100000 | 2.5 / 4.5 (388k @4p) | — | 1.5 / 4.0 (593k @4p) | — |

_@≤80% load = the largest ladder rung at ≤80% of peak throughput — the server's service latency with headroom. @saturation = the pinned plateau rung, where a closed-loop fleet measures its own queueing (≈ in-flight ÷ ceiling by Little's law), NOT the server's per-request cost. Compare against the unloaded single-request baseline (~1 ms for wal) before reading anything into large saturation values._

## Saturation walks (pods → ops/s, p50 ms)

- **memory 10000**: 4:620k@1.5ms → 8:680k@2.8ms  (pinned 8, ladder_exhausted)
- **wal-ideal 10000**: 4:422k@2.3ms → 8:401k@4.9ms  (pinned 8, ladder_exhausted)
- **memory 100000**: 4:593k@1.5ms → 8:636k@2.8ms  (pinned 8, ladder_exhausted)
- **wal-ideal 100000**: 4:388k@2.5ms → 8:372k@5.2ms  (pinned 8, ladder_exhausted)

## Findings

_TODO: written by hand on top of the generated data._

## Caveats

_Single-node best-case; not 3-node Raft. Throughput is a saturation ceiling per the ladder._
