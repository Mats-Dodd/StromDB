# wal-tune-cpu16 — write-throughput report

## Throughput at saturation (ops/s)

| streams | wal-3x3-16c |
|---|---|
| 100000 | 537k |

† = not saturated (ladder exhausted) — treat as a lower bound.

## Pod memory at saturation — peak / p50 (MiB)

| streams | wal-3x3-16c |
|---|---|
| 100000 | 732 / 658 |

_Pod working set = cgroup `memory.current − inactive_file` (anon + active page cache), sampled each second at the pinned rung. **peak** = high-water (catches bursts like an in-RAM Raft log filling); **p50** = median (what the server steadily holds resident). peak ≈ p50 ⇒ steadily resident; peak ≫ p50 ⇒ transient spikes._

## Latency (ms, p50 / p99)

| streams | wal-3x3-16c @≤80% load | wal-3x3-16c @saturation |
|---|---|---|
| 100000 | 1.7 / 4.4 (543k @4p) | 1.7 / 4.1 |

_@≤80% load = the largest ladder rung at ≤80% of peak throughput — the server's service latency with headroom. @saturation = the pinned plateau rung, where a closed-loop fleet measures its own queueing (≈ in-flight ÷ ceiling by Little's law), NOT the server's per-request cost. Compare against the unloaded single-request baseline (~1 ms for wal) before reading anything into large saturation values._

## Saturation walks (pods → ops/s, p50 ms)

- **wal-3x3-16c 100000**: 4:543k@1.7ms → 8:537k@3.5ms → 12:534k@5.4ms  (pinned 4, plateau)

## Findings

_TODO: written by hand on top of the generated data._

## Caveats

_Single-node best-case; not 3-node Raft. Throughput is a saturation ceiling per the ladder._
