# run-ursula — write-throughput report

## Throughput at saturation (ops/s)

| streams | ursula-memory | ursula-disk |
|---|---|---|
| 100 | 63k | 2k |
| 1000 | 111k† | 7k |
| 10000 | 146k | 12k |

† = not saturated (ladder exhausted) — treat as a lower bound.

## Pod memory at saturation — peak / p50 (MiB)

| streams | ursula-memory | ursula-disk |
|---|---|---|
| 100 | 2580 / 1960 | 1082 / 1046 |
| 1000 | 2359 / 1956 | 1631 / 1475 |
| 10000 | 3496 / 3159 | 2486 / 2351 |

_Pod working set = cgroup `memory.current − inactive_file` (anon + active page cache), sampled each second at the pinned rung. **peak** = high-water (catches bursts like an in-RAM Raft log filling); **p50** = median (what the server steadily holds resident). peak ≈ p50 ⇒ steadily resident; peak ≫ p50 ⇒ transient spikes._

## Saturation walks (pods → ops/s)

- **ursula-disk 100**: 4:2k → 8:2k  (pinned 4, plateau)
- **ursula-memory 100**: 4:63k → 8:65k  (pinned 4, plateau)
- **ursula-disk 1000**: 4:7k → 8:5k  (pinned 4, plateau)
- **ursula-memory 1000**: 4:86k → 8:99k → 16:111k  (pinned 16, ladder_exhausted)
- **ursula-disk 10000**: 8:12k → 16:12k  (pinned 8, plateau)
- **ursula-memory 10000**: 8:127k → 16:146k → 24:156k  (pinned 16, plateau)

## Findings

_TODO: written by hand on top of the generated data._

## Caveats

_Single-node best-case; not 3-node Raft. Throughput is a saturation ceiling per the ladder._
