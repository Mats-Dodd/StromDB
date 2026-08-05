# run-durable — write-throughput report

## Throughput at saturation (ops/s)

| streams | wal | wal-tailcache | memory |
|---|---|---|---|
| 100 | 502k | 505k | 446k |
| 1000 | 678k | 621k | 500k |
| 10000 | 733k | 685k | 583k |
| 100000 | 928k | 822k | 1282k† |

† = not saturated (ladder exhausted) — treat as a lower bound.

## Pod memory at saturation — peak / p50 (MiB)

| streams | wal | wal-tailcache | memory |
|---|---|---|---|
| 100 | 69 / 12 | 93 / 44 | 123 / 11 |
| 1000 | 43 / 35 | 61 / 51 | 48 / 40 |
| 10000 | 193 / 103 | 194 / 136 | 147 / 92 |
| 100000 | 854 / 545 | 909 / 510 | 726 / 481 |

_Pod working set = cgroup `memory.current − inactive_file` (anon + active page cache), sampled each second at the pinned rung. **peak** = high-water (catches bursts like an in-RAM Raft log filling); **p50** = median (what the server steadily holds resident). peak ≈ p50 ⇒ steadily resident; peak ≫ p50 ⇒ transient spikes._

## Saturation walks (pods → ops/s)

- **memory 100**: 16:446k → 24:455k  (pinned 16, plateau)
- **wal 100**: 16:463k → 24:502k → 32:531k  (pinned 24, plateau)
- **wal-tailcache 100**: 16:465k → 24:505k → 32:534k  (pinned 24, plateau)
- **memory 1000**: 16:500k → 24:479k  (pinned 16, plateau)
- **wal 1000**: 16:626k → 24:678k → 32:713k  (pinned 24, plateau)
- **wal-tailcache 1000**: 16:621k → 24:667k  (pinned 16, plateau)
- **memory 10000**: 32:583k → 48:620k  (pinned 32, plateau)
- **wal 10000**: 32:674k → 48:733k → 64:774k  (pinned 48, plateau)
- **wal-tailcache 10000**: 32:685k → 48:726k  (pinned 32, plateau)
- **memory 100000**: 80:434k → 100:587k → 110:1282k  (pinned 110, ladder_exhausted)
- **wal 100000**: 80:928k → 100:733k  (pinned 80, plateau)
- **wal-tailcache 100000**: 80:822k → 100:864k  (pinned 80, plateau)

## Findings

_TODO: written by hand on top of the generated data._

## Caveats

_Single-node best-case; not 3-node Raft. Throughput is a saturation ceiling per the ladder._
