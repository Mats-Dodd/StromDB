# run-node — write-throughput report

## Throughput at saturation (ops/s)

| streams | node |
|---|---|
| 100 | 66k† |
| 1000 | 95k |
| 10000 | 101k |

† = not saturated (ladder exhausted) — treat as a lower bound.

## Pod memory at saturation — peak / p50 (MiB)

| streams | node |
|---|---|
| 100 | 180 / 114 |
| 1000 | 436 / 229 |
| 10000 | 969 / 787 |

_Pod working set = cgroup `memory.current − inactive_file` (anon + active page cache), sampled each second at the pinned rung. **peak** = high-water (catches bursts like an in-RAM Raft log filling); **p50** = median (what the server steadily holds resident). peak ≈ p50 ⇒ steadily resident; peak ≫ p50 ⇒ transient spikes._

## Saturation walks (pods → ops/s)

- **node 100**: 4:55k → 8:60k → 16:66k  (pinned 16, ladder_exhausted)
- **node 1000**: 4:61k → 8:95k → 16:88k  (pinned 8, plateau)
- **node 10000**: 8:101k → 16:80k  (pinned 8, plateau)

## Findings

_TODO: written by hand on top of the generated data._

## Caveats

_Single-node best-case; not 3-node Raft. Throughput is a saturation ceiling per the ladder._
