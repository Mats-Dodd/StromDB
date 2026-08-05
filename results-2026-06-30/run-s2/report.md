# run-s2 — write-throughput report

## Throughput at saturation (ops/s)

| streams | s2 |
|---|---|
| 100 | 2k |
| 1000 | ERROR (creation_choke) |

† = not saturated (ladder exhausted) — treat as a lower bound.

## Pod memory at saturation — peak / p50 (MiB)

| streams | s2 |
|---|---|
| 100 | 65 / 45 |
| 1000 | — |

_Pod working set = cgroup `memory.current − inactive_file` (anon + active page cache), sampled each second at the pinned rung. **peak** = high-water (catches bursts like an in-RAM Raft log filling); **p50** = median (what the server steadily holds resident). peak ≈ p50 ⇒ steadily resident; peak ≫ p50 ⇒ transient spikes._

## Saturation walks (pods → ops/s)

- **s2 100**: 2:2k → 4:2k  (pinned 2, plateau)
- **s2 1000**: 2:19k → 4:0k  (pinned None, creation_choke)

## Findings

_TODO: written by hand on top of the generated data._

## Caveats

_Single-node best-case; not 3-node Raft. Throughput is a saturation ceiling per the ladder._
