# canonical-write-ursula — write-throughput report

## Throughput at saturation (ops/s)

| streams | ursula-memory | ursula-disk |
|---|---|---|
| 100 | 48k | 4k |
| 1000 | 55k | 7k |
| 10000 | 49k | 8k |

† = not saturated (ladder exhausted) — treat as a lower bound.

## Pod memory at saturation — peak / p50 (MiB)

| streams | ursula-memory | ursula-disk |
|---|---|---|
| 100 | 3260 / 2099 | 1306 / 1147 |
| 1000 | 2302 / 1737 | 1753 / 1507 |
| 10000 | 3534 / 3056 | 2689 / 2330 |

_Pod working set = cgroup `memory.current − inactive_file` (anon + active page cache), sampled each second at the pinned rung. **peak** = high-water (catches bursts like an in-RAM Raft log filling); **p50** = median (what the server steadily holds resident). peak ≈ p50 ⇒ steadily resident; peak ≫ p50 ⇒ transient spikes._

## Latency (ms, p50 / p99)

| streams | ursula-memory @≤80% load | ursula-memory @saturation | ursula-disk @≤80% load | ursula-disk @saturation |
|---|---|---|---|---|
| 100 | 1.1 / 34.1 (49k @4p) | 1.2 / 34.0 | 6.7 / 84.5 (4k @4p) | 6.7 / 84.8 |
| 1000 | 10.8 / 62.9 (55k @4p) | 10.9 / 63.4 | 111.3 / 297.7 (7k @4p) | 111.7 / 298.8 |
| 10000 | 202.9 / 287.0 (50k @8p) | 204.7 / 291.6 | 1401.9 / 1899.5 (8k @8p) | 1403.9 / 1904.6 |

_@≤80% load = the largest ladder rung at ≤80% of peak throughput — the server's service latency with headroom. @saturation = the pinned plateau rung, where a closed-loop fleet measures its own queueing (≈ in-flight ÷ ceiling by Little's law), NOT the server's per-request cost. Compare against the unloaded single-request baseline (~1 ms for wal) before reading anything into large saturation values._

## Saturation walks (pods → ops/s, p50 ms)

- **ursula-disk 100**: 4:4k@6.7ms → 8:5k@6.4ms  (pinned 4, plateau)
- **ursula-memory 100**: 4:49k@1.1ms → 8:52k@1.2ms  (pinned 4, plateau)
- **ursula-disk 1000**: 4:7k@111.3ms → 8:7k@111.4ms  (pinned 4, plateau)
- **ursula-memory 1000**: 4:55k@10.8ms → 8:55k@11.0ms  (pinned 4, plateau)
- **ursula-disk 10000**: 8:8k@1401.9ms → 16:8k@1394.7ms  (pinned 8, plateau)
- **ursula-memory 10000**: 8:50k@202.9ms → 16:50k@201.5ms  (pinned 8, plateau)

## Findings

_TODO: written by hand on top of the generated data._

## Caveats

_Single-node best-case; not 3-node Raft. Throughput is a saturation ceiling per the ladder._
