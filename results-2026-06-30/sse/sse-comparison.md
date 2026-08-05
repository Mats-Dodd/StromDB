# SSE Fan-out — delivery latency

1 stream, 1 writer @ 50 ev/s, swept total subscribers; one well-provisioned
client pod (single wall clock). Writer-paced → metric is delivery latency.

## Median (p50, ms)

| config \ subscribers | 1 | 10 | 100 | 1000 |
|---|---|---|---|---|
| wal (cache off) | 0.998 | 1.104 | 1.455 | 4.139 |
| wal (cache on) | 1.001 | 1.088 | 1.436 | 3.723 |
| ursula in-memory | 0.988 | 1.102 | 1.422 | 3.275 |
| ursula disk | 1.893 | 2.355 | 2.601 | 4.227 |

## Full spread (p50 / p99 / max, ms)

| config | subs | p50 | p90 | p99 | p999 | max |
|---|---|---|---|---|---|---|
| wal (cache off) | 1 | 0.998 | 1.085 | 1.172 | 1.261 | 1.261 |
| wal (cache off) | 10 | 1.104 | 1.207 | 1.32 | 1.405 | 1.529 |
| wal (cache off) | 100 | 1.455 | 1.635 | 1.826 | 2.211 | 2.603 |
| wal (cache off) | 1000 | 4.139 | 4.951 | 5.479 | 6.051 | 7.679 |
| wal (cache on) | 1 | 1.001 | 1.1 | 1.198 | 1.256 | 1.256 |
| wal (cache on) | 10 | 1.088 | 1.186 | 1.313 | 1.415 | 1.475 |
| wal (cache on) | 100 | 1.436 | 1.629 | 1.794 | 1.971 | 2.273 |
| wal (cache on) | 1000 | 3.723 | 4.655 | 5.207 | 5.623 | 6.123 |
| ursula in-memory | 1 | 0.988 | 1.082 | 1.189 | 1.295 | 1.295 |
| ursula in-memory | 10 | 1.102 | 1.213 | 1.337 | 45.503 | 45.567 |
| ursula in-memory | 100 | 1.422 | 1.601 | 1.754 | 1.873 | 2.151 |
| ursula in-memory | 1000 | 3.275 | 4.087 | 4.759 | 5.203 | 6.011 |
| ursula disk | 1 | 1.893 | 1.984 | 2.081 | 2.139 | 2.139 |
| ursula disk | 10 | 2.355 | 2.481 | 2.879 | 42.847 | 42.911 |
| ursula disk | 100 | 2.601 | 2.795 | 3.113 | 3.821 | 4.015 |
| ursula disk | 1000 | 4.227 | 4.955 | 5.575 | 6.087 | 7.651 |

## Pod memory vs subscribers — peak / p50 (MiB)

| config \ subscribers | 1 | 10 | 100 | 1000 |
|---|---|---|---|---|
| wal (cache off) | 6 / 5 | 7 / 6 | 11 / 10 | 27 / 23 |
| wal (cache on) | 6 / 5 | 6 / 6 | 8 / 7 | 26 / 21 |
| ursula in-memory | 15 / 15 | 15 / 15 | 15 / 15 | 16 / 15 |
| ursula disk | 15 / 15 | 15 / 15 | 15 / 15 | 16 / 15 |

_Pod working set (cgroup `memory.current − inactive_file`) during each subscriber-count cell. **Flat across the row ⇒ a shared fan-out buffer** (one resident tail served to all subscribers); growth ⇒ per-subscriber buffering._


_p50 = median; lower is better. — = not measured._
