# reads-longpoll — read-scalability report

Each cell: aggregate read throughput (MiB/s) @ p99 latency (ms). ‡ = backpressure (503/429) observed at this load.

## wal — throughput @ p99 over stream_count × connections

| streams | 32 | 128 | 512 | 1024 | 2048 |
|---|---|---|---|---|---|
| 100 | 6MiB/s@5ms | 25MiB/s@6ms | ERR(0) | 200MiB/s@6ms | 400MiB/s@7ms |
| 1000 | 4MiB/s@50ms | 16MiB/s@50ms | 65MiB/s@49ms | 129MiB/s@52ms | 244MiB/s@57ms |

Peak read throughput per cardinality:
- streams=100: 400 MiB/s at 2048 connections
- streams=1000: 244 MiB/s at 2048 connections

## ursula — throughput @ p99 over stream_count × connections

| streams | 32 | 128 | 512 | 1024 | 2048 |
|---|---|---|---|---|---|
| 100 | 3MiB/s@178ms | 11MiB/s@181ms | 40MiB/s@187ms | 78MiB/s@185ms | 161MiB/s@180ms |
| 1000 | 0MiB/s@1611ms | 2MiB/s@1386ms | 7MiB/s@1308ms | 14MiB/s@1313ms | 29MiB/s@1309ms |

Peak read throughput per cardinality:
- streams=100: 161 MiB/s at 2048 connections
- streams=1000: 29 MiB/s at 2048 connections

## Findings

_TODO: written by hand on top of the generated data._
