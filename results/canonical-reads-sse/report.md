# canonical-reads-sse — read-scalability report

Each cell: aggregate read throughput (MiB/s) @ p99 latency (ms). ‡ = backpressure (503/429) observed at this load.

## wal — throughput @ p99 over stream_count × connections

| streams | 64 | 256 | 1024 | 2048 |
|---|---|---|---|---|
| 10 | 1MiB/s@3ms | 3MiB/s@5ms | 13MiB/s@2ms | 25MiB/s@5ms |
| 100 | 1MiB/s@11ms | 3MiB/s@18ms | 12MiB/s@32ms | 24MiB/s@42ms |

Peak read throughput per cardinality:
- streams=10: 25 MiB/s at 2048 connections
- streams=100: 24 MiB/s at 2048 connections

## ursula — throughput @ p99 over stream_count × connections

| streams | 64 | 256 | 1024 | 2048 |
|---|---|---|---|---|
| 10 | 1MiB/s@2ms | 3MiB/s@2ms | 13MiB/s@4ms | 25MiB/s@3ms |
| 100 | 1MiB/s@40ms | 3MiB/s@44ms | 11MiB/s@54ms | 20MiB/s@62ms |

Peak read throughput per cardinality:
- streams=10: 25 MiB/s at 2048 connections
- streams=100: 20 MiB/s at 2048 connections

## Findings

_TODO: written by hand on top of the generated data._
