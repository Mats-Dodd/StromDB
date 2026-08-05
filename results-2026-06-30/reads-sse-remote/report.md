# reads-sse-remote — read-scalability report

Each cell: aggregate read throughput (MiB/s) @ p99 latency (ms). ‡ = backpressure (503/429) observed at this load.

## wal — throughput @ p99 over stream_count × connections

| streams | 64 | 256 | 1024 | 2048 |
|---|---|---|---|---|
| 10 | 1MiB/s@1ms | 3MiB/s@1ms | 13MiB/s@2ms | 25MiB/s@3ms |
| 100 | 1MiB/s@0ms | 3MiB/s@1ms | 13MiB/s@1ms | 25MiB/s@1ms |

Peak read throughput per cardinality:
- streams=10: 25 MiB/s at 2048 connections
- streams=100: 25 MiB/s at 2048 connections

## ursula — throughput @ p99 over stream_count × connections

| streams | 64 | 256 | 1024 | 2048 |
|---|---|---|---|---|
| 10 | 1MiB/s@1ms | 3MiB/s@2ms | 13MiB/s@3ms | 25MiB/s@4ms |
| 100 | 1MiB/s@41ms | 3MiB/s@44ms | 11MiB/s@56ms | 20MiB/s@61ms |

Peak read throughput per cardinality:
- streams=10: 25 MiB/s at 2048 connections
- streams=100: 20 MiB/s at 2048 connections

## Findings

_TODO: written by hand on top of the generated data._
