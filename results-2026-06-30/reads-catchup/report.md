# reads-catchup — read-scalability report

Each cell: aggregate read throughput (MiB/s) @ p99 latency (ms). ‡ = backpressure (503/429) observed at this load.

## wal — throughput @ p99 over stream_count × connections

| streams | 8 | 32 | 128 | 512 |
|---|---|---|---|---|
| 10 | 2225MiB/s@76ms | 2384MiB/s@249ms | ERR(0) | ERR(0) |
| 100 | 2171MiB/s@82ms | 2382MiB/s@252ms | ERR(0) | ERR(0) |

Peak read throughput per cardinality:
- streams=10: 2384 MiB/s at 32 connections
- streams=100: 2382 MiB/s at 32 connections

## ursula — throughput @ p99 over stream_count × connections

| streams | 8 | 32 | 128 | 512 |
|---|---|---|---|---|
| 10 | 2354MiB/s@67ms | 2378MiB/s@547ms | ERR(0) | ERR(0) |
| 100 | ERR(0) | ERR(0) | ERR(0) | ERR(0) |

Peak read throughput per cardinality:
- streams=10: 2378 MiB/s at 32 connections
- streams=100: 0 MiB/s at 8 connections

## Findings

_TODO: written by hand on top of the generated data._
