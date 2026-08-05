# canonical-reads-catchup — read-scalability report

Each cell: aggregate read throughput (MiB/s) @ p99 latency (ms). ‡ = backpressure (503/429) observed at this load.

## wal — throughput @ p99 over stream_count × connections

| streams | 8 | 32 | 128 | 512 |
|---|---|---|---|---|
| 10 | 1408MiB/s@97ms | 2390MiB/s@234ms | 2372MiB/s@1314ms | 2770MiB/s@4796ms |
| 100 | 1430MiB/s@97ms | 2388MiB/s@235ms | 2355MiB/s@1432ms | 2738MiB/s@4850ms |

Peak read throughput per cardinality:
- streams=10: 2770 MiB/s at 512 connections
- streams=100: 2738 MiB/s at 512 connections

## ursula — throughput @ p99 over stream_count × connections

| streams | 8 | 32 | 128 | 512 |
|---|---|---|---|---|
| 10 | 2346MiB/s@89ms | 2373MiB/s@476ms | 2489MiB/s@2228ms | 2910MiB/s@10551ms |
| 100 | ERR(0) | ERR(0) | ERR(0) | ERR(0) |

Peak read throughput per cardinality:
- streams=10: 2910 MiB/s at 512 connections
- streams=100: 0 MiB/s at 8 connections

## Findings

_TODO: written by hand on top of the generated data._
