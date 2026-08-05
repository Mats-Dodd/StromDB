# canonical-mixed-writes — mixed read/write interference report

Sweep axis: **readers**. Latency cells are p50/p99 ms. ‡ = backpressure (503/429) observed in that class.

## wal — 10000 streams

| readers | write ops/s | write ms | read ops/s | read MiB/s | read ms | deliv rec/s | deliv ms | status |
|---|---|---|---|---|---|---|---|---|
| 0 | 49960 | 3.8/1220.6 | 0 | 0.0 | — | 0 | — | ok |
| 1000 | 49909 | 3.9/1323.0 | 50 | 3.0 | 1.5/377.3 | 0 | — | ok |
| 10000 | 50048 | 3.7/1385.5 | 499 | 30.4 | 1.4/340.5 | 0 | — | ok |
| 100000 | 50034 | 4.5/2961.4 | 4988 | 303.7 | 1.4/1072.1 | 0 | — | ok |

## Findings

_TODO: written by hand on top of the generated data._
