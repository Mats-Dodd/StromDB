# canonical-mixed-delivery — mixed read/write interference report

Sweep axis: **writer_rate**. Latency cells are p50/p99 ms. ‡ = backpressure (503/429) observed in that class.

## wal — 2000 streams

| rate/writer | write ops/s | write ms | read ops/s | read MiB/s | read ms | deliv rec/s | deliv ms | status |
|---|---|---|---|---|---|---|---|---|
| 2 | 4067 | 1.4/130.2 | 0 | 0.0 | — | 4000 | 0.4/132.0 | ok |
| 8 | 16056 | 1.5/62.4 | 0 | 0.0 | — | 15955 | 0.4/62.5 | ok |
| 20 | 39969 | 2.5/189.7 | 0 | 0.0 | — | 33233 | 1.6/122.9 | ok |
| 33 | 65899 | 11.3/346.6 | 0 | 0.0 | — | 65634 | 10.9/111.3 | ok |
| max | 74744 | 23.2/116.9 | 0 | 0.0 | — | 74400 | 36.9/134.1 | ok |

## memory — 2000 streams

| rate/writer | write ops/s | write ms | read ops/s | read MiB/s | read ms | deliv rec/s | deliv ms | status |
|---|---|---|---|---|---|---|---|---|
| 2 | 4067 | 1.4/117.1 | 0 | 0.0 | — | 3335 | 0.3/132.1 | ok |
| 8 | 16050 | 1.4/7.3 | 0 | 0.0 | — | 15946 | 0.3/4.2 | ok |
| 20 | 39982 | 1.7/59.3 | 0 | 0.0 | — | 39825 | 0.5/32.1 | ok |
| 33 | 65908 | 1.8/85.8 | 0 | 0.0 | — | 65680 | 0.7/21.0 | ok |
| max | 126714 | 12.8/48.7 | 0 | 0.0 | — | 126288 | 12.0/61.8 | ok |

## Findings

_TODO: written by hand on top of the generated data._
