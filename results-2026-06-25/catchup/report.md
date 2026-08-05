# Catch-up / reconnect — durable vs ursula vs s2

_Reproduces Ursula's methodology (ursula.tonbo.io/benchmark): each client reconnects to its OWN pre-populated stream and catches up via that system's native path — **ursula** `GET /bootstrap` (snapshot+tail), **durable** `offset=-1` and **s2** `/records` (full log). Equal hardware (1 server node each — Ursula's published run gave ursula 3 nodes vs 1 for DS/S2)._

## Per-client catch-up p99 latency (ms) — lower is faster

| pre_events (stream) | durable | wal | node | s2 | ursula |
|---|---|---|---|---|---|
| 200 | — | — | 185.983 | — | — |
| 1000 | — | — | — | — | — |
| 2000 | — | — | 2121.73 | — | — |

## p50 latency (ms)

| pre_events | durable | wal | node | s2 | ursula |
|---|---|---|---|---|---|
| 200 | — | — | 92.799 | — | — |
| 1000 | — | — | — | — | — |
| 2000 | — | — | 1240.06 | — | — |

## Aggregate replay throughput (MiB/s) — total bytes served / stampede time

| pre_events | durable | wal | node | s2 | ursula |
|---|---|---|---|---|---|
| 200 | — | — | 700.004 | — | — |
| 1000 | — | — | — | — | — |
| 2000 | — | — | 906.128 | — | — |

## Response body per client (KiB) — smaller = less to transfer

| pre_events | durable | wal | node | s2 | ursula |
|---|---|---|---|---|---|
| 200 | — | — | 200 | — | — |
| 1000 | — | — | — | — | — |
| 2000 | — | — | 2000 | — | — |

## Errors / caveats

- None.

_clients=1000 (each own stream), event_bytes=1024, snapshot_bytes=51200 (ursula only)._