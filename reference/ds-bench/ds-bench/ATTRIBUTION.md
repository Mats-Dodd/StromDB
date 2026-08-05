# Attribution

`ds-bench` is a verbatim fork of `ursula-bench`, part of tonbo-io/ursula
(https://github.com/tonbo-io/ursula), commit
0b2d0dabf0a6544b909823e0d1d1149b98274e25, licensed under Apache-2.0. The three
workloads (multi-stream, fan-out, bootstrap), their HDR-histogram latency
methodology, and the multi-backend API-style abstraction originate there.

Local changes are packaging only: standalone crate layout, pinned dependency
versions, removal of the unused `rand`/`tokio-stream` deps, and replacing the single
`ursula-observability::init` call with `tracing-subscriber`. No workload logic or
measurement methodology has been modified. (Track 1 runs only multi-stream and
fan-out; the bootstrap module is compiled but not run, because its /bootstrap and
/snapshot endpoints are not part of the Durable Streams protocol.)
