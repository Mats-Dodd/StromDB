# ds-bench campaign report — TEMPLATE

Copy into `results/REPORT.md` for a campaign. Structure follows the workloads we
report (canonical suites cover exactly these); the caveat checklist at the bottom
exists because past reports shipped inflated or mis-attributed numbers — run it
before publishing.

> Header: date, server build (branch @ commit / PR), hardware (server machine +
> CPU pin + storage layout, client fleet), zone. Per-suite provenance in
> `PROVENANCE.md`; per-suite grids in each subdirectory.

## 1. Write saturation (`canonical-write`, `canonical-write-ursula`)

Peak append/s at saturation (256 B payloads, saturation pod-ladder per cardinality):

| streams | wal-ideal | memory | ursula-mem | ursula-disk |
|---|---|---|---|---|
| 10k  | | | | |
| 100k | | | — | — |

- State which cells are true plateaus vs ladder-bounded lower bounds (`reason`
  field: `plateau` vs `ladder_exhausted`) — never present a ladder ceiling as a
  server ceiling.
- Compare against the reference numbers in the suite `_doc`; a breached
  regression gate is a finding, not a footnote.

## 2. SSE fan-out (`scripts/run-sse.sh` — 1 stream, subscriber ladder)

Delivery p50/p99 (ms) vs subscriber count {1, 10, 100, 1000}, per config.

## 3. Read scalability (`canonical-reads-catchup`, `canonical-reads-sse`)

- SSE tail: ops/s @ p99 ms per connection level.
- Catch-up: MiB/s @ p99 ms per connection level. Mark client-pod OOM cells as
  client-bound gaps, not server data.

## 4. Mixed read/write interference (`canonical-mixed-{cal,writes,delivery}`)

- Anchor: single-pod mixed-shape ceiling from `canonical-mixed-cal`; sweeps pin
  writers at ~60% of it.
- Paced readers vs pinned writes: write ops/s must hold flat across reader
  levels; report replays/s + read MiB/s + read p50/p99.
- Delivery under write load: del/s must track writes/s; call out any collapse
  (the 2026-07-02 memory-mode delivery collapse pattern).

## Known gaps & artifacts

List every dropped cell, re-run, resume, and client-bound ceiling explicitly.

---

## Pre-publication caveat checklist (numbers-inflation guards)

Every one of these has produced a wrong or inflated headline in a past campaign:

1. **Storage layout verified?** Stream data files MUST be on local NVMe lanes,
   never the boot PD, and WAL lanes ≠ data lanes (AGENTS.md §0 invariants). A
   mis-mounted layout mis-states wal throughput by 5–26× in either direction.
2. **Windows aligned?** Only barrier-aligned fleet windows (`windows_aligned`)
   count; an unaligned cell reports partial-fleet throughput.
3. **Plateau vs ladder?** `ladder_exhausted` cells are lower bounds — extend the
   ladder or label them; comparing a plateau to a ladder ceiling fabricates a win.
4. **Same image + digest for every arm being compared?** Record digests in
   PROVENANCE.md; the resume digest is tag-based and does NOT prove content.
5. **Sanity-check against physics.** Appends/s × payload vs device writeback;
   fsync/s vs device barrier rate; a "durable" number beating the no-fsync
   ceiling (e.g. wal > memory) means the measurement or the durability is broken
   — treat as a bug until explained (2026-07-02's 2.05M @500k on 4 CPUs was this).
6. **Cardinality shape.** Report 10k AND 100k; a >30% drop is the cliff
   regression gate firing, not a shrug.
7. **Client-bound cells** (fleet OOM, creation choke) are gaps — never zeros,
   never averaged in.
