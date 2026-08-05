# durable streams server architecture

This document specifies StromDb's storage engine.  Its correctness protocol, storage layout, program shape, writes, reads, maintenance and garbage collection. It remains authority unless superseded by explicit RFC's.  

[`durable-streams-protocol.md`](durable-streams-protocol.md) remains the external Durable Streams contract.

The design rule is:

> Keep durable authority minimal, derive everything else, and admit no work
> that the bounded physical engine cannot eventually materialize.


## overview 

A stromdb instance operates over a logical partition.  

One partition is a serialized fact stream backed only by immutable S3 objects.
Its complete current state is:

```text
newest permanent Seal at WAL cut W
        |-- Ledger: inline adaptive ordered-range LSM manifest
        |-- Tally:  inline adaptive ordered-range LSM manifest
        `-- Annals: inline adaptive ordered-range LSM manifest
                         |
                         `-- immutable bounded payload packs

current logical state
    = newest Seal's complete inline manifest
    + exact ordered WAL suffix after W
```


The hot mutation path is:

```text
parse -> admit -> one create-only WAL PUT -> strict fold -> publish -> reply
```


The durable concepts each job:

| Concept | Job |
| --- | --- |
| Seal | choose the unique current version, serialize writer succession, and atomically publish the complete three-tree manifest |
| WAL RUN/FENCE | make mutations durable with one PUT and make takeover exact |
| SST | incrementally organize ordered index state |
| Payload pack | retain large values without rewriting them during index compaction |

## design requirement

The engine must uphold the following:

1. A successfull write must survive process loss, object storage is the systems only source of durability. RAM and NVME are only ever treated as accelerators. 
2. A successful write only requires one obejct storage round trip.
3. A most one writer process is the current authority for a partition and this correctness does not depend on clocks or leases.
4. Recovery reconstructs every agkownledged write exactly and in order.
5. Materialization is incrimental.
7. Every compaction, checkpoint, read, replay step, buffer, queue, object,
   manifest, and collection batch has a named bound or is finite and
   resumable in bounded turns.
8. GC may leak. 
9.  One partition should remain viable at a scale of 10 million streams.  

The engine does not provide transactions between independent public requests
or streams. One admitted request is atomic; ordering between requests is the
partition writer's admission order.

A partition is the serialization and recovery boundary. A stream and its fork
family must route together so lineage, pins, retention, and inherited reads can
remain one bounded fact protocol. V1 may deploy one partition; later partition
placement and movement belong to the router/control plane and must not create
a second storage authority. 


## system anatomy

The system is built around 4 primitives who compose into our corrctness protocol and performance characteristics. 

```text
Seal       commits
WAL        remembers
LSM forest organizes
packs      carry bytes
```

The seal is our clean serialization point for the many trees in our lsm forest. It is our immutable serving manifest. 

The wal is our linearization point, it carries the data for all stores into one point of linearizibility for a write.  

The LSM forest is a set of individual LSM stores tuned to the different requirements of workload prescribed to servers in the durable streams protocol.  

payloads are payloads, our machinery revolves around serving these correctly and efficiently.

Every materialized state change follows one S3 publication shape:

```text
zero or more fresh immutable children
    -> one complete exact-successor immutable Seal
```

S3 has no multi-object transaction or atomic rename. The conditional Seal
create is both manifest publication and transaction commit: children written
before it are either selected together or remain unreachable garbage. There
is no intermediate root object.

## 4. Protocol state and physical projections

The public protocol exposes one stream abstraction. The engine represents its
state through three ordered projections because their workloads differ. They
are not three transactions or three authorities. One `OperationFact` can
change all three atomically at one WAL coordinate.


### 4.1 Ledger: ordered identity and lifecycle


Ledger is keyed primarily by canonical stream path and stores cold identity:

```text
stream path
    -> stream identity
       content type and configuration
       creation and permanent path tombstone
       open/closed/soft-deleted lifecycle
       TTL or absolute-expiry policy
       parent stream and exact fork boundary
```

Its dominant operations are exact lookup, ordered prefix scan, glob build,
fork-lineage traversal, and rare lifecycle mutation. Lexical range order is
part of the workload rather than an incidental implementation property.

Path tombstones are permanent logical values because the protocol discourages
reusing a deleted stream URL. Compaction does not silently erase that fact.


### 4.2 Tally: current state and admission

### 4.2 Tally: current state and admission

NB the exact semantics/ layout of the tally LSM are still subejct to change by a further RFC.  They still adhere to the currect correctness protocol though. 

Tally is a set of ordered last-value namespaces:

```text
stream/<stream>
producer/<stream>/<producer>
subscription/<subscription>
link-by-sub/<subscription>/<stream>
link-by-stream/<stream>/<subscription>
deadline/<bucket>/<deadline>/<identity>
```

It contains stream tails, closure and retention versions, producer epochs and
sequences, per-writer `Stream-Seq` state, cumulative positions, fork pins,
logical capacity, subscription cursors, wake generations, leases, retry
schedules, and bidirectional stream membership.

Tally is the authoritative logical capacity projection. Admission-critical
Tally and Ledger state is rebuilt locally before a partition becomes Ready.

For fork accounting, a stream row distinguishes:

```text
visible_retained_*          bytes/extents readable through this stream,
                            including an inherited prefix

owned_unique_referenced_*  canonical Annals bytes/extents owned by this
                            stream and retained by it or descendants
```

Forking increases logical visibility but creates no duplicate Annals row and
therefore adds no owned-unique bytes.

### 4.3 Annals: ordered retained history

NB the exact semantics/ layout of the tally LSM are still subejct to change by a further RFC.  They still adhere to the currect correctness protocol though. 

Annals is keyed by canonical owner and stream offset:

```text
(owning stream, start offset)
    -> extent identity
       end offset
       cumulative byte/message positions
       bounded message-boundary metadata
       payload locator and checksum
```

Its dominant operations are predecessor seek, stream-range scan, append,
point trim, and payload-locator replacement. Annals contains index metadata,
not payload bodies.

A fork stores its lineage and boundary in Ledger and its pins/counters in
Tally. It does not duplicate inherited Annals entries. Reads traverse a
bounded lineage and then read canonical owner extents.


## storage capability contract

strom db depends on a deliberatly narrow object store adapter.  It must provide:

1. linearizable create-if-absent with exactly one direct winner;
2. immutable object bytes while a key exists;
3. strong read-after-create GET and ordered LIST;
4. bounded lexicographic LIST pages with an exclusive continuation poin
5. independently checksummed bounded range GETs;
6. exact-validator conditional deletion for a currently observed WAL RUN;
7. idempotent deletion for other authorized content objects;
8. bounded decode before allocation, including key/body agreement; and

Explicit operation concerns ie IAM are explicitly out of scope.  

Seal deletion is denied outright.

 Lifecycle deletion is disabled for all
Courant namespaces. The WAL collector is the only role allowed to delete in
the shared RUN/FENCE namespace.

The adapter normalizes conditional-create results as evidence:

```rust
enum CreateEvidence {
    Direct,       // this request received the winning response
    DurableMatch, // the exact bytes exist; their author is unknown
    NotOurs,      // different bytes occupy the immutable coordinate
    Unresolved,   // the request may still take effect
}
```

`Direct` is stronger than byte equality. The adapter must not manufacture it
by transparently sending another create after an ambiguous request.

Authority-bearing Seal and WAL candidates are sent exactly once. Bounded exact
GETs may reconcile the same frozen bytes after an ambiguous response. Content
objects such as SSTs and packs may use create-or-verify because their presence
alone grants no authority.

## 6. Durable namespace and identities

Keys are canonical, versioned, and derived from domain identities. The shape
is conceptually:

```text
partition/<partition>/seal/v1/<reverse generation>
partition/<partition>/wal/v1/<reverse batch>
partition/<partition>/table/v1/<store>/<birth generation>/<attempt>/<ordinal>
partition/<partition>/payload-pack/v1/<birth generation>/<attempt>/<ordinal>
```

Reverse coordinates are fixed-width decimal encodings:

```text
storage_ordinal = u64::MAX - logical_ordinal
```

Ascending `ListObjectsV2(MaxKeys=1)` therefore returns the greatest Seal
generation or greatest surviving WAL coordinate. The exact wire spelling is a
versioned format decision and receives golden vectors.

Fresh immutable object identity is scoped to the Seal generation that could
first select it:

```rust
struct AttemptId {
    owner_claim: SealGeneration,
    local_counter: u64,
}

struct FreshIdentity {
    birth_generation: SealGeneration,
    attempt: AttemptId,
    ordinal: u32,
}

struct TableObjectId {
    fresh: FreshIdentity,
    store: StoreKind,
}

struct PayloadPackId(FreshIdentity);
```

The counter is checked and process-local. After restart, a process must author
a new claim before preparing objects, so a legal writer never reuses
`(owner_claim, local_counter)`. An attempt never adopts an arbitrary orphan
found by LIST or content hash.

Every object envelope contains its logical identity, format, bounded lengths,
and checksum. Decode verifies:

```text
length bound
-> magic, object kind, and format
-> checksum
-> bounded body with no trailing bytes
-> domain invariants
-> canonical key/body identity
```

Foreign or noncanonical objects under a Courant-owned prefix are durable
contradictions, not candidates to ignore.