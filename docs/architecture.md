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

## 4. protocol state and physical projections

The public protocol exposes one stream abstraction. The engine represents its
state through three ordered projections because their workloads differ. They
are not three transactions or three authorities. One `OperationFact` can
change all three atomically at one WAL coordinate.


### 4.1 ledger: ordered identity and lifecycle


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



### 4.2 tally: current state and admission

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

### 4.3 annals: ordered retained history

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

## 6. durable namespace and identities

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

## 7 permanent seals

The Seal is the only mutable-in-time authority, represented as an immutable
permanent sequence. It is also the complete manifest for the current physical
version:

```rust
struct Seal {
    partition: PartitionId,
    generation: SealGeneration,
    through: WalCut,
    wal_owner_at_through: ReplayOwner,
    format: SealFormat,
    ledger: TreeVersion,
    tally: TreeVersion,
    annals: TreeVersion,
}

enum ReplayOwner {
    NoOwner,
    Owner(OwnerToken),
}
```

Every persisted field has one recovery obligation:

| Field | Why it cannot be derived later |
| --- | --- |
| `partition`, `generation` | authenticate key/body identity and exact succession |
| `through` | separate the materialized prefix from the readable WAL suffix and authorize covered-RUN collection |
| `wal_owner_at_through` | resume strict owner folding after covered RUNs have been deleted |
| `format` | choose the durable decoder and comparator semantics |
| three `TreeVersion`s | name the complete current physical serving state |

`WalCut` includes the virtual genesis cut zero. Real WAL objects use nonzero
`BatchId` values. `ReplayOwner::NoOwner` is legal only at cut zero; every real
cut records the owner in force after folding that coordinate.

Genesis is generation 1 at cut zero with `NoOwner` and one canonical empty
`TreeVersion` for each store. Every later Seal is the exact successor of one
observed permanent Seal. Seal keys are never overwritten, deleted, or reused:

```text
1, 2, 3, ...
```

Every Seal carries a complete manifest and can serve while it is newest.  Serving and recovery never need to load an earlier Seal. The Seal generation
is therefore the exact physical view identity; `through` remains only the
logical WAL coverage boundary.

“Self-contained” means that the Seal names the complete dependency graph
without consulting a predecessor manifest. It does not mean SST footers,
blocks, or payload bytes are embedded in the Seal.

## 8. Legal Seal transitions

Every candidate targets exactly `head.generation + 1`. Skipping a generation
is illegal even if the skipped key appears absent.

### 8.1 Genesis

Provisioning creates one canonical generation-1 Seal with empty trees, cut
zero, and `NoOwner`. Genesis grants no ownership.


### 8.2 Claim

A claim changes neither logical nor physical state. It copies the complete
manifest:

```text
candidate.generation = head.generation + 1
candidate.through = head.through
candidate.wal_owner_at_through = head.wal_owner_at_through
candidate.{ledger,tally,annals} = head.{ledger,tally,annals}
```

Only the caller receiving `Direct` may construct `AuthoredClaim`. A
`DurableMatch` proves that the Seal exists but never proves which claimant
created it. A claim authorizes takeover; it does not authorize serving.



### 8.3 Advance

A Ready writer may publish a Seal at a greater WAL cut:

```text
candidate.generation           = source.generation + 1
candidate.through              > source.through
candidate.wal_owner_at_through = owner produced by exact fold through candidate.through
logical(candidate)             = fold(logical(source), selected WAL prefix)
candidate children             = source-carried or candidate-generation-fresh
```

When the source is genesis, its logical source is the empty state at virtual
cut zero with `NoOwner`; the first advancing Seal ends at a nonzero occupied
WAL coordinate and records the owner in force there.

The stored owner is historical replay state. It need not equal the publisher's
current owner token when a bounded checkpoint stops inside a suffix written by
an earlier owner.

Every fresh table and retained payload pack is durable before the Seal create
is attempted. Only a `Direct` Seal result grants a continuing
`PublishedCheckpoint` capability.


### 8.4 Maintain

A Ready writer may select a new physical version at the same cut:

```text
candidate.generation           = source.generation + 1
candidate.through              = source.through
candidate.wal_owner_at_through = source.wal_owner_at_through
logical(candidate)             = logical(source)
candidate children             = source-carried or candidate-generation-fresh
```

There is no useful same-cut physical rewrite while all three genesis trees are
empty.

Maintenance may compact SSTs, rewrite range boundaries, or move payload bytes.
It must preserve every logical key, extent identity, payload byte, capacity
value, and ordering rule. A result is tied to its exact source Seal identity
and is never rebased onto a newer Seal.


Claims, advances, and maintenance all contend for the same next permanent
Seal coordinate.


### 8.5 Publication outcomes

One prepared Seal candidate is sent once. Transition handling is:

| Evidence | Claim | Advance or maintain |
| --- | --- | --- |
| `Direct` | provisional `AuthoredClaim` | `PublishedCheckpoint` |
| `DurableMatch` | no ownership; rediscover or stop | no continuing capability; stop and bootstrap |
| `NotOurs` | rediscover the permanent head | fenced or contested; stop |
| `Unresolved` | never serve; stop | poison; bootstrap resolves history |

There is no post-create newest-generation query after `Direct`. Permanent
contiguous Seal keys plus exact succession prove that the directly created
candidate was the maximum at its create linearization point: no legal later
Seal can exist while its predecessor coordinate is absent. A successor may be
created before the response returns; that is ordinary asynchronous fencing.

A directly published Advance or Maintain updates the writer's locally owned
head generation while retaining the owner token from its authored claim. That
capability records valid lineage, not a clock lease or promise that the route
can never be superseded.
