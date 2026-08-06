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
        |-- Directory: inline adaptive ordered-range LSM manifest
        |-- Ledger:    inline adaptive ordered-range LSM manifest
        |-- Tally:     inline adaptive ordered-range LSM manifest
        `-- Annals:    inline adaptive ordered-range LSM manifest
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
| Seal | choose the unique current version, serialize writer succession, and atomically publish the complete four-tree manifest |
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

## protocol state and physical projections

The public protocol exposes one stream abstraction. The engine represents its
state through four ordered projections because their workloads differ. They
are not four transactions or four authorities. One `OperationFact` can
change multiple projections atomically at one WAL coordinate.


### directory: ordered paths


Directory is keyed by canonical stream path and resolves protocol names to
partition-local identities:

```text
stream path
    -> Live(stream uid) | Tombstone(stream uid)
```

Directory tombstones are permanent logical values because the protocol
discourages reusing a deleted stream URL. Compaction does not silently erase
that fact.


### ledger: ordered identity and lifecycle


Ledger is keyed by partition-local stream UID and stores stream metadata:

```text
stream uid
    -> content type and configuration
       creation coordinate
       open/closed/soft-deleted lifecycle
       TTL or absolute-expiry policy
       parent stream and exact fork boundary
```

Directory owns ordered prefix scans and glob construction. Ledger owns exact
UID lookup, fork-lineage traversal, and rare lifecycle mutation.



### tally: current state and admission

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

Tally is the authoritative logical capacity projection once its RFC defines
that state. The currently implemented Directory and Ledger state is rebuilt
locally before a partition becomes Ready.

For fork accounting, a stream row distinguishes:

```text
visible_retained_*          bytes/extents readable through this stream,
                            including an inherited prefix

owned_unique_referenced_*  canonical Annals bytes/extents owned by this
                            stream and retained by it or descendants
```

Forking increases logical visibility but creates no duplicate Annals row and
therefore adds no owned-unique bytes.

### annals: ordered retained history

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

## durable namespace and identities

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

Every typed durable object is one concrete rkyv archive selected by its typed
object-store key and decoder. The archive contains its logical identity.
Decode verifies:

```text
SealKey             -> Seal
WalKey              -> WalObject::Run | WalObject::Fence
TableKey(Directory) -> DirectorySstArchive
TableKey(Ledger)    -> LedgerSstArchive
```

`StreamRecord` appears only inside a Ledger SST value; it has no standalone
durable codec or object location. Decode verifies:

```text
length bound
-> checked archived access
-> collection-count and resident-memory bounds before allocation
-> domain invariants
-> key/body identity
```

There is no envelope, application checksum, or content-format version before
StromDB makes a released compatibility promise. Foreign, malformed, or
semantically invalid objects under a Courant-owned prefix are durable
contradictions, not candidates to ignore. rkyv locates its root at the end of
the supplied slice, so unreachable leading bytes are tolerated inside the
complete-object bound but are never emitted. The exact rkyv release and
format-control features are the current archive contract;
[`rkyv.md`](rkyv.md) records the implemented boundary. The `v1` path segment
remains a key-spelling namespace and is not an archive version.

## permanent seals

The Seal is the only mutable-in-time authority, represented as an immutable
permanent sequence. It is also the complete manifest for the current physical
version:

```rust
struct Seal {
    partition: PartitionId,
    generation: SealGeneration,
    replay: WalReplayPoint,
    directory: TreeVersion,
    ledger: TreeVersion,
    tally: TreeVersion,
    annals: TreeVersion,
}

enum WalReplayPoint {
    Genesis,
    Through { batch: BatchId, owner: OwnerToken },
}
```

Every persisted field has one recovery obligation:

| Field | Why it cannot be derived later |
| --- | --- |
| `partition`, `generation` | authenticate key/body identity and exact succession |
| `replay` | separate the materialized prefix from the readable WAL suffix and retain the owner in force after folding |
| four `TreeVersion`s | name the complete current physical serving state |

`WalReplayPoint::Genesis` represents the virtual cut zero. Real WAL objects
use nonzero `BatchId` values, and every `Through` point records the owner in
force after folding that coordinate.

Genesis is generation 1 at `WalReplayPoint::Genesis` and one canonical empty
`TreeVersion` for each store. Every later Seal is the exact successor of one
observed permanent Seal. Seal keys are never overwritten, deleted, or reused:

```text
1, 2, 3, ...
```

Every Seal carries a complete manifest and can serve while it is newest.  Serving and recovery never need to load an earlier Seal. The Seal generation
is therefore the exact physical view identity; `replay` remains only the
logical WAL coverage boundary and owner at that boundary.

“Self-contained” means that the Seal names the complete dependency graph
without consulting a predecessor manifest. It does not mean SST row bodies or
payload bytes are embedded in the Seal.

## legal seal transitions

Every candidate targets exactly `head.generation + 1`. Skipping a generation
is illegal even if the skipped key appears absent.

### genesis

Provisioning creates one canonical generation-1 Seal with empty trees and
`WalReplayPoint::Genesis`. Genesis grants no ownership.


### claim

A claim changes neither logical nor physical state. It copies the complete
manifest:

```text
candidate.generation = head.generation + 1
candidate.replay = head.replay
candidate.{directory,ledger,tally,annals} = head.{directory,ledger,tally,annals}
```

Only the caller receiving `Direct` may construct `AuthoredClaim`. A
`DurableMatch` proves that the Seal exists but never proves which claimant
created it. A claim authorizes takeover; it does not authorize serving.



### advance

A Ready writer may publish a Seal at a greater WAL cut:

```text
candidate.generation           = source.generation + 1
candidate.replay.batch         > source.replay.batch
candidate.replay.owner         = owner produced by exact fold through candidate.replay.batch
logical(candidate)             = fold(logical(source), selected WAL prefix)
candidate children             = source-carried or candidate-generation-fresh
```

When the source is genesis, its logical source is the empty state at virtual
cut zero; the first advancing Seal ends at a nonzero occupied WAL coordinate
and records the owner in force there.

The stored owner is historical replay state. It need not equal the publisher's
current owner token when a bounded checkpoint stops inside a suffix written by
an earlier owner.

Every fresh table and retained payload pack is durable before the Seal create
is attempted. Only a `Direct` Seal result grants a continuing
`PublishedCheckpoint` capability.


### maintain

A Ready writer may select a new physical version at the same cut:

```text
candidate.generation           = source.generation + 1
candidate.replay               = source.replay
logical(candidate)             = logical(source)
candidate children             = source-carried or candidate-generation-fresh
```

There is no useful same-cut physical rewrite while all four genesis trees are
empty.

Maintenance may compact SSTs, rewrite range boundaries, or move payload bytes.
It must preserve every logical key, extent identity, payload byte, capacity
value, and ordering rule. A result is tied to its exact source Seal identity
and is never rebased onto a newer Seal.


Claims, advances, and maintenance all contend for the same next permanent
Seal coordinate.


### publication outcomes

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

## wal

A WAL coordinate contains one of two disjoint canonical bodies:

```rust
struct WalObject {
    partition: PartitionId,
    batch: BatchId,
    owner: OwnerToken,
    body: WalBody,
}

enum WalBody {
    Run(WalFacts),
    Fence,
}
```

A RUN is non-empty and contains requests in admission order. Each
`OperationFact` is one already-decided public mutation with bounded Ledger,
Tally, and Annals effects plus any append payload and response data required
for deterministic replay.

Facts contain absolute guarded values where replaying a relative instruction
could depend on later state. Examples include producer state, stream tail,
subscription generation, capacity counters, fork pins, lifecycle versions,
and deadline rows.

All effects of one protocol mutation remain in one fact group. An append and
close updates payload history, tail, producer/`Stream-Seq` state, closure,
capacity, subscription fanout, and TTL state together. Fork creation updates
the new Ledger identity, lineage pins, Tally counters, and any initial payload
together. Subscription ack/release similarly advances its cursor, generation,
lease, and next-wake intent as one fact.

One strict reducer is shared by normal commit, replay, generated histories,
and offline verification:

```rust
fn strict_fold(
    state: &mut DurableState,
    fact: &OperationFact,
) -> Result<Applied, FoldContradiction>;
```

Normal admission guarantees `Applied`. During recovery, a duplicate, no-op,
rejection, owner mismatch, offset regression, invalid fork edge, or producer
sequence mismatch is a contradiction: a durable accepted fact must apply
exactly once.


## the life of a mutation

### parse at the edge

The HTTP layer parses foreign bytes into domain values before submission:

- canonical stream and subscription paths;
- content type and JSON framing;
- bounded request body and message count;
- opaque offsets and fork sub-offsets;
- producer ID, epoch, and sequence;
- close, retention, expiry, and conditional fields; and
- authenticated authorization context.

It performs no storage-engine state lookup and never constructs durable object
keys.

### enter bounded ingress

The handler uses non-blocking submission to a bounded many-producer,
single-consumer queue. A full queue means the request was never admitted and
returns retryable load shedding.

Once submitted, client cancellation only drops the response waiter. It does
not cancel a fact that may become durable.

### pure admission

The writer is the sole mutable owner of `AdmittedState`. It decides requests
in queue order through a pure function:

```rust
fn admit(state: &AdmittedState, command: ProtocolCommand) -> Admission;

enum Admission {
    Immediate(ProtocolReply),
    Deferred {
        fact: Option<OperationFact>,
        reply: DeferredReply,
        dependency: DurabilityBarrier,
    },
    Shed(CapacityKind),
}
```

Accepted effects enter `AdmittedState` immediately, allowing a later request
to observe earlier accepted pending work. They remain unreadable and
unacknowledged until durable.

`None` represents an idempotent result whose truth depends on an earlier
uncommitted fact. A new accepted mutation contributes exactly one bounded
`OperationFact`; the WAL RUN supplies group commit across many such facts.

An idempotent response depending on an uncommitted fact inherits that fact's
barrier. For example, a duplicate create cannot return success while the
original create remains only in `PendingRun`.


### group commit

Accepted facts and payload bytes enter one bounded `PendingRun`. The writer has
zero or one immutable WAL create in flight. While one PUT is pending, later
requests accumulate in the next run; there is no batching timer required for
group commit.

The active flight owns its batch, owner, exact canonical bytes, facts, payload,
waiters, and create future. It is never re-encoded after an ambiguous send.

WAL completion is handled before more ingress because it releases the flight
slot and waiting callers.


### commit, publish, reply

For a directly successful WAL create:

```text
create linearizes
-> strict-fold every operation into durable local state
-> install the run in the durable suffix and global overlay
-> publish a new immutable PublishedView
-> release replies through the ordered dispatcher
-> promote PendingRun into the next flight
```

Publication precedes reply. A client receiving success can immediately route
a read that observes the operation.

An ambiguous WAL create may be accepted after an exact byte match because the
Ready writer is the sole legal author of that batch and the bytes are frozen.
A different occupant fences the writer. An unresolved create poisons it. No
Seal read is added to the direct-success path.

Reply release requires:

```text
durability evidence
and recoverability from WAL or the newest Seal's closure
and PublishedView installation
```

A delayed response can arrive after a later checkpoint and WAL collection.
The newest Seal closure may then be the recoverability proof even though the
original RUN has been deleted.

## ownership, takeover, and recovery fencing

Object-store fencing protects history; it does not make a claimed process
Ready. Bootstrap performs:

```text
discover and validate newest permanent Seal H
decode H's bounded inline manifest and capacity metadata
Direct-create claim C = H + 1 carrying H's complete manifest
load complete admission-resident Directory/Ledger state
find and create one permanent first-hole WAL fence for owner C
strictly replay every coordinate after H.replay through that fence
perform one mandatory newest-generation refresh
publish Ready only if the newest generation is still C
```

From `WalReplayPoint::Genesis`, replay starts at batch 1 without a prior owner.

### first-hole fence

RUNs and FENCEs share one coordinate namespace. A claimant places its FENCE at
the first coordinate not occupied by an earlier legal object:

- an older-owner RUN or FENCE is included in replay;
- a same- or newer-owner FENCE makes the claimant stale;
- direct fence creation fixes the inclusive replay endpoint; and
- exact read-back of the claimant's canonical fence may prove the endpoint
  exists, because the fence itself grants no ownership.

FENCE objects are permanent. A paused writer can therefore never regain an old
coordinate after ordinary covered RUNs have been collected.

WAL names use reverse batch ordinals as a placement optimization:

```text
listed_tail = ascending LIST MaxKeys=1
base_through = claimed_seal.replay.batch_or_zero
candidate_tail = max(base_through, listed_tail or none)
try FENCE at checked(candidate_tail + 1)
on collision, re-list and retry
```


LIST chooses the first candidate. Exact GET and strict replay remain authority.
The clamp is required because covered RUN deletion can leave the greatest
surviving object at a historical FENCE below the Seal cut.


### strict replay

Replay begins with the owner recorded by the claimed Seal's replay point, or
with no prior owner at genesis:

```text
FENCE: owner must strictly increase
RUN:   owner must equal the current replay owner
FACT:  strict-fold exactly once in batch and within-run order
```

The claimant's final fence must leave replay owner equal to its claim token.
Any gap, corrupt body, owner violation, duplicate fact, or invalid reducer
transition triggers a head refresh. A greater generation means ordinary
fencing; the claim still being current makes the anomaly a durable
contradiction.

The final generation refresh immediately precedes Ready publication. Removing
it admits a stale-serving execution. There is no need for a Seal query after
every successfully read coordinate.


## published and resident state

The writer maintains three different logical moments because combining them
would either expose pending work or make dependent admission incorrect:

```text
AdmittedState   durable + in-flight + pending effects; writer decisions only
DurableState    newest Seal + proven WAL effects; writer-owned
PublishedView   immutable reader contract; durable effects only
```

The current published view is:

```rust
struct PublishedView {
    identity: ViewIdentity,
    seal: LoadedSeal,
    directory: ResidentDirectory,
    ledger: ResidentLedger,
    tally: ResidentTally,
    annals: AnnalsReadDirectory,
    overlay: DurableWalOverlay,
}
```

`ViewIdentity` includes the route assignment, monotonically increasing local
view version, and exact Seal identity. Because Seal coordinates are permanent
and immutable, generation identifies the durable physical version. A same-cut
maintenance publication changes physical view identity even though its logical
cut does not change.

Directory and Ledger state must be locally queryable before Ready. Their
representation may use RAM, packed local files, persistent maps, or local NVMe
after measurement. It is always reconstructed from S3 and never becomes
authority. The exact Tally and Annals resident/read planning shapes remain
deferred to their RFCs.

The global post-W overlay is keyed by complete logical keys. It carries no
physical range identity:

```text
PublishedView = Seal trees + global WAL overlay after Seal.replay
```

That one choice lets same-cut compaction and range changes install without
repartitioning a large local suffix.

Published views are immutable. Read tasks borrow a large view synchronously,
copy one bounded seed, and release it before any await. Long-poll and SSE tasks
retain only a small watch/version token while sleeping.


## one range-lsm mechanism

All four projections use the same durable structure:

```rust
struct TreeVersion {
    ranges: NonEmptyVec<RangeVersion>,
}

struct RangeVersion {
    start: KeyBound,
    end: KeyBound,
    runs: Vec<SortedRun>, // newest to oldest
}

struct SortedRun {
    tables: NonEmptyVec<TableRef>,
}

struct TableRef {
    object: TableObjectId,
    object_bytes: NonZeroU64,
}
```

The comparator is fixed by the store and is not repeated inside each
`TreeVersion`. The first materialized slice reads complete bounded Directory
and Ledger SST objects into resident state. Indexes, filters, block locations,
and footer references remain deferred until a measured remote-read workload
requires them.

The complete inline Seal manifest obeys:

```text
ranges are sorted, gap-free, non-overlapping, and cover the keyspace
every key belongs to exactly one range
every SST is owned by exactly one store, range, and run
every SST key lies inside its owner range
tables inside a run are ordered and key-disjoint
runs are ordered only by their position in the `TreeVersion`, newest first
one run contains at most one value or tombstone per key
all ranges, runs, tables, refs, counts, keys, and bytes are bounded
```

The last run is simply the oldest run. There is no separate `Bottom` state
machine.

### sst format

Directory and Ledger each use one concrete checked rkyv root containing the
partition, fresh table identity, and complete ordered row collection. The
concrete root supplies the store kind rather than archiving it twice:

```text
DirectorySstArchive -> [(DirectoryKey, DirectoryEntry)]
LedgerSstArchive    -> [(StreamUid, LedgerCell)]
```

Their typed `TableKey` and decoder choose the expected root, so there is no
shared container tag, header, row codec, footer, nested `StreamRecord` frame,
or standalone `StreamRecord` codec.

Decode checks the complete object bound before archive access. It then makes a
preflight pass over the checked archive to validate identity, non-emptiness,
strict ordering, row semantics, and decoded resource bounds before reserving
the owned result. A second pass reconstructs keys and values through their
domain constructors. Tally and Annals remain canonically empty until their own
RFCs define rows and access paths.


### point and range reads

The current Directory and Ledger slice reads complete bounded SSTs during
bootstrap and serves point and range operations from resident state. Remote
point-read filters, indexes, and block addressing remain deferred until a
measured workload requires them.

### flush

For a checkpoint prefix `(P, W]`:

- Directory, Ledger, and Tally retain the final absolute value for each
  changed key;
- Annals retains ordered extents and exact point tombstones;
- values are grouped using the candidate Seal's range boundaries; and
- each touched range receives one newest run, split into whole bounded SSTs.

Untouched ranges, runs, and tables are carried exactly from the immediate
source Seal.

### run compaction

Within one range, compaction consumes a contiguous interval of adjacent runs.
The newer input wins and the output replaces the interval at the same
precedence position.

A point tombstone may disappear only when the selected interval reaches the
oldest run. Otherwise an older value could reappear. Directory's permanent path
tombstones remain logical values regardless of physical coverage. Tally and
Annals deletion tombstones may be removed only after complete older coverage
and their logical lifecycle rules permit absence.

The initial scheduler is size-tiered:

- merge similarly sized adjacent upper runs;
- compact an old suffix under tombstone or physical-space pressure;
- stay below the hard per-range run count; and
- fully compact a range before changing its boundaries.

Compaction policy is tunable. Merge precedence and tombstone legality are
format semantics.


## garbage collection

Collection derives proof from one captured permanent head and its complete
inline manifest. It holds no durable cursor. LIST pages and candidate sets are
bounded, and a crash may restart from the beginning.

Safety permits leaks. Failure, uncertainty, corruption, or an incomplete proof
always fails closed.


### seals

Seal keys are never deleted. Permanence is what makes exact-successor create a
fence even after arbitrary process pauses. Because the complete manifest is
inside each Seal, there is no separate manifest object to trace or collect.

### wal runs and fences

WAL uses a separate deletion theorem. Birth generation is not evidence that a
not-yet-materialized RUN is dead.

A decoded RUN at batch B may be deleted only when:

```text
B <= H.replay.batch
and every logical effect is represented by the complete Seal
and every retained payload byte formerly sourced from B is in a reachable pack
and exact-validator conditional deletion succeeds
```

A RUN after `H.replay` is never collectible. A FENCE is never deletable
or replaceable by collector policy. The typed delete constructor accepts a
decoded eligible RUN and its exact GET validator; it cannot accept a FENCE or
raw key. `404`, `409`, or `412` discards the proof and replans without an
unconditional fallback.


### sst

Exact SST gc logic is deffered.


## forks, retention, and lifecycle

Forks are explicitly deffered to a followup RFC


## subscriptions and delivery

This is explicitly deffered to a followup RFC


## bootstrap state machine

Bootstrap is an explicit event-driven machine, not one helper with hidden
retries:

```rust
enum BootstrapPhase {
    DiscoverHead,
    ReadHead { generation: SealGeneration },
    PublishClaim { prepared: PreparedClaim },
    LoadAdmissionBase { claim: AuthoredClaim },
    PlaceFence { claim: AuthoredClaim, candidate: BatchId },
    Replay { next: BatchId, fence: BatchId },
    RefreshAnomaly,
    FinalRefresh,
    Ready,
}
```

The implementation may split effect-start and effect-completion variants to
satisfy borrowing, but it does not hide transitions or retry indefinitely
inside adapters.

Bootstrap performs the bounded Seal decode and structural capacity checks
before competing for ownership. The same GET supplies the complete manifest;
there is no second metadata fetch. Only a direct claimant pays to load and
validate the complete resident Directory and Ledger SST archives. This avoids
several contenders performing full cold bootstrap work.

The partition does not accept traffic until:

- the newest self-contained Seal is decoded;
- its range structure, TableRefs, exact object lengths, and bulk source bounds
  are valid;
- admission-resident Directory/Ledger state is built and cross-checked;
- the claim was directly authored;
- the first-hole FENCE was established;
- strict replay reached that fence with the claimed owner;
- the final newest-generation observation still matches; and
- the recovered PublishedView is installed.

Missing current children, corruption, contradictory counts, or an over-bound
current source make the partition unready. Recovery never interprets absence
as an empty tree or silently scans backward to an older convenient Seal.


## storage adapter APIs


The engine depends on narrow private contracts rather than a general-purpose
object-store handle:

```rust
trait SealStore {
    async fn create_seal(
        &self,
        candidate: EncodedSeal,
    ) -> Result<CreateEvidence, SealStoreError>;

    async fn newest_generation(
        &self,
        partition: PartitionId,
    ) -> Result<Option<SealGeneration>, SealStoreError>;

    async fn read_seal(
        &self,
        identity: SealIdentity,
    ) -> Result<Option<DecodedSeal>, SealStoreError>;
}

trait WalStore {
    async fn create_wal(
        &self,
        candidate: EncodedWal,
    ) -> Result<CreateEvidence, WalStoreError>;

    async fn read_wal(
        &self,
        identity: WalIdentity,
    ) -> Result<Option<ObservedWal>, WalStoreError>;

    async fn newest_surviving_batch(
        &self,
        partition: PartitionId,
    ) -> Result<Option<BatchId>, WalStoreError>;

    async fn delete_run(
        &self,
        proof: AuthorizedWalRunDelete,
    ) -> Result<ConditionalDeleteObservation, WalStoreError>;
}

trait ContentStore {
    async fn create_or_verify(
        &self,
        object: EncodedContentObject,
    ) -> Result<DurableContentObject, ContentStoreError>;

    async fn read_object(
        &self,
        identity: ContentObjectId,
    ) -> Result<Option<DecodedContentObject>, ContentStoreError>;

    async fn read_range(
        &self,
        range: AuthenticatedObjectRange,
    ) -> Result<Option<VerifiedRangeBytes>, ContentStoreError>;

    async fn list_page(
        &self,
        request: BoundedContentPageRequest,
    ) -> Result<ContentPage, ContentStoreError>;

    async fn delete(
        &self,
        proof: AuthorizedContentDelete,
    ) -> Result<DeleteObservation, ContentStoreError>;
}
```

These signatures are responsibility sketches. Concrete private traits may be
combined when that removes ceremony, but the special rules for newest-head
listing, exact WAL deletion, authenticated range construction, and typed
content deletion must remain impossible to bypass.


## failure, fencing, routing, and shutdown

Courant is fail-stop around uncertainty.

```rust
enum WriterExit {
    Shutdown,
    Fenced { observed: SealGeneration },
    Poisoned { coordinate: DurableCoordinate, detail: String },
    Contradiction { coordinate: DurableCoordinate, detail: String },
}
```

- `Fenced` is normal ownership movement.
- `Poisoned` means an effect may have happened but this process lacks evidence
  to continue.
- `Contradiction` means durable bytes violate the storage model.
- Capacity shedding occurs before fact creation and is not a writer exit.

On poison or fencing, the writer revokes readiness, stops admission, withholds
unresolved definitive replies, abandons non-authoritative candidates, records
exact durable coordinates, and exits for fresh bootstrap. It never reports
definite failure while a request may still linearize.

Object-store fencing does not revoke an HTTP socket. The router exposes exactly
one assignment for a partition and uses a generation-fenced response gate:

```text
revoke old gate
-> stop admitting old mutation/read chunks
-> expose new Ready assignment
```

A read chunk already admitted through the atomic gate may finish. An acquired
but ungated chunk is discarded. A non-atomic “check route, then write headers”
does not refine this contract.

Graceful shutdown first removes the assignment from routing, closes ingress,
drains already accepted commands, resolves the one active WAL create,
publishes every proven result, stops starting maintenance, and allows finished
content PUTs to become selected or orphaned. It does not delete a Seal or
FENCE to relinquish ownership.

Forced shutdown can abandon client connections. Recovery determines durable
outcomes afterward.



## Final model

The complete recovery equation is:

```text
CurrentState
    = LogicalState(newest Seal's inline tree versions)
      folded with every valid WAL RUN after Seal.replay
      in coordinate and within-RUN order
```

The complete publication equation is:

```text
current exact source
    + exact selected WAL prefix or logically equivalent maintenance
    + source-carried or successor-fresh immutable children
    -> one complete exact-successor permanent Seal
```

The complete deletion rule is:

```text
delete only when one captured current composition
proves the object is no longer nameable,
and no legal successor can resurrect it
```

The intended system has one flow:

```text
the Seal commits
the WAL remembers
the forest organizes
the packs carry bytes
GC removes only what the current composition can no longer name
```

This design provides us with, a single round trip append, exact failover, atomic protocol facts, bounded incrimental compaction, ordered stream reads, payload seperation all while using only s3 as source of both durability and authority.
