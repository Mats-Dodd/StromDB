# Correctness Protocol

This document outlines StromDB's initial correctness protocol for immutable publication, writer takeover, WAL durability, materialization, recovery, and gc. 

## 1. Decision

Each partition has one permanent, immutable, contiguous sequence of Seal
objects. Once `seal/<generation>` is created, neither Strom nor an object
store lifecycle rule may delete, overwrite, or reuse that key.

There is no mutable manifest pointer, GC anchor, Seal-retention floor, or Seal
collector. Every legal Seal is the exact successor of an observed Seal:

```text
1, 2, 3, ...
```

This permanence removes the Seal-slot ABA problem. A directly successful
conditional create of generation `H + 1` proves that `H + 1` was the unique
maximum at the create's linearization point: a legal `H + 2` cannot already
exist while `H + 1` is absent. A second post-create head query would establish
another historical freshness point, but could itself become stale before its
response is delivered. It is therefore not part of the correctness protocol.

The normal append path remains one object-store round trip:

```text
admit and batch
      |
      v
create-only PUT wal/<batch>       one object-store request
      |
      v
strict-fold durable state
      |
      v
publish the immutable in-memory view
      |
      v
release successful replies
```

Claims and checkpoints are cold-path conditional creates. A claim does not
permit serving by itself. A claimant must install a permanent WAL fence,
replay exactly, and perform one final current-generation check immediately
before it publishes readiness.

## 2. Guarantees and proof boundary

Under the storage, process, and deployment contracts below, Strom preserves:

1. **Permanent Seal history.** Every created Seal key retains exactly one byte
   sequence forever.

2. **One recovery head.** The greatest Seal generation observed by a strong
   ordered LIST is the only recovery head for that observation.

3. **Exact succession.** Every Seal after genesis is generation `H + 1` and is
   derived from the exact permanent Seal at H. Generations never skip.

4. **Authored claims.** Only the caller that receives direct success for a
   claim create obtains an `AuthoredClaim`. Byte equality does not confer
   ownership.

5. **Fenced readiness.** A claimant publishes a serving view only after one
   permanent first-hole fence, exact ordered replay, and a final observation
   that its claim generation is still current.

6. **One-PUT appends.** An uncontended ordinary append needs no Seal or manifest
   read after its direct WAL PUT.

7. **Publish before reply.** Success is released only after durable evidence,
   strict state installation, and publication of the corresponding view.

8. **Monotonic cuts.** Materialized WAL watermarks and the owner at those cuts
   never regress.

9. **Complete recovery.** The current derived roots plus the WAL suffix above
   the current watermark reconstruct every acknowledged fact exactly and in
   order.

10. **Reachability-safe GC.** A legal collector never deletes a current recovery
    dependency or a WAL fact not already represented by a complete checkpoint.

11. **Permanent WAL fences.** A fence coordinate is never reopened.

Safety permits garbage: abandoned roots, physically published but locally
unaccepted Seals, reappeared covered runs, and stale objects may remain. They
are not alternate partition state.

## 3. Required storage and deployment capability

Strom is defined against a narrow adapter capability.


### 3.1 Object semantics

For Strom owned prefixes, the adapter MUST provide:

1. linearizable create-if-absent, with exactly one direct winner;

2. immutable bytes while a key is present;

3. strongly consistent GET and LIST operations;

4. read-after-create visibility;

5. lexicographic server-side listing with a bounded page and exclusive start;

6. an atomic exact-validator conditional delete for the current WAL content,
   using an opaque validator returned by the exact GET;

7. idempotent deletion for other collectable content;

8. bounded decoding with length, envelope, checksum, canonical-key, and
   key/body checks; and

9. exclusive IAM control: users, lifecycle rules, and incompatible binaries
   cannot mutate strom-owned keys.


The Seal namespace denies deletion outright. Runs and fences deliberately
share the same `wal/<batch>` keyspace, so there is no enforceable fence-only IAM
prefix. Lifecycle rules and non-collector credentials deny deletion across the
whole WAL namespace; only the collector role may issue WAL deletes. Its private
typed API accepts an `AuthorizedWalRunDelete` constructed from a decoded `Run`
at or below an observed W plus that exact read's validator, and cannot accept a
raw key or `Fence`. The delete linearizes only if the current object still has
that validator. Fence permanence is therefore a protocol/type/refinement
invariant backed by conditional deletion and credential isolation, not a claim
that S3 can inspect the object body in an IAM prefix rule.


The S3 adapter uses [conditional `DeleteObject` with
`If-Match`](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html).
Bucket policy [requires the conditional-delete
header](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-delete-enforce.html)
and rejects missing or wildcard-only validators for the WAL namespace. The
adapter treats `404`, `409`, and `412` as “candidate no longer proven current”:
discard the delete capability and replan. It never falls back to an
unconditional delete.


`ObjectValidator` is an exact-content validator, not an incarnation number:
recreating the same run bytes may recreate its ETag and is safe to delete below
W. Correctness requires the supported WAL PUT mode and threat model to treat
different canonical WAL bodies, especially RUN versus FENCE, as distinct under
that validator. This is an explicit cryptographic/backend premise, verified for
the exact production upload and encryption configuration. An embedded nonce
would only randomize already-distinct bodies; it could not turn S3's ETag
comparison into a mathematical incarnation proof. A deployment that cannot
accept this premise needs a true version-identity design, not another Seal
field.


The supported S3 adapter uses general-purpose S3 buckets and a direct
`ListObjectsV2` query. S3 Express directory buckets are not supported by this
head lookup because their listing and `StartAfter` behavior do not provide the
required ordered query.

The generic `object_store::ObjectStore::list` API does not promise ordering.
The engine therefore uses a narrower interface:

```rust
trait SealStore {
    async fn create_seal(
        &self,
        candidate: &EncodedSeal,
    ) -> Result<CreateEvidence, SealStoreError>;

    async fn newest_generation(
        &self,
        partition: PartitionId,
    ) -> Result<Option<SealGeneration>, SealStoreError>;

    async fn read_seal(
        &self,
        partition: PartitionId,
        generation: SealGeneration,
    ) -> Result<Option<DecodedSeal>, SealStoreError>;
}
```

### 3.2 Create evidence

The adapter normalizes conditional-create outcomes:

```rust
enum CreateEvidence {
    Direct,       // this request received the winning create response
    DurableMatch, // exact bytes are present; the author is unknown
    NotOurs,      // different bytes occupy the permanent coordinate
    Unresolved,   // the request may still take effect
}
```

`DurableMatch` proves content durability but not claim authorship. An adapter
MUST NOT manufacture `Direct` by transparently issuing a fresh create after an
ambiguous request. A later absent GET does not prove that an in-flight request
cannot still land.


### 3.3 Namespace

Seal generations use reverse-ordered fixed-width names:

```text
partition/<partition>/seal/<20-digit-storage-ordinal>
storage_ordinal = u64::MAX - generation
```

Ascending lexicographic order places the greatest generation first. One
server-side page with `MaxKeys=1` finds it, independent of partition age.

Other coordinates are forward ordered:

```text
partition/<partition>/wal/<20-digit-batch>
partition/<partition>/ledger/<20-digit-through>
partition/<partition>/tally/<20-digit-through>
partition/<partition>/annals/<20-digit-through>
partition/<partition>/annals-node/<creation-cut>/<ordinal>
partition/<partition>/payload/<source-batch>/<ordinal>
```

Generation and batch counters use checked arithmetic, never wrap, and produce
a typed terminal error at exhaustion. Non-canonical names under an owned
prefix are contradictions. Full historical namespace auditing is an offline
maintenance operation, not part of O(1) head discovery.