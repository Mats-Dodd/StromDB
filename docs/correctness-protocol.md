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

