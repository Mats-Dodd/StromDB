# durable streams server architecture

This document specifies StromDb's storage engine.  Its correctness protocol, storage layout, program shape, writes, reads, maintenance and garbage collection. 

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


