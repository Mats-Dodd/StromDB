# Durable streams server architecture

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