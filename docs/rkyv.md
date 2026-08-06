# rkyv durable object boundary

## Status

This document records the implemented durable-codec boundary. It is not an
accepted compatibility RFC.

StromDB has not shipped and has no stored-data compatibility promise. The old
Postcard, envelope, CRC-32C, and hand-written SST representations have been
deleted rather than retained behind compatibility readers. Writers emit only
the rkyv representation described here, and readers accept only that
representation.

The change is deliberately narrow. rkyv replaces generic byte-layout work;
it does not replace StromDB's domain model, object-store adapter, LSM shape,
or correctness protocol.

## Decision

Every durable object family has one concrete archived root selected by its
typed storage location and decoder:

```text
SealKey                 -> ArchivedSeal
WalKey                  -> ArchivedWalObject -> Run | Fence
TableKey(Directory)     -> ArchivedDirectorySstArchive
TableKey(Ledger)        -> ArchivedLedgerSstArchive
```

There is no universal stored-object enum or outer envelope. WAL remains an
enum because one WAL coordinate may contain either a run or a fence.
`StreamRecord` is archived only as part of `LedgerCell::Value` inside a Ledger
SST. It has no standalone codec or object location.

The implementation uses the official rkyv crate with an exact release and
explicit format controls:

```toml
[workspace.dependencies]
rkyv = {
    version = "=0.8.18",
    default-features = false,
    features = [
        "std",
        "bytecheck",
        "unaligned",
        "little_endian",
        "pointer_width_32",
    ],
}
```

Only `strom-storage-domain` depends directly on rkyv. There is no schema
crate, compiler, generated source tree, `build.rs`, FlatBuffers tool, or
devenv pin. Cargo owns the complete archive dependency and configuration.

The workspace continues to forbid unsafe code. Every read uses
`rkyv::access`; unchecked access APIs are not part of the implementation.

## What rkyv owns

rkyv owns representation mechanics:

- scalar and enum layout;
- relative pointers;
- strings and collection lengths;
- structural validation of pointers, bounds, and discriminants; and
- access to a checked archived view without an intermediate owned wire graph.

StromDB still owns storage semantics:

- complete object-size bounds;
- key/body identity agreement;
- collection and resident-memory limits;
- reconstruction through domain constructors;
- strictly ordered and unique SST rows;
- tree, store, generation, and replay invariants; and
- all-or-nothing publication and recovery.

The simplification is that the project validates only its own facts. It no
longer implements generic frames, cursor arithmetic, declared lengths,
checksums, or enum tags.

## Format contract

rkyv is not a self-describing interchange protocol. The current archive
contract is the combination of:

1. the Rust root and field types;
2. rkyv `0.8.18`; and
3. the five enabled features shown above.

`little_endian` and `pointer_width_32` make scalar and relative-pointer choices
explicit. `unaligned` allows safe checked access to arbitrary object-store
byte slices without copying into aligned storage. `bytecheck` provides the
checked access path.

There is no content-format version, magic value, object-kind byte, checksum,
or compatibility envelope. The `v1` segment in durable object keys is a
key-spelling namespace and is independent of the bytes stored at that key.

rkyv locates a root at the end of its input slice. Readers therefore tolerate
unreachable leading bytes inside the complete-object bound. Writers never emit
that padding, and exact-byte create reconciliation still compares the complete
candidate body, so a padded body cannot masquerade as bytes this writer
produced. Rejecting unreachable prefixes would require a framing length or a
full re-encode comparison; neither buys a storage invariant worth
reintroducing that machinery before release.

An rkyv upgrade is a deliberate archive-format change even if Cargo would
otherwise consider it semver compatible. Before StromDB has a release
compatibility promise, local objects and archive fixtures may be replaced.
The exact dependency pin makes such changes visible in review.

## Crate boundaries

The archive port preserves the project layering:

```text
strom-domain
    protocol values and canonical parsers

strom-storage-domain
    storage vocabulary, archive spelling, checked decode, durable bounds

strom-object-store
    opaque immutable bounded bytes

stromdb
    typed stores, fold, merge, resident budgets, correctness protocol
```

`strom-domain` remains rkyv-free. Protocol values such as
`StreamContentType`, `ExpiryPolicy`, and `StreamLifecycle` do not acquire
storage-format derives.

`strom-storage-domain` owns all rkyv calls, archived roots, protocol adapters,
semantic decode, and public codec errors. Generated archived type names are
not re-exported.

`strom-object-store` remains format-blind. Encoders return a completed
`Vec<u8>`; `FrozenBytes::try_from` moves that allocation into `bytes::Bytes`
without a complete-buffer copy.

`stromdb` joins each typed key and expected identity with the concrete
decoder. rkyv does not absorb engine policy or cross-object invariants.

## Archive modeling

### Storage-owned values

Storage-owned values derive `Archive` and `Serialize` directly:

```rust
#[derive(rkyv::Archive, rkyv::Serialize)]
pub enum WalObject {
    Run(WalRun),
    Fence(WalFence),
}
```

The derive is applied transitively to storage identifiers, Seal manifests,
WAL facts, Directory keys and entries, and Ledger values. These types do not
derive `rkyv::Deserialize`.

Decode deliberately reads a checked archived view and then reconstructs an
authoritative value through explicit conversions and constructors. A generic
deserializer would obscure the point at which StromDB invariants are restored.

Public APIs remain domain-shaped:

```rust
pub fn encode_wal(object: &WalObject) -> Result<Vec<u8>, EncodeError>;

pub fn decode_wal(
    identity: &WalIdentity,
    bytes: &[u8],
) -> Result<WalObject, DecodeError>;
```

### Protocol-owned values

The private `archive` module contains three narrow adapters for protocol
values embedded in storage objects:

```rust
enum ExpiryArchive {
    None,
    SlidingTtl(u64),
    AbsoluteExpiry(i128),
}

enum LifecycleArchive {
    Open,
    Closed,
}
```

Content type is archived from a borrowed canonical `&str`; encoding allocates
no temporary string per fact or row. Decode requires the archived spelling to
be canonical and then passes it through `StreamContentType` parsing. TTL and
absolute expiry use their canonical domain conversions, and lifecycle is
mapped exhaustively. The adapters use rkyv's `ArchiveWith` and
`SerializeWith` traits, so no remote-derive helper dependency is needed.

### Borrowed SST roots

The two SST encoding roots borrow their existing row slices:

```rust
#[derive(rkyv::Archive, rkyv::Serialize)]
struct DirectorySstArchive<'rows> {
    partition: PartitionId,
    fresh: FreshIdentity,

    #[rkyv(with = rkyv::with::InlineAsBox)]
    rows: &'rows [(DirectoryKey, DirectoryEntry)],
}
```

Ledger uses the same shape with `(StreamUid, LedgerCell)` rows. The concrete
root and decoder supply the store kind, so the archive does not carry a
redundant Directory/Ledger tag. The supplied `TableKey` is still rejected when
it names the wrong store. `InlineAsBox` writes the borrowed slice into the
completed archive, so the encoder does not construct a second owned row graph.

Seal and WAL archive their existing owned graphs directly; they need no root
wrapper.

## Encoding

All object families share one small helper built from rkyv's high-level
serializer:

```rust
let mut writer = BoundedWriter::new(bytes_max);
let mut arena = rkyv::ser::allocator::Arena::new();

rkyv::api::high::to_bytes_in_with_alloc::<_, _, rkyv::rancor::Failure>(
    value,
    &mut writer,
    arena.acquire(),
)?;
```

`BoundedWriter` refuses the first write that would cross the object-family
limit, so an over-bound value is never fully materialized. Seal and WAL use
their 1 MiB and 4 MiB bounds. SSTs additionally reject an impossible row count
or minimum archived-row footprint before iteration, then use the 128 MiB
writer bound. This bounds encode work even when a caller supplies a grossly
oversized slice.

`Failure` intentionally erases rkyv's internal error detail. Callers can act
on serialization failure or an over-bound object, not on dependency-specific
error structure. rkyv re-exports `rancor`, so no direct rancor dependency is
needed.

Each call owns a scoped scratch arena. A large SST therefore releases its
resolver storage when encoding returns instead of retaining the largest block
on an async worker's thread-local arena. The output is an ordinary `Vec<u8>`
because the `unaligned` format removes the need for `AlignedVec`.

The output starts with `Vec::new()` and grows through exact reserve requests.
A maximum size is a rejection bound, not a useful allocation capacity;
preallocating 128 MiB for every SST would be wasteful. A measured workload may
justify a bounded input-derived capacity estimate later without changing the
archive API.

Directory and Ledger SST encoders additionally reject the wrong store, empty
tables, and rows that are not strictly ordered before serialization.

## Checked decode

Every decoder begins with the same structural sequence:

```text
complete byte bound
    -> rkyv::access of the concrete root
    -> resource and identity gates
    -> explicit domain reconstruction
    -> owned result
```

Checked access is direct:

```rust
use rkyv::rancor::Failure;

let archived = rkyv::access::<ArchivedWalObject, Failure>(bytes)
    .map_err(|_archive_error| DecodeError::MalformedArchive)?;
```

The raw byte bound runs first. `bytecheck` then validates the archived
structure without materializing the result. No decoder calls
`rkyv::from_bytes`, and no decoder exposes or retains an archived type.

The schemas are shallow and non-recursive, so the default checked-access
validator is sufficient. A recursive durable shape would require a separately
designed validation-work bound.

### Seal

Seal archives the existing `Seal` graph directly:

```text
Seal
├── partition and generation
├── WalReplayPoint
├── Directory TreeVersion
├── Ledger TreeVersion
├── Tally TreeVersion
└── Annals TreeVersion
```

Decode checks the 1 MiB complete-object bound, obtains a checked
`ArchivedSeal`, and compares its partition and generation with the supplied
`SealIdentity` before allocating manifest collections. It then gates ranges,
runs, tables, and key-bound lengths, reconstructs the manifest, and calls
`Seal::new`.

The constructors retain the real semantic checks: one full-keyspace range,
bounded non-empty runs, table store and birth-generation agreement, unique
table identities, bounded object lengths, and canonically empty deferred
Tally and Annals trees.

### WAL

`WalObject` is the direct root enum. Decode checks the 4 MiB bound, obtains a
checked archive, and reconstructs the partition and batch before comparing
them with the supplied `WalIdentity`.

A run must contain between one and `WAL_RUN_FACTS_MAX` facts. That gate runs
before allocating the result vector. Each fact then reconstructs Directory
paths, content type, and expiry through their canonical domain boundaries.
Checked archived nonzero coordinates convert directly to their domain
newtypes without manufacturing impossible zero-error branches.

### Directory and Ledger SSTs

Each SST decoder accepts a typed `TableKey` and a complete object byte slice.
After the 128 MiB gate and checked root access, it reconstructs the archived
partition and fresh identity. The concrete decoder supplies the store kind;
these fields are compared with the expected location.

The root must contain at least one row. Decode then makes a preflight pass over
the archived rows before allocating the result vector:

- Directory checks canonical key spelling without allocating, strict byte
  ordering, and the conservative resident logical-byte total.
- Ledger checks strict UID ordering, complete record semantics, and the
  conservative resident logical-byte total.

The preflight also caps row count at the partition path-occupancy bound. Only
after those resource-sensitive gates succeed does `try_reserve_exact` reserve
the result and a second pass reconstruct the complete owned row set. Directory
keys reuse the canonical `StreamId` validation before making their one owned
copy; Ledger values rebuild `StreamRecord` through its constructor and
protocol adapters. Decode returns no partial table.

There is no codec-only row-count constant. The existing partition
path-occupancy bound, complete-object bound, and conservative resident
logical-byte bound govern the collection.

## Zero-copy boundary

rkyv makes the structural and inspection phase zero-copy:

```text
object-store bytes
    -> checked archived root borrowing those bytes
    -> explicit domain reconstruction
    -> owned StromDB value
```

The checked archive reads strings, vectors, enums, and scalars from the input
buffer. There is no intermediate owned wire object graph. Current public APIs
still return owned Seal, WAL, and SST values, so domain reconstruction makes
the allocations those values require.

This is the intentional boundary. Retaining borrowed archived views in the
engine would introduce lifetime-bearing APIs and is not justified by the
current whole-table bootstrap workload.

## Error boundary

The common public errors expose storage decisions, not rkyv internals:

```rust
enum EncodeError {
    Serialization,
    EncodedBytesOverMax { bytes_max: usize },
}

enum DecodeError {
    EncodedBytesOverMax { bytes_max: usize, bytes_actual: usize },
    MalformedArchive,
    InvalidBody,
    IdentityMismatch,
}
```

`SstDecodeError` distinguishes only decisions its caller can act on: supplied
store misuse, a byte/resource limit, malformed structure, invalid body, or
key/body identity disagreement. Parser-internal distinctions such as empty,
unordered, or invalid rows collapse into `InvalidBody`. Encoding retains
specific local-input errors for empty and unordered rows. rkyv, bytecheck, and
rancor types do not appear in public error variants.

## Dependency choices

The implementation adds exactly one direct dependency to
`strom-storage-domain`: rkyv.

It uses rkyv's re-exports of bytecheck and rancor. `rend`, `ptr_meta`, and
`munge` remain implementation details. Optional collection integrations are
disabled because no archived root needs them.

No companion crate is justified in this slice:

- `rkyv_util::OwnedArchive` solves retention of an archived view, which the
  current owned-returning APIs do not do.
- `rkyv_intern` would add speculative archive-time deduplication.
- `rkyv_dyn` targets archived trait objects, which these concrete roots do not
  contain.
- third-party remote-derive helpers add more surface than the three explicit
  protocol adapters remove.

Postcard and CRC-32C are gone from the workspace. Serde remains where the
protocol layer still uses it independently of durable storage.

## Repository shape

The implementation adds no crate or general codec framework:

```text
strom-storage-domain/src/
├── archive.rs
├── seal.rs
├── seal/
│   └── codec.rs
├── wal.rs
├── wal/
│   └── codec.rs
├── sst.rs
└── sst/
    ├── directory.rs
    └── ledger.rs
```

`archive.rs` contains the shared encode/bound helpers and protocol adapters.
Concrete roots and semantic conversion remain beside the object family that
owns them.

The port deleted:

- the common envelope and its magic, kind, version, and CRC handling;
- private Postcard wire graphs and custom Serde visitors;
- the standalone `StreamRecord` codec;
- manual SST headers, cursors, prefix compression, frames, and tags;
- `SealFormat` and content-codec version constants; and
- Postcard/CRC dependencies and byte-layout tests that asserted only the
  deleted representation.

No deprecated wrappers, fallback readers, or alternate encoders remain.

## Test contract

The codec suite anchors the boundary StromDB relies on:

- representative and property-generated Seal, WAL, Directory, and Ledger
  values round-trip;
- minimal archive fixtures independently anchor each concrete root;
- checked access rejects truncated archives;
- deliberately misaligned slices decode under the selected `unaligned`
  format;
- unreachable leading padding is accepted but never emitted;
- byte bounds run before archive access;
- key/body identity mismatches fail closed;
- WAL fact limits are tested at and above the bound;
- SST encoders reject empty, unordered, and wrong-store input;
- SST decoders reject structurally valid unordered rows and validate structure
  before location identity; and
- protocol adapters reconstruct values through canonical domain conversions.

Round trips alone could allow an encoder and decoder to drift together. The
small fixtures make archive-layout changes visible without preserving the
large old suites for envelopes, checksums, cursor offsets, or prefix spelling.

## Compatibility policy

There is no compatibility mechanism in this implementation. Existing local
objects may be deleted, and pre-release archive fixtures may be deliberately
updated with a root or rkyv configuration change. No field or reader branch is
reserved for a hypothetical future version.

Before StromDB ships a durable-format promise, the project must revisit this
policy. It can then freeze archive DTOs, retain decode fixtures from released
writers, and introduce an explicit version only for a concrete incompatible
change. That future mechanism does not belong in the current pre-release
format.

## Result

The durable boundary is now four concrete roots, checked access, thin domain
reconstruction, one exact dependency, and only the semantic checks StromDB
uniquely owns. The project no longer maintains a parallel serialization
subsystem.
