# Testing architecture

This document defines the test architecture for StromDB.

The main rule is:

> Test protocol decisions as data, test storage failures through the real
> adapter seam, and test durable claims across reopen.

[`stromstyle.md`](stromstyle.md) defines the general code and test style. This
document defines the concrete test structure for the storage system.

## Purpose

StromDB uses immutable object storage as its only durable authority. Correctness
depends on more than successful reads and writes. The engine must also behave
correctly when:

- a create succeeds but its response is lost;
- a create fails before it reaches storage;
- an occupant already owns an immutable coordinate;
- a reconciliation read fails or sees absence;
- one child of a checkpoint fails;
- Seal publication has an ambiguous result;
- collection stops after some deletes;
- a writer loses authority during an operation;
- the process stops at a durable boundary; and
- recovery observes partial work that was never published.

An in-memory object store proves normal storage behavior. It cannot produce all
of these cases by itself. Tests need one deterministic, scripted object store at
the existing storage seam.

## Current assessment

The existing suite has strong tests for:

- pure forest transitions;
- admission rules;
- checkpoint planning;
- durable codecs and golden fixtures;
- normal adapter behavior;
- normal writer durability;
- public engine reopen; and
- writer succession and fencing.

The existing suite has weak coverage for:

- adapter error classification;
- ambiguous creates through the adapter;
- bootstrap storage failures;
- WAL reconciliation failures;
- checkpoint table reconciliation;
- Seal publication failures;
- partial collection failures;
- exact shutdown and publication boundaries; and
- recovery after each failure point.

Some writer tests install a completed flight with a chosen `CreateEvidence`.
This is valid for a pure writer transition test. It does not prove that the
adapter derives that evidence from backend behavior. Such a test must not claim
storage or resend behavior.

Repeated calls to `ObjectStoreAdapter::in_memory()` are not separate mocks.
They use one central implementation. The problem is that this implementation
has no script, no fault control, no operation gate, and no strict trace.

## Design goals

The test system must:

1. Use the production `ObjectStoreAdapter`.
2. Inject faults below the adapter.
3. Use the existing `object_store::ObjectStore` trait.
4. Keep scripts fixed, deterministic, and directly reviewable.
5. Model failure before and after a durable effect.
6. Support concurrent checkpoint operations without assuming one global order.
7. Expose exact events instead of using sleeps or scheduler polling.
8. Record enough detail to diagnose a failure from test output.
9. Check durable state across shutdown, process loss, and reopen.
10. Remove duplicate setup and duplicate contract tests.

## Non-goals

The test system is not:

- a general mock framework;
- a simulator for every S3 feature;
- a second storage abstraction;
- a replacement for pure protocol tests;
- a replacement for codec fixtures;
- a replacement for a small real-S3 contract suite; or
- a probabilistic fault system.

Do not add traits for `SealStore`, `WalStore`, or `TableStore` to support tests.
Do not add a second raw-store trait. The external `ObjectStore` trait is already
the correct injection seam.

## Test layers

The suite has four layers. Each layer has one job.

### Pure protocol tests

Pure tests feed domain inputs and effect completions into protocol logic. They
observe state, plans, and requested effects as data.

Use this layer for:

- forest transitions;
- admission;
- fold rules;
- checkpoint planning;
- evidence-to-writer transitions; and
- closed finite error matrices.

These tests must not use object storage, sleeps, or task scheduling.

### Raw adapter contract tests

Adapter tests prove the translation between `object_store` behavior and StromDB
behavior.

Use this layer for:

- `CreateEvidence` classification;
- `StoreError` classification;
- create-if-absent rules;
- bounded reads;
- list order and continuation;
- canonical key checks;
- delete idempotence; and
- backend-specific contract checks.

Run normal contract claims against `InMemory`. Use the scripted store for
adversarial backend results. A real-S3 job can run the portable normal contract
claims.

### Typed-store tests

Typed-store tests prove only the rules added by Seal, WAL, and table stores.

Use this layer for:

- durable key spelling;
- reverse generation order;
- identity checks;
- checked decode;
- typed bounds;
- typed reconciliation; and
- authorized delete coordinates.

Do not repeat raw adapter claims in every typed store.

### Engine scenarios

Engine scenarios prove complete behavior across the public engine boundary and
durable recovery boundaries.

Use this layer for:

- write, publish, and reply order;
- ambiguous WAL creates;
- fencing;
- checkpoint materialization;
- Seal publication;
- partial collection;
- shutdown;
- process loss; and
- reopen.

Each scenario checks invariants after every meaningful transition.

## Scripted object store

Add one `ScriptedObjectStore` to `strom-object-store` behind a `test-support`
feature. It wraps `object_store::memory::InMemory` and implements
`object_store::ObjectStore`.

The wrapper has four responsibilities:

1. Match selected operations.
2. apply a deterministic action;
3. gate an operation when the scenario needs an exact boundary; and
4. record a complete trace.

All operations outside a selected strict scope delegate to `InMemory`.

The engine continues to receive:

```rust
ObjectStoreAdapter::new(scripted.backend())
```

No production engine type knows that the backend is scripted.

### Location

The intended structure is:

```text
crates/strom-object-store/src/
    test_support/
        mod.rs
        scripted.rs
        gate.rs
        trace.rs
```

`strom-object-store` exposes this module only for its own tests and for
dependents that enable `test-support`.

`strom-storage-engine` enables that feature only for development and test
builds.

Do not create a workspace test crate until more than one independent subsystem
needs the same higher-level scenario vocabulary.

## Script vocabulary

The script API uses StromDB key types and the small operation set that the
adapter uses. It does not expose arbitrary callbacks.

The initial operation vocabulary is:

```rust
enum Operation {
    Create,
    Read,
    List,
    Delete,
}
```

An expectation contains:

```rust
struct Expectation {
    operation: Operation,
    target: Target,
    action: Action,
    count: Count,
}
```

`Target` supports:

```rust
enum Target {
    Key(ObjectKey),
    Prefix(ObjectKey),
}
```

Exact keys are preferred. Prefix matching is for list operations and bounded
families of concurrent checkpoint objects.

### Backend failures

Scripts produce external backend failures, not `StoreError` or
`CreateEvidence`.

The minimum failure vocabulary is:

```rust
enum BackendFailure {
    Transport,
    PermissionDenied,
    Unauthenticated,
    NotFound,
    AlreadyExists,
    Precondition,
}
```

The scripted backend converts these values into the corresponding
`object_store::Error`. The production adapter then derives the StromDB result.

This rule is important. A script that returns `CreateEvidence::Unresolved`
would bypass the behavior that the test must prove.

### Create actions

Create actions are:

```rust
enum CreateAction {
    Pass,
    FailBefore(BackendFailure),
    ApplyThenFail(BackendFailure),
}
```

`Pass` delegates to `InMemory` and returns its result.

`FailBefore` returns the selected error without calling `InMemory`. No durable
effect occurs.

`ApplyThenFail` first delegates to `InMemory`. If the inner create succeeds,
the wrapper replaces the successful response with the selected error. The bytes
are durable, but the caller does not receive proof.

`ApplyThenFail` models the central ambiguous-create case:

```text
request reaches storage
    -> immutable object is created
    -> response is lost
    -> adapter returns Unresolved
    -> engine owns reconciliation
```

The script must also support a failed create with no durable effect. The engine
must distinguish these cases only by later evidence.

### Read actions

Read actions are:

```rust
enum ReadAction {
    Pass,
    Fail(BackendFailure),
    FailBody(BackendFailure),
}
```

`Fail` returns an error before a result is available.

`FailBody` returns valid metadata but fails while the body is consumed. This
proves that bounded-read and occupant-comparison code handles streamed body
failure.

Do not add arbitrary body replacement to normal engine scenarios. Plant exact
bytes in `InMemory` when a test needs foreign or malformed durable state.

### List actions

List actions are:

```rust
enum ListAction {
    Pass,
    Fail(BackendFailure),
    ReturnOutOfOrder,
    ReturnForeignKey,
}
```

The invalid actions exist to prove adapter contradiction checks. Engine
scenarios normally use `Pass` or `Fail`.

### Delete actions

Delete actions are:

```rust
enum DeleteAction {
    Pass,
    Fail(BackendFailure),
    ApplyThenFail(BackendFailure),
}
```

`ApplyThenFail` proves that collection remains safe when a delete takes effect
but its response is lost. Collection may leak, but it must not delete outside
its authority or make unpublished state authoritative.

## Matching and order

A single global FIFO queue is not correct for StromDB. Checkpoint child creates
run concurrently with `buffer_unordered`. Valid completion order can differ
between runs.

The script uses strict scopes and phases.

### Strict scopes

A strict scope selects one exact key or one prefix.

Inside the scope:

- every matching operation must have an expectation;
- an extra call is a failure;
- a missing call is a failure; and
- the configured count is exact.

Outside the scope, operations delegate to `InMemory`.

This lets a WAL test describe one WAL coordinate without listing every
bootstrap read.

### Phases

A script is a sequence of phases. Expectations in one phase can match in any
order. The next phase does not begin until all expectations in the current
phase are complete.

For example:

```text
phase 1:
    create table A
    create table B
    create table C

phase 2:
    create Seal

phase 3:
    delete old WAL run
    delete retired table
```

This proves publication order without imposing an order between independent
table creates.

For a single coordinate, separate phases give a strict sequence:

```text
phase 1: create WAL and fail after apply
phase 2: read the same WAL
```

If a call matches a later phase before the current phase is complete, the
harness reports an order failure.

Do not add a general dependency graph until a real scenario cannot use phases.

## Gates

A gate exposes an exact operation boundary to a test.

A gate has these operations:

```rust
gate.wait_until_blocked().await;
gate.release();
```

The scripted store reports arrival before it waits. The test can then inspect
the engine while the operation is blocked.

Use gates to prove:

- a reply is not released before durable completion;
- a view is not published before the commit point;
- shutdown waits for or abandons the correct work;
- writer succession revokes old work;
- child tables exist before Seal publication; and
- collection starts only after publication.

Do not use sleeps, timing margins, repeated `yield_now()`, or scheduler polling
to establish these facts.

Fault selection and scheduling are separate concepts. A gate controls when an
operation continues. An action controls what happens when it continues.

## Trace and verification

The scripted store records every operation:

```rust
struct TraceEntry {
    sequence: u64,
    operation: Operation,
    key: ObjectKey,
    phase: usize,
    action: Action,
    effect_applied: bool,
    result: RecordedResult,
}
```

The exact implementation can use a more compact internal form, but diagnostics
must show:

- expected operation;
- actual operation;
- key or prefix;
- active phase;
- remaining expectations;
- whether a durable effect occurred; and
- the complete relevant trace.

Tests finish with an explicit check:

```rust
script.verify();
```

Do not assert in `Drop`. A second panic during failure hides useful evidence.

An unexpected operation must be recorded immediately. The operation can return
a synthetic backend error so the engine task can stop. `verify()` reports the
script mismatch in domain language.

## Engine scenario harness

Add one scenario harness under the storage-engine integration tests. It drives
the public `Engine` boundary and owns deterministic entropy, the scripted
backend, command helpers, observations, and reopen.

The scenario vocabulary is:

```rust
enum Step {
    Open,
    Command(StreamCommand),
    AwaitGate(GateId),
    ReleaseGate(GateId),
    ExpectReply(ExpectedReply),
    ExpectView(ExpectedView),
    ExpectWriterExit(ExpectedExit),
    Shutdown,
    Crash,
    Reopen,
    VerifyStore,
}
```

`Crash` drops the active engine without graceful shutdown. It does not remove
durable objects or clear the script.

The runner checks general invariants after each meaningful step:

- the published view contains only acknowledged or correctly committed facts;
- a refused command does not change the view;
- partition identity remains stable across reopen;
- observed generations and batch positions do not move backward;
- only a published Seal selects checkpoint tables;
- writer loss makes the old engine unavailable; and
- every operation remains within named bounds.

The first implementation must stay small. Add a scenario step only when at
least two tests need the same action or when the action names a protocol
boundary. Do not hide a clear test behind a fluent language.

## Fixtures

Central test support owns:

- deterministic entropy construction;
- common partition and owner values for planted durable objects;
- `DirectoryKey` construction;
- stream command construction;
- encoded Seal, WAL, and table builders;
- durable object planting;
- published-view observations; and
- trace formatting.

A fixture must contain only facts that matter to the claim.

Do not create one large default world. Prefer small builders that require the
test to name authority, generation, batch, key, and body when those facts
matter.

Use golden byte fixtures only when exact encoding is the claim. Engine scenario
fixtures should use checked production encoders.

## Required coverage

### Adapter

The adapter suite must cover:

- first create returns `Direct`;
- equal occupant returns `DurableMatch`;
- foreign occupant returns `NotOurs`;
- create transport failure before apply returns `Unresolved`;
- create transport failure after apply returns `Unresolved`;
- failed occupant metadata read returns `Unresolved`;
- failed occupant body read returns `Unresolved`;
- permission and authentication failures return `Rejected`;
- ordinary read and list transport failures return `Retryable`;
- absence returns `None`;
- oversized metadata returns `Contradiction`;
- body growth past the bound returns `Contradiction`;
- out-of-order list results return `Contradiction`;
- foreign list keys return `Contradiction`; and
- delete is idempotent.

### Bootstrap

Bootstrap scenarios must cover:

- genesis create wins directly;
- genesis race finds matching durable bytes;
- genesis create is unresolved with matching bytes;
- genesis create is unresolved with absence;
- claim create is unresolved;
- claim is occupied by another owner;
- newest Seal list fails;
- selected Seal read fails;
- WAL suffix list fails;
- WAL read fails;
- replay finds a gap or contradictory identity; and
- every accepted durable prefix reopens to the same state.

### Writer

Writer scenarios must cover:

- direct WAL completion;
- ambiguous WAL create with matching bytes;
- ambiguous WAL create with foreign bytes;
- ambiguous WAL create with absence;
- reconciliation read is retryable;
- reconciliation read is rejected;
- create returns `NotOurs`;
- no authority create is resent;
- success becomes visible before reply;
- refusal does not change the view;
- writer succession fences old work;
- shutdown with an active WAL flight;
- shutdown with an active checkpoint; and
- suffix exhaustion has a bounded outcome.

### Checkpoint

Checkpoint scenarios must cover:

- all child tables are durable before Seal create;
- one child create fails before apply;
- one child create fails after apply;
- table reconciliation finds matching bytes;
- table reconciliation finds foreign bytes;
- table reconciliation finds absence;
- table reconciliation read fails;
- publication claim abandons child work;
- Seal create succeeds directly;
- Seal create is ambiguous with matching bytes;
- Seal create is ambiguous with foreign bytes;
- Seal create is ambiguous with absence;
- Seal create is retryable or rejected;
- contradiction fails closed; and
- reopen ignores complete but unpublished child tables.

### Collection

Collection scenarios must cover:

- all authorized deletes succeed;
- one WAL delete fails before apply;
- one WAL delete fails after apply;
- one table delete fails before apply;
- one table delete fails after apply;
- failure after earlier deletes leaks only;
- collection never deletes tables selected by the successor Seal;
- repeated collection is safe; and
- reopen remains correct after every partial collection prefix.

## Migration plan

The change is one complete test-architecture project.

### Build the scripted store

Implement all four operations, strict scopes, phases, gates, traces, and
verification.

Test the harness itself before engine tests depend on it. The harness tests must
prove:

- pass-through;
- failure before apply;
- failure after apply;
- exact counts;
- missing calls;
- extra calls;
- wrong operations;
- wrong keys;
- early later-phase calls;
- unordered calls within one phase;
- gate arrival and release; and
- useful mismatch diagnostics.

### Expand adapter contracts

Add all adversarial adapter cases. Keep the existing normal contract cases.
Separate portable backend claims from scripted-backend self-tests.

### Add engine scenario support

Create the integration harness and central fixtures. First express existing
public engine tests with it. This proves that the harness does not weaken normal
claims.

### Add the failure matrix

Add bootstrap, writer, checkpoint, and collection scenarios. Every scenario
must include reopen when durable state can differ from memory state.

### Reduce private mocks

Keep direct flight installation only for pure writer transition tests. Rename
such tests so they claim evidence handling, not backend behavior.

Storage claims must use `ScriptedObjectStore`.

### Delete duplication

After coverage moves to the correct layer:

- remove repeated raw adapter claims from Seal and WAL tests;
- remove repeated in-memory setup from scenario bodies;
- remove copied partition, key, and entropy helpers;
- replace polling loops with gates;
- remove helpers that only supported deleted tests; and
- keep typed-store tests focused on typed behavior.

The migration is not complete while both the old and new forms prove the same
claim.

## Review rules

A test change must answer:

1. What semantic claim does this test prove?
2. Which test layer owns that claim?
3. What is the commit or authority boundary?
4. Does the test check the state immediately before and after that boundary?
5. Can a fixed script or closed matrix replace repeated control flow?
6. Does a storage claim pass through `ObjectStoreAdapter`?
7. Does a durable claim cross reopen?
8. Does the failure output show the expected and observed domain facts?
9. Did the change add a duplicate source of truth?
10. Can old test code now be deleted?

## Completion criteria

The test-architecture work is complete when:

- one scripted object store controls all injected storage failures;
- the adapter contract covers every evidence and error class;
- no storage-engine test uses scheduler polling for causality;
- checkpoint table and Seal failure matrices are complete;
- collection is tested after every partial delete prefix;
- ambiguous WAL and Seal creates are tested through the adapter;
- durable scenarios reopen after each meaningful failure point;
- typed stores no longer repeat raw adapter contracts;
- common fixtures have one source;
- script mismatches produce a useful trace; and
- `just ci` passes.

The desired result is not more test code. It is one clear test language for
durable effects, fewer repeated examples, and stronger proof of the storage
protocol.
