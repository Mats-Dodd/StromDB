# Testing architecture

This document defines the test architecture for StromDB.

The main rule is:

> Keep protocol tests pure, inject storage failures at the real adapter seam,
> and prove durable claims across reopen.

[`stromstyle.md`](stromstyle.md) defines the general test style. This document
defines the storage test seam and the work that belongs at each test layer.

## Purpose

Object storage is StromDB's only durable authority. Correctness depends on
behavior that a normal in-memory store cannot produce:

- a create can take effect before its response is lost;
- a request can fail before it reaches storage;
- reconciliation can see matching bytes, foreign bytes, absence, or failure;
- checkpoint work can stop before Seal publication;
- collection can stop after some deletes; and
- a writer can lose authority while an operation is in flight.

Tests need deterministic control of these failures. They do not need a general
mock framework or a second description of the engine's I/O program.

## Principles

### Test stable behavior

Prefer the hardest stable semantic boundary that can prove the claim.

- Test forest, admission, and planning as pure logic.
- Test raw result translation through `ObjectStoreAdapter`.
- Test key spelling and checked decode through typed stores.
- Test durability, authority, and recovery through `Engine`.

Do not mock StromDB modules to reduce test extent. Replace only the external
effect.

### Keep normal behavior implicit

The test store delegates normal operations to `InMemory`. A test names only the
fault, gate, or malformed response that matters to its claim.

Do not require tests to script successful bootstrap reads, normal reopen reads,
or unrelated object operations.

### Keep control flow in the test

Use normal Rust control flow for open, command, shutdown, process loss, and
reopen. Do not add an engine scenario language or generic step interpreter.

Small fixture builders and assertion helpers are useful. They must not hide
operation order, fault placement, publication, authority, or reopen.

### Make invalid faults difficult to express

The fault vocabulary must follow the storage model.

- Only an ambiguous transport failure can occur after a create takes effect.
- Permission and authentication failures occur before an effect.
- An occupied result requires an occupant or an explicit race.
- A body failure requires a body.
- A malformed list requires enough entries to make the fault real.

The test store must reject a configured fault that cannot occur.

### Treat test friction as design feedback

If a focused test needs large setup or internal stubs, first check the
production boundary. A cleaner pure function or effect seam can improve both
the implementation and its tests.

Do not solve every difficult test with a more powerful helper.

## Test layers

### Pure protocol tests

Pure tests feed commands, observations, and effect completions into protocol
logic. They observe state, plans, and requested effects as data.

Use pure tests for:

- forest transitions;
- admission and refusal;
- checkpoint planning;
- evidence-to-writer transitions;
- bounds; and
- closed finite matrices.

These tests must not use storage, sleeps, or task scheduling.

### Adapter contract tests

Adapter tests prove the translation from `object_store` behavior to StromDB
behavior.

Use adapter tests for:

- `CreateEvidence`;
- `StoreError`;
- create-if-absent mode;
- bounded body reads;
- list order and continuation;
- foreign list keys; and
- idempotent delete.

Run normal claims against `InMemory`. Use the fault store for adversarial
results. Portable normal claims can also run against real S3.

### Typed-store tests

Typed-store tests prove only behavior added by Seal, WAL, and table stores:

- key spelling;
- reverse ordinal order;
- body identity;
- checked decode;
- typed bounds;
- reconciliation; and
- authorized deletes.

Do not repeat raw adapter contracts in every typed store.

### Durable engine tests

Durable tests use the public engine boundary. They prove:

- durability before reply;
- visibility at the commit point;
- ambiguous write reconciliation;
- writer fencing;
- checkpoint publication;
- partial collection safety;
- shutdown boundaries; and
- recovery after process loss.

Cross reopen whenever durable state can differ from memory state.

## Minimal fault store

Add one test-only object store in `strom-object-store`. It wraps
`object_store::memory::InMemory` and implements
`object_store::ObjectStore`.

The name can be `FaultStore` or `ScriptedObjectStore`. Its behavior matters more
than its name.

It has four jobs:

1. Delegate normal operations to `InMemory`.
2. Apply selected one-shot faults.
3. Expose exact operation gates.
4. Record targeted call counts and useful failure diagnostics.

It must not implement phases, strict scopes, a general expectation engine, or a
second protocol language.

### Test-only location

The intended structure is:

```text
crates/strom-object-store/src/
    test_support.rs
    test_support/
        fault_store.rs
        gate.rs
```

Expose it behind a `test-support` feature for dependent crate tests. Production
engine types continue to receive:

```rust
ObjectStoreAdapter::new(store.backend())
```

No production engine type knows that the backend can inject faults.

## Fault rules

A fault rule selects one operation and one target:

```rust
struct Rule {
    operation: Operation,
    target: Target,
    effect: Effect,
}

enum Operation {
    Create,
    Read,
    List,
    Delete,
}

enum Target {
    Key(ObjectKey),
    Prefix(ObjectKey),
}
```

Rules are one-shot by default. After a rule runs, later matching calls pass
through normally.

Exact keys are preferred. Prefix rules are for list operations or a bounded
family of checkpoint objects.

### Effects

Keep the effect vocabulary small:

```rust
enum Effect {
    FailBefore(BackendFailure),
    CreateThenLoseResponse,
    DeleteThenLoseResponse,
    FailBody(BackendFailure),
    UnderreportMetadata,
    ReturnOutOfOrder,
    ReturnForeignKey,
}
```

`CreateThenLoseResponse` performs the create in `InMemory`, requires it to
succeed, and then returns an ambiguous transport error.

`DeleteThenLoseResponse` performs the delete, requires the request to succeed,
and then returns an ambiguous transport error.

If a required effect cannot occur, the store records a fault mismatch. It must
not silently consume the rule.

Use operation-specific effects instead of accepting every combination of action
and backend failure. This prevents impossible cases such as bytes becoming
durable before a permission refusal.

### Matching

Unmatched operations always pass through.

If more than one rule can match the same call, configuration must fail before
the test runs. Do not choose one rule by declaration order.

The store must validate operation options that are part of the claim. In
particular, `Operation::Create` requires `PutMode::Create`. An overwrite PUT is
not a create.

## Gates

A gate exposes an operation arrival to the test:

```rust
let gate = Gate::new();
let store = FaultStore::new().gate(create(seal_key), gate.clone());

let checkpoint = tokio::spawn(run_checkpoint(store.backend()));

gate.wait_until_blocked().await;
assert_children_are_durable(&store, &children).await?;
gate.release();
checkpoint.await??;
```

The gate reports arrival before it waits. Release is idempotent.

Gates and faults are independent. A test can gate a normal operation or gate an
operation that will receive a fault after release.

A cancelled gated operation is not a completed injected effect. The internal
operation log must show the cancellation in failure diagnostics. Fault
verification must fail if cancellation prevented a configured fault from
running.

Use gates instead of sleeps, timing margins, `yield_now()` loops, or repeated
polling.

## Targeted observations and diagnostics

Do not expose a complete public operation trace. Most tests should inspect
durable state and engine results, not private call order.

Expose one targeted call-count query for protocol claims such as no-resend and
bounded work:

```rust
store.assert_called_once(Operation::Create, &wal_key)?;
```

Use call counts only when the number of external effects is part of the
contract.

Use durable object observations for other claims:

- gate Seal creation, then check that every child table is present;
- after collection failure, check which authorized objects remain;
- after ambiguous create, reopen and check the recovered state; and
- after cancellation, check that no unpublished object became authoritative.

The store can keep an internal operation log for diagnostics. It is not part of
the normal test API. When verification fails, include the relevant observed
operations so the failure can be understood without a debugger.

## Verification

Verification is narrow:

```rust
store.verify()?;
```

It checks:

- every configured fault ran;
- every configured gate observed its selected call;
- no fault configuration was ambiguous;
- every required after-effect took effect; and
- no injected fault became ineffective.

It does not check every normal call. It does not require a full expected trace.

Do not assert in `Drop`. Return one diagnostic that includes:

- unused or ineffective rules;
- cancelled selected operations;
- operation and target;
- expected effect;
- observed result; and
- the relevant internal operation log.

## Example tests

### Ambiguous WAL create

```rust
let store = FaultStore::new().inject(
    create(wal_key.clone()),
    Effect::CreateThenLoseResponse,
);

let engine = Engine::open(store.backend(), entropy()).await?;
assert_eq!(CreateOutcome::Created, create_stream(&engine, &id).await?);
assert_live(&engine, &id)?;
assert_eq!(CloseOutcome::Shutdown, engine.shutdown().await);

let reopened = Engine::open(store.backend(), entropy()).await?;
assert_live(&reopened, &id)?;
assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);

store.assert_called_once(Operation::Create, &wal_key)?;
store.verify()?;
```

The test names one exceptional event. Reconciliation and reopen use normal
pass-through storage.

### Checkpoint publication order

```rust
let gate = Gate::new();
let store = FaultStore::new().gate(create(seal_key.clone()), gate.clone());

let checkpoint = tokio::spawn(run_checkpoint(store.backend()));
gate.wait_until_blocked().await;

assert_children_are_durable(&store, &children).await?;

gate.release();
checkpoint.await??;
store.verify()?;
```

No phase script repeats the checkpoint implementation.

### Bootstrap read failure

```rust
let store = FaultStore::new().inject(
    read(genesis_key),
    Effect::FailBefore(BackendFailure::Transport),
);

assert!(matches!(
    Engine::open(store.backend(), entropy()).await,
    Err(OpenError::Retryable { .. })
));

let recovered = Engine::open(store.backend(), entropy()).await?;
assert_eq!(CloseOutcome::Shutdown, recovered.shutdown().await);
store.verify()?;
```

The first matching read fails. Reopen passes through without another script
phase.

## Fixtures and assertions

Central test support can own:

- deterministic entropy;
- common partition and owner values;
- stream command builders;
- Seal, WAL, and table builders;
- durable object planting;
- published-view projections; and
- targeted call-count and object-presence helpers.

A fixture contains only facts that matter to the claim. Avoid one large default
world.

Small domain value builders are useful:

```rust
let wal = WalFixture::run()
    .at_batch(batch)
    .with_create(path);
```

A value builder constructs data. It does not run the engine.

Prefer one complete semantic comparison over fragmented assertions. Failure
output must show expected and observed domain facts.

## Case matrices

Use one narrow `check_case` function when setup, execution, and invariant checks
are identical for a closed set of cases.

Each case must:

- have a semantic name;
- contain only facts that differ;
- state its expected domain result; and
- print its name, input, expected result, and actual result on failure.

Split the matrix when cases need different control flow. Do not add optional
fields until the case type becomes an interpreter.

## Required coverage

### Adapter

Cover:

- direct create;
- equal and foreign occupants;
- failure before create;
- create followed by response loss;
- failed occupant metadata and body reads;
- definitive refusal;
- retryable read and list errors;
- oversized metadata and body growth;
- unordered and foreign list results; and
- idempotent delete.

### Bootstrap

Cover:

- genesis creation and races;
- unresolved genesis with match and absence;
- unresolved writer claim;
- Seal list and read failure;
- WAL list and read failure;
- replay gaps and contradictions; and
- reopen from every accepted durable prefix.

### Writer

Cover:

- direct and ambiguous WAL completion;
- matching, foreign, absent, and failed reconciliation;
- no resend of authority creates;
- visibility before reply;
- writer succession;
- active-flight shutdown; and
- bounded suffix exhaustion.

### Checkpoint

Cover:

- child durability before Seal publication;
- child failure before and after apply;
- table reconciliation outcomes;
- cancellation before publication;
- Seal failure before and after apply;
- Seal reconciliation outcomes;
- unpublished child garbage; and
- reopen after each result.

### Collection

Cover:

- complete collection;
- failure before and after each delete;
- leak-only partial progress;
- repeated collection;
- protection of successor tables; and
- reopen after every partial delete prefix.

## Implementation order

1. Add the minimal fault store, gates, targeted call counts, diagnostics, and
   self-tests.
2. Add adversarial adapter contract tests.
3. Add narrow shared fixtures.
4. Add durable bootstrap, writer, checkpoint, and collection tests.
5. Replace scheduler polling with gates.
6. Remove duplicate adapter claims from typed-store tests.
7. Remove copied fixtures and helpers.
8. Delete any test that a stronger stable-boundary test supersedes.

Do not keep a complex temporary expectation framework. Build the minimal shape
directly.

## Review rules

Before accepting a test, answer:

1. What semantic claim does it protect?
2. What plausible incorrect implementation does it reject?
3. Would it survive a structurally different correct implementation?
4. Is it at the purest faithful layer?
5. Does it name only relevant faults and data?
6. Does a storage claim pass through `ObjectStoreAdapter`?
7. Does a durable claim cross reopen?
8. Is call count or order truly part of the contract?
9. Does failure output show the semantic mismatch?
10. Can old test code now be deleted?

## Completion criteria

The work is complete when:

- one minimal fault store owns injected storage failures;
- normal operations need no script;
- all configured faults are one-shot and verified;
- impossible fault combinations cannot be built;
- no test uses scheduler polling for causality;
- authority creates have explicit no-resend checks;
- checkpoint and collection failure matrices are complete;
- durable tests reopen after meaningful failure points;
- typed stores do not repeat raw adapter contracts;
- common fixtures have one source; and
- `just ci` passes.

The desired result is a small test effect seam, direct Rust tests, and strong
proof of durable behavior. The test store must remove complexity from tests,
not move that complexity into a new framework.
