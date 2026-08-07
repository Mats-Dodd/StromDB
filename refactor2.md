# Checkpoint locality and decided store outcomes

Status: implemented.
Scope: `crates/strom-storage-engine`. One refactor, no behavior change (one
benign interleave change, decision 3).
Predecessor: `refactor.md` (the WriterState extract; implemented).

## Why

Two frictions remain after the WriterState extract.

**The checkpoint concept has no home module.** One logical operation —
prepare and publish a checkpoint — is divided across five modules:

| Role | Where |
| --- | --- |
| decide "due", build input + ticket | `writer/state.rs` `take_checkpoint` |
| pure Delta/Full plan and encode | `checkpoint/prepare.rs` `prepare_checkpoint` |
| I/O pipeline and publication gate | `checkpoint.rs` `execute_checkpoint` |
| classify Seal evidence, install | `writer.rs` `complete_checkpoint` |
| collect the covered WAL | `collection.rs` `collect_advance` |

To trace one checkpoint bug a maintainer holds all five open. Two different
types share the name `CheckpointPlan` (`writer/state.rs` and `prepare.rs`).
The deletion test says the logic earns its keep — deleting any piece makes it
reappear at its callers — but the concept has no locality.

**The `CreateEvidence` tables repeat across seams.** Five normal call sites
each hold a copy of the Direct/DurableMatch/NotOurs/Unresolved
classification, with rules that differ by durable kind, not by caller.
Genesis is a sixth, special classifier:

| Call site | Rule |
| --- | --- |
| WAL run (`writer.rs` `complete_flight`) | `Direct\|DurableMatch` durable; `Unresolved` gets one exact reconcile read |
| WAL fence (`bootstrap.rs` `PlaceFence`) | same reconcile protocol; `NotOurs` re-plans the candidate |
| claim Seal (`bootstrap.rs` `PublishClaim`) | only `Direct` grants the capability; no reconcile |
| advancing Seal (`writer.rs` `complete_checkpoint`) | same as claim |
| checkpoint table (`checkpoint.rs` `establish_table`) | create-or-verify; `Unresolved` calls `reconcile_table` |
| genesis Seal (`bootstrap.rs` `provision_genesis`) | `Direct\|DurableMatch` established; `NotOurs` loses the ordinary race |

The typed stores return raw evidence, so each caller must know the correct
table, and `CheckpointOutcome::Seal` leaks
`Result<CreateEvidence, TypedStoreError>` across the checkpoint↔writer seam.
The classification per durable kind is protocol, and the store owning that
durable kind is its one correct home.

## The design

Each typed store classifies the evidence for its own durable kind and returns
a small decided outcome. The checkpoint module then owns prepare, establish,
publish, and classify behind one completion type. The writer shell only
spawns tasks, installs results, or exits.

### Decisions taken (and the tradeoffs behind them)

1. **Stores return decided outcomes, not raw evidence.** The evidence table
   for one durable kind appears exactly once, beside the send-once and
   reconcile discipline it depends on. Callers keep only a small map from
   the decided outcome to their own exit type (`WriterExit` vs
   `BootstrapExit`). `establish_wal` and `publish_authority` each have two
   callers, so those seams are real, not hypothetical. `establish_table` has
   one caller and earns its move on locality alone: the table evidence rule
   belongs beside the store that owns the table durable kind.
2. **Genesis gets its own decided outcome.** Its rule is unique by design: a
   lost genesis race is normal and the loser adopts the winner. It therefore
   does not use the authority classifier. `SealStore::establish_genesis`
   returns a genesis-specific outcome, while `publish_authority` serves claim
   and advance. The methods accept different candidate types:
   `EncodedGenesisSeal` and `EncodedAuthoritySeal`. Their constructors enforce
   genesis versus non-genesis coordinates before encoding. The raw create
   operation becomes private. A caller therefore cannot select the wrong
   evidence rule by passing a general `EncodedSeal`.
3. **The reconcile read moves into `WalStore`.** The spawn seam changes: the
   writer's flight task runs create plus reconcile, not create alone. The
   store is the owner of the send-once discipline and of the exact-bytes
   compare, so the frozen candidate never crosses back to the shell for a
   comparison. `EncodedWal` retains the candidate's `PartitionId` as well as
   its batch and bytes, so the reconcile decode cannot receive a partition
   that disagrees with the candidate. The shell's `Flight` is exactly
   `{ batch, task: JoinHandle<Result<WalEstablishment, TypedStoreError>> }`. The
   alternative — classification in the store, reconcile in the shell —
   keeps the reconcile observable on the writer loop for a future timeout or
   metric point, but splits one protocol across the seam again. Rejected.
   The method performs exactly one create and at most one exact GET. It does
   not retry, list, select another coordinate, or change a bootstrap phase.
   Bootstrap keeps re-list, candidate advance, and phase transitions.
   One interleave change follows: today `complete_flight` awaits the
   reconcile read inline, so the loop blocks during it; afterwards the flight
   task performs the reconcile and the loop can admit commands in that time.
   The protocol is unchanged — the state flight is still active, so no second
   run can start. This scheduling difference is allowed, not a contract; tests
   assert the single-flight rule, not whether admission runs during the GET.
4. **`execute_checkpoint` returns a decided completion in the same change that
   introduces `publish_authority`.**
   `PreparedCheckpoint` and raw Seal evidence never cross the checkpoint
   seam. There is no intermediate compiling state in which
   `SealPublication` crosses into `writer.rs` for another classification.
   `CheckpointOutcome::Contradiction { cut, .. }` loses its `cut`
   field — the ticket already names it, and `writer.rs` already asserts the
   two are equal.
5. **`collection.rs` moves to `checkpoint/collect.rs`.** Collection only
   runs after an advancing Seal publication; it is part of the checkpoint
   concept. The directory then reads `checkpoint.rs` + `checkpoint/prepare.rs`
   + `checkpoint/collect.rs` and the whole lifecycle is one place. The shell
   keeps the collector `JoinHandle` — task ownership is shell work.
   `targeted_table_deletes` and its private `seal_tables` walk stay in
   `store/table.rs`: the walk is the only constructor of
   `AuthorizedTableDelete`, and that private construction is the proof that a
   collector deletes only the tables the Seal diff names. Moving the walk out
   of the store would need a public constructor and dissolve the proof. The
   one-Seal-owned-walk follow-up gives the walk its final home later.
6. **`WriterState` keeps only "when is a checkpoint due".**
   `take_checkpoint`, the ticket, and `install_checkpoint` do not move. The
   division after this refactor: the state decides, the checkpoint module
   does the whole operation, the shell spawns and installs.

### The interfaces

Sketch, not spelling. The compiler and the first test pick the final shapes.

```rust
impl TableStore {
    /// Create-or-verify one fresh content table. Content presence grants
    /// no authority, so an Unresolved create reconciles internally.
    async fn establish_table(&self, candidate: &EncodedTable) -> TableEstablishment;
}

enum TableEstablishment {
    Established,
    Abandoned,                          // retryable, rejected, or absent
    Contradiction { detail: String },
}

impl WalStore {
    /// Send the candidate exactly once. After Unresolved, reconcile with
    /// one bounded exact GET against the frozen candidate bytes. The candidate
    /// retains the partition needed for checked decode.
    async fn establish_wal(&self, candidate: &EncodedWal)
        -> Result<WalEstablishment, TypedStoreError>;
}

enum WalEstablishment {
    Durable,           // Direct | DurableMatch | Unresolved then exact match
    Occupied,          // NotOurs | Unresolved then different bytes
    UnresolvedAbsent,  // Unresolved, then absent; the create may still occur
}

impl SealStore {
    /// Establish canonical genesis. A matching winner is safe to adopt.
    /// Send-once, no reconcile.
    async fn establish_genesis(&self, candidate: &EncodedGenesisSeal)
        -> Result<GenesisEstablishment, TypedStoreError>;

    /// Publish an authority-bearing candidate (claim or advance).
    /// Send-once, no reconcile: only Direct grants a capability.
    async fn publish_authority(&self, candidate: &EncodedAuthoritySeal)
        -> Result<SealPublication, TypedStoreError>;
}

enum GenesisEstablishment {
    Established,  // Direct | DurableMatch
    LostRace,     // NotOurs: rediscover and adopt the winner
    Unresolved,   // retryable; never serve
}

enum SealPublication {
    Authored,     // Direct: the caller holds the capability
    NoAuthority,  // DurableMatch | NotOurs: no direct author proof
    Unresolved,   // never serve; the caller stops
}

// checkpoint.rs
async fn execute_checkpoint(
    adapter: ObjectStoreAdapter,
    input: CheckpointInput,
    publication: PublicationGate,
) -> CheckpointCompletion;

enum CheckpointCompletion {
    Installed(CheckpointInstall),
    Abandoned,
    Fenced,
    Poisoned { detail: String },
    Contradiction { detail: String },
}
```

The error shapes are deliberately asymmetric. `TableEstablishment` absorbs
store errors because its one caller folds `Retryable` and `Rejected` into
`Abandoned`. `establish_wal`, `establish_genesis`, and `publish_authority`
return `Result<_, TypedStoreError>` because their callers map errors at their
own seams. The writer maps WAL `Retryable | Rejected` to `Poisoned`;
checkpoint execution maps the same Seal failures to `Poisoned`; bootstrap maps
`Retryable` to `Retryable` and `Rejected` to `Contradiction` through
`map_typed_store_error`. Do not "align" the shapes.

Normal caller maps after the change:

| Decided outcome | Writer | Bootstrap |
| --- | --- | --- |
| `WalEstablishment::Durable` | `record_wal_durable` | fence placed; enter `Replay` |
| `WalEstablishment::Occupied` | `Fenced` | re-list, next fence candidate |
| `WalEstablishment::UnresolvedAbsent` | `Poisoned` | `Retryable` |
| `SealPublication::Authored` | `install_checkpoint` | `AuthoredClaim` |
| `SealPublication::NoAuthority` | `Fenced` | `Fenced` |
| `SealPublication::Unresolved` | `Poisoned` | `Retryable` (never serve) |

Superseded placement (`refactor3.md` decision 6): the checkpoint effect now
ends at `CheckpointPrepared`; the pure writer machine issues
`PublishAuthority` and maps `SealPublication` beside the WAL outcome maps. The
typed `SealStore` still exclusively owns evidence classification and the
send-once publication operation, so this decision's correctness substance is
unchanged.

Genesis has its own map: `Established` continues provisioning, `LostRace`
rediscovers and adopts the winner, and `Unresolved` returns `Retryable`.

The checkpoint map is exhaustive:

| Checkpoint observation | `CheckpointCompletion` | Writer action |
| --- | --- | --- |
| all tables established, Seal `Authored` | `Installed(CheckpointInstall)` | install, publish the view, start collection |
| preparation cancelled, table `Abandoned`, or publication gate lost | `Abandoned` | return the exact ticket through `abandon_checkpoint` |
| Seal `NoAuthority` | `Fenced` | abandon the ticket, exit `Fenced` at `ticket.cut` |
| Seal `Unresolved` | `Poisoned { detail }` | abandon the ticket, exit `Poisoned` at `ticket.cut` |
| Seal store `Retryable | Rejected` | `Poisoned { detail }` | abandon the ticket, exit `Poisoned` at `ticket.cut` |
| table or Seal store contradiction, preparation failure | `Contradiction { detail }` | abandon the ticket, exit `Contradiction` at `ticket.cut` |
| checkpoint task join failure | no completion value | abandon the ticket, exit `Contradiction` at `ticket.cut` |

### What moves where

Into `store/table.rs`:

- `establish_table` and its classification from `checkpoint.rs`, as
  `TableStore::establish_table`. `create_table` and `reconcile_table`
  become private. `EstablishTableError` dissolves into `TableEstablishment`.

Into `store/wal.rs`:

- the WAL evidence table and the one-shot reconcile read from
  `writer.rs` `complete_flight` and `bootstrap.rs` `PlaceFence`, as
  `WalStore::establish_wal`. Only the store then compares candidate bytes:
  `EncodedWal::as_slice` and `ObservedWal::as_slice` lose their external
  callers and become store-private. `EncodedWal` retains its `PartitionId`,
  and `establish_wal` takes no separate partition argument. `create_wal`
  becomes private: after this change `establish_wal` is its only production
  caller.

Into `store/seal.rs`:

- the genesis classification from `bootstrap.rs` `provision_genesis`, as
  `SealStore::establish_genesis`;
- the shared claim/advance classification from `bootstrap.rs`
  `PublishClaim` and `writer.rs` `complete_checkpoint`, as
  `SealStore::publish_authority`;
- role-specific `EncodedGenesisSeal` and `EncodedAuthoritySeal` candidates,
  whose constructors prevent the two evidence rules from being mixed;
- the raw create operation becomes private.

Into `checkpoint.rs`:

- mapping the decided `SealPublication` for the advance path into
  `CheckpointCompletion`. Evidence classification occurs only in
  `SealStore`. `PreparedCheckpoint` becomes private to the module.
  `prepare.rs`'s `assemble_checkpoint_seal` encodes the successor as an
  `EncodedAuthoritySeal` — the one type `publish_authority` accepts.

Into `checkpoint/collect.rs`:

- all production code and tests from `collection.rs`; delete the old file.
  `targeted_table_deletes` and its private `seal_tables` walk stay in
  `store/table.rs` beside `AuthorizedTableDelete` (decision 5). Checkpoint
  preparation keeps its own Seal walk for now; unifying those two walks
  remains a follow-up.

Stays where it is:

- `writer/state.rs` untouched except imports: `take_checkpoint`,
  `CheckpointTicket`, `install_checkpoint`, `abandon_checkpoint`.
- The shell's select loop, `Flight` (exactly `{ batch, task }`, with the task
  returning `Result<WalEstablishment, TypedStoreError>` and owning the
  `EncodedWal` for its full lifetime),
  `CheckpointFlight`, `assert_effect_records`, the collector handle.
- `PublicationGate` and the bounded table pipeline inside `checkpoint.rs`.

Renames:

- `prepare.rs`'s private `CheckpointPlan { Delta, Full }` becomes
  `PlanShape`. The state's `CheckpointPlan { input, ticket }` keeps its
  name; it is the one that crosses a seam.

### Behavior to preserve exactly

- Each WAL and Seal candidate is sent once. `establish_wal` performs exactly
  one create and at most one reconcile GET;
  `establish_genesis` and `publish_authority` each perform exactly one create
  and no reconcile.
- The advance path installs only on `Authored`. `DurableMatch` on an
  advancing Seal is fenced, never adopted.
- Bootstrap fence `NotOurs` re-plans the candidate and requires the next
  candidate to strictly advance; the non-advancing list stays a
  `Contradiction`.
- Content tables (`establish_table`): `Direct | DurableMatch` established;
  `NotOurs` contradiction; `Unresolved` reconciles; absent-after-unresolved
  abandons.
- The writer discards its state flight (`discard_wal_flight`) on every
  non-durable WAL outcome before it exits.
- Checkpoint abandonment still returns the exact ticket
  (`abandon_checkpoint` validation unchanged).
- Publication precedes reply release (unchanged shell ordering).

## Test plan: replace, don't layer

The interface is the test surface. The new store methods get tests in their
store files, against the in-memory adapter and the `FaultStore` — the two
real adapters at that seam.

New tests:

1. `establish_table`: each evidence class; `Unresolved` then match, foreign,
   and absent; retryable and rejected map to `Abandoned`; store contradiction
   passes through.
2. `establish_wal`: `Direct` and `DurableMatch` are `Durable`; `NotOurs` is
   `Occupied`; `Unresolved` then exact match is `Durable`; then different
   bytes is `Occupied`; then absent is `UnresolvedAbsent`; exactly one create
   and at most one GET are issued (the send-once claim, observed through the
   fault store's operation counts at this store-internal seam, not through
   the engine).
3. `establish_genesis`: `Direct | DurableMatch` is `Established`; `NotOurs`
   is `LostRace`; `Unresolved` is `Unresolved`; exactly one create and no GET
   are issued.
4. `publish_authority`: `Direct` is `Authored`; `DurableMatch` and `NotOurs`
   are `NoAuthority`; `Unresolved` is `Unresolved`; exactly one create and no
   GET are issued.
5. One checkpoint-module claim: `execute_checkpoint` returns `Fenced` when a
   competitor occupies the successor coordinate, and `Installed` carries the
   exact successor Seal. Exhaustively cover the checkpoint map above,
   including store error details and preparation contradiction.
6. Bootstrap engine claims: ambiguous fence then matching bytes enters
   `Replay`; ambiguous fence then different bytes re-lists; ambiguous fence
   then absence is `Retryable`; `NotOurs` re-lists and strictly advances the
   candidate; claim `DurableMatch` is fenced; claim `Unresolved` issues no
   GET.
7. Caller error maps: writer maps WAL `Retryable | Rejected` to `Poisoned`;
   checkpoint execution maps Seal `Retryable | Rejected` to
   `CheckpointCompletion::Poisoned`; bootstrap maps `Retryable` to `Retryable`
   and `Rejected` to `Contradiction`.

Moves and deletions:

- Delete the evidence branches in `writer.rs` `complete_flight` and
  `complete_checkpoint`, `bootstrap.rs` `PublishClaim`, `PlaceFence`, and
  `provision_genesis`, and `checkpoint.rs`
  `establish_table`/`EstablishTableError`.
- Audit `tests/engine/writer.rs` and `tests/engine/checkpoint.rs`: keep every
  test that proves a durable outcome through the real seam (fencing, reopen,
  publication before reply, the bounded suffix). Delete a test only when it
  re-checks one evidence mapping that a store test now proves and asserts
  nothing durable (expected: none or few).
- Keep the two seam claims from `refactor.md`: publication before reply
  release, and shell/state effect-record agreement.
- Keep the single-flight scheduling claim. Do not add a test that depends on
  whether admission runs during the reconcile GET.
- When raw WAL and Seal create methods become private, convert bootstrap and
  collection test setup that plants durable objects to direct in-memory or
  `FaultStore` adapter setup in the same step. Do not retain production
  visibility only for fixtures.

## Order of work

Each completed step compiles and passes `just ci` on its own.

1. Rename `prepare.rs`'s `CheckpointPlan` to `PlanShape`.
2. `TableStore::establish_table`: move the classification from
   `checkpoint.rs`; make `create_table`/`reconcile_table` private; write the
   store tests.
3. `WalStore::establish_wal`: rewrite `writer.rs` `complete_flight` and the
   `bootstrap.rs` fence arm against it; shrink `Flight` to `{ batch, task }`;
   make `create_wal` private and convert the `collection.rs` and
   `bootstrap.rs` test fixtures that plant WAL objects to direct adapter
   setup in the same step; write the store tests.
4. `SealStore::establish_genesis` and `publish_authority`, plus
   `CheckpointCompletion`, as one change: add the role-specific encoded Seal
   candidates; make raw create private and convert the `collection.rs` test
   fixtures that plant Seals to direct adapter setup in the same step;
   rewrite `provision_genesis` and `PublishClaim`; map `SealPublication`
   inside `execute_checkpoint`; shrink `complete_checkpoint` to the
   completion match; write the store and checkpoint tests. No Seal evidence
   or store outcome crosses into the writer shell.
5. Move `collection.rs` and its tests to `checkpoint/collect.rs`; add the
   Bootstrap and caller-map engine claims; audit existing engine tests.
6. Update `docs/architecture.md` "storage adapter APIs" with the decided
   store methods and private raw operations; mark `refactor.md` as
   implemented and note that its decision to leave evidence classification
   in the shell was superseded by this refactor; run final `just ci`.

## Follow-up, out of scope here

- `prepare.rs`'s Full path still reads `directory_rows()`/`ledger_rows()`
  across the forest seam; give `Forest` one "emit all rows as checkpoint
  cells" operation and make the row accessors test-only.
- `engine.rs` drops `Fenced { observed }` and exit `batch` when mapping to
  public errors; decide whether the public interface should carry them.
- `store/table.rs`'s table-delete planning (`targeted_table_deletes`) and
  `prepare.rs`'s `seal_tables` walk the same Seal shape twice; one Seal-owned
  walk would delete the duplicate and give the delete-proof walk its final
  home (decision 5).
