# The protocol crate: the writer becomes a pure machine

Status: implemented.
Scope: new crate `strom-storage-protocol`; encoded candidates move to
`strom-storage-domain`; the object-store PUT body is renamed;
`crates/strom-storage-engine` shrinks to bootstrap, stores, effects, and the
writer interpreter.
One refactor, no behavior change (five named exception groups, decisions 2
and 5–8).
Predecessors: `refactor.md` (the WriterState extract; implemented) and
`refactor2.md` (decided store outcomes; implemented).

## Why

`refactor.md` decision 1 rejected the full sans-I/O machine
(`step(Event) -> Vec<Effect>`) and noted that more event sources would make the
move compelling. Those roadmap sources have not landed yet, so this document
does not claim that literal reversal condition has fired. Instead, the middle
cut exposed stronger present-day reasons to continue: the vocabulary that an
effect boundary needs — decided store outcomes, encoded candidates, effect
tickets — now exists, while the remaining shell/state split creates the three
concrete frictions below. The move is now cheap and useful on current facts.

Three concrete frictions motivate taking it now rather than later.

**The shell's control flow is the hardest code in the engine and has the
weakest tests.** `writer.rs` selects over three optional task borrows through
an eight-arm match on `(flight, checkpoint, ingress_open)`. `terminate` is a
35-line drain with no direct test. Every interleaving claim (a checkpoint
completes during a flight, ingress closes during a checkpoint) is only
reachable today through a real runtime and the fault store.

**Two records of one fact, everywhere.** The shell's `Option<Flight>` and
`Option<CheckpointFlight>` mirror the state's in-flight markers.
`assert_effect_records`, `has_flight`, `active_checkpoint`, and two seam tests
exist only to keep the mirrors aligned. The category should not exist.

**Shared mutable state crossed a seam it should not have.**
`PublicationGate` is an `Arc<AtomicBool + Notify>` handshake between the
writer and the checkpoint task, waived past our own `disallowed_types` lint.
It exists only because checkpoint execution is one opaque async block, so
cancellation needs a side channel instead of being a decision.

The shape that removes all three is the same shape: the protocol is a
synchronous machine, `handle(&mut self, Event) -> WriterStep`, and the shell is
a thin interpreter that executes ordered outputs and feeds effect completions
back as events. Effects and immediate actions are data; the interpreter is the
only I/O. `stromstyle` §5 and §7
already mandate the decide/do split and protocol tests without I/O; this
refactor is the last step of that mandate, plus a compiler-enforced dependency
boundary: a crate that cannot name the adapter or stores. A narrow source lint
and an isolated crate check hold the runtime half of the boundary (decision
10); Cargo feature unification means a dependency feature list alone cannot.

StromDB is an unusually good host for this pattern. The two documented costs
of sans-I/O in production Rust are timer bookkeeping (busy-loop bugs around
`poll_timeout`) and tedious hand-written state for long linear workflows. The
writer correctness protocol uses no clocks and no leases by design, so the
machine has zero timers. Bootstrap is a different, sequential workflow and
remains in the engine; converting it to an event/effect machine is not needed
to remove the writer's present-day complexity and is outside this refactor.

## The design

One new crate, `strom-storage-protocol`, owns the writer machine, the forest
and fold, admission, and the writer-visible decided-outcome vocabulary.
Encoded durable candidates move to `strom-storage-domain`, beside the codecs
and identities that prove them. `strom-storage-engine` keeps bootstrap, the
adapter, the typed stores, checkpoint preparation, collection, and the writer
interpreter. The engine depends on the protocol crate, never the reverse.

The effect boundary sits at the typed-store operations `refactor2.md`
decided, not at raw PUT/GET. The stores are the interpreter's instruction set;
their decided outcomes are the machine's event vocabulary. Raw-byte effects
(`Upload { key, bytes }`) would pull send-once, reconcile, and the evidence
tables back out of the stores — reversing `refactor2.md` for no coverage the
store tests do not already provide.

### Decisions taken (and the tradeoffs behind them)

1. **One method: `handle(&mut self, Event) -> WriterStep`. No `poll_*` split, no
   timers, no `Protocol` trait.** The push/poll API family (quinn-proto,
   str0m) exists to service timers and packet pacing; we have neither. A
   single method also deletes the caller-ordering contract those APIs carry
   in documentation ("call the polls in this order after every mutation").
   After applying an event, `handle` computes its own follow-on work — the
   next WAL run, a due checkpoint, quiescent shutdown — so the shell's
   pre-phase (`take_flight`, `start_checkpoint`, the quiescence check)
   dissolves. Concrete enums, no generic protocol framework: there is one
   machine and no second implementation requiring a trait. The interpreter
   feeds one `Started` event before selecting external observations so a
   checkpoint already due at recovery is issued while ingress is idle; the
   machine accepts that event exactly once.
2. **Any interleaving of issued completions and ingress is legal.** The
   machine asserts protocol rules (an effect was issued, ticket identity,
   batch identity, budget discipline), never the relative arrival order of
   valid outstanding completions. Duplicate, stale, and unissued completions
   remain protocol violations. A completion from an issued checkpoint
   preparation remains valid after cancellation is requested. If
   `CheckpointPrepared` is consumed while its exact marker is `Cancelling`,
   the machine discards the outcome, clears the marker, and never publishes
   it. This covers the race in which preparation physically completes before
   the interpreter's abort takes effect. The `biased` select keeps completed
   effects ahead of ingress as scheduling policy, but a `JoinMap` does not
   preserve today's WAL-before-checkpoint priority when both are ready (named
   exception group one). A related scheduling difference applies to
   collection: a
   collector that has physically finished remains logically occupied until
   its `CollectFinished` event is selected. If an advancing Seal completion is
   selected first, that advance skips collection where today's
   `reap_collector` pre-phase would have cleared the finished handle. Both
   differences are leak-only or scheduling-only; no correctness claim depends
   on either ordering, and tests exercise both.
3. **Budgets are machine markers; a completion is the only release.** One WAL
   flight, one checkpoint, one collector — each an explicit marker inside the
   machine, each freed only by its completion event. Effects are therefore
   produced by the consumption of completions: the loop is self-clocking and
   no queue between machine and interpreter can grow without bound. One event
   emits a bounded effect list; the checkpoint table pipeline stays inside
   one effect (decision 6) partly for this reason. The collector marker moves
   into the machine, deleting the shell's `reap_collector` and `is_none`
   check. Collection preserves today's skip-on-busy behavior: when an
   advancing checkpoint installs while a collection marker exists, the
   checkpoint installs but emits no `Collect`, queues no replacement, and
   leaves the marker untouched. `CollectFinished` clears only its exact
   issued cut; physical task completion without consumption of that event
   does not clear the marker. This is safe because collection is leak-only
   maintenance; skipping can retain unreachable objects but can never delete
   an object still selected by a published Seal. Coalescing from the last
   collected Seal to the newest Seal requires a broader collector protocol
   and remains out of scope. Ingress backpressure is untouched: the bounded
   mpsc and `Overloaded` shedding stay at the edge, as design requirement 7's
   named bounds demand.
4. **Effects carry owned encoded candidates from `strom-storage-domain`. No
   buffer pool, no caller buffers.** The storage-domain crate already owns
   durable identities and the codecs that turn them into bytes, so its
   production candidate constructors now return positive encoded types
   (`EncodedWal`, `EncodedGenesisSeal`, `EncodedAuthoritySeal`,
   `EncodedTable`) instead of making the engine pair a naked `Vec<u8>` with
   facts the codec already knew. `EncodedTable` has distinct Directory and
   Ledger constructors that accept rows and call the matching SST codec; it
   has no constructor that accepts already encoded bytes beside a
   caller-supplied key. Candidates hold
   `bytes::Bytes`: immutable, cheaply cloned, plain data. Send-once correctness
   therefore stays physical — a candidate is never re-encoded, and reconcile
   compares the exact bytes sent. The engine converts those bytes without a
   copy into the object-store adapter's bounded opaque `PutBody`; the existing
   engine const assertions prove each durable-object bound fits
   `PUT_BYTES_MAX`. `FrozenBytes` is renamed `PutBody`: its non-empty,
   adapter-PUT-bound proof is transport vocabulary and stays in
   `strom-object-store`, while it stops leaking into durable candidates. The
   raw `encode_wal`, `encode_seal`, and SST codec functions remain available
   and continue to return bytes for corruption tests, fixtures, and tooling.
   The positive candidate constructors are the production seam: they call
   those codecs once and retain the identity, role proof, and exact bytes.
5. **The writer interpreter is a `JoinMap<EffectKey, WriterEvent>` plus
   ingress.** Every completion-producing effect task resolves to the event it
   produces; correlation data (batch, ticket) rides inside the successful
   event. `tokio_util::task::JoinMap` retains the `EffectKey` even when a task
   panics or is cancelled, so no hand-written task-ID registry exists. The
   interpreter asserts that a key is absent before spawning — `JoinMap`'s
   replace-on-duplicate behavior must never conceal a machine budget bug. The
   eight-arm select collapses to two arms, the `&mut task` borrow dance
   disappears. A cancelled checkpoint-preparation join surfaces as
   `CheckpointPreparationCancelled`; the machine accepts it only for the exact `Cancelling`
   marker. Every task panic, and cancellation of any other effect kind, is an
   interpreter invariant failure and panics with the retained `EffectKey`.
   Runtime task failure is not a storage observation and therefore never
   becomes `WriterExit::Contradiction`.

   The map covers the collector too (named exception group two): today
   `reap_collector` drops a finished collector without inspecting a panic;
   here a collector panic is observed and panics the writer interpreter.
   Collection's ordinary storage failures remain leak-only outcomes handled
   inside the collection effect.

   Immediate actions and completion-producing effects are different output
   types. `PublishView`, `SendReplies`, and
   `CancelCheckpointPreparation` are `WriterAction`s executed inline; WAL
   establishment, checkpoint preparation, Seal publication, and collection
   are `WriterEffect`s spawned in the `JoinMap`. `WriterOutput` preserves one
   total list order across both. This makes publication before reply release
   data without pretending either action is an effect. Cancellation aborts an
   already-issued preparation; the resulting keyed join completion still
   returns as an event and is the only operation that releases its marker.

   `JoinMap::join_next` returns `None` immediately when the map is empty, so
   its biased select arm is enabled only while the map is nonempty. Without
   that guard the nominal two-arm loop would busy-loop and starve ingress. The
   select's `else` arm is an invariant failure: a live machine always has
   either open ingress or an outstanding effect.
6. **Checkpoint execution splits at the publish boundary; `PublicationGate`
   is deleted.** `PrepareCheckpoint` covers planning, encoding, and the
   bounded table pipeline, and returns one `CheckpointPrepared` event. The
   machine answers it: if ingress is open and the ticket is valid, it emits
   `PublishAuthority` with the prepared candidate. If ingress closes while
   the marker is still `Preparing`, the machine moves it to `Cancelling` and
   emits `Action(CancelCheckpointPreparation { ticket })`; the interpreter
   aborts that exact `JoinMap` entry. The keyed preparation then terminates in
   one of two ways. If abort takes effect, its cancelled join becomes
   `CheckpointPreparationCancelled`. If preparation completed before abort took effect, its
   successful join still becomes `CheckpointPrepared`. Both events are legal
   only for the exact `Cancelling` marker, both clear it, and a prepared
   outcome observed there is discarded without publication. No additional
   interpreter cancellation registry is required.

   If `CheckpointPrepared` is consumed before `IngressClosed`, the machine has
   already moved to `Publishing`; ingress closure never cancels an issued Seal
   publication, and the writer waits for its result exactly as it does today.
   Cancellation is therefore a pure machine decision and an explicit
   interpreter action, so the gate, `cancel_before_publish`, the `Notify`
   wait, and the `disallowed_types` waiver are all deleted. The
   `SealPublication` map moves into the machine beside the other exit maps,
   superseding
   `refactor2.md` decision 4's placement (its substance — evidence
   classification lives only in `SealStore` — is unchanged). The machine
   holds the prepared successor and snapshot in its checkpoint marker while
   the publish is in flight, so `CheckpointInstall` stops crossing any seam
   and is deleted. The table pipeline's interior stays async inside the
   engine: it is genuinely concurrent I/O, and one `Prepared` event keeps
   the bounded-effects rule (decision 3). Aborting the outer effect drops its
   table receiver; the blocking producer observes the closed bounded channel
   and stops emitting. Unlike today's gate path, the writer does not await an
   already-running blocking producer after the outer task reports cancelled;
   it may finish winding down after writer shutdown (named exception group
   three). It has no remaining effect consumer and cannot issue storage I/O.
   Already-created tables remain harmless orphan objects, matching today's
   cancellation outcome.
7. **Delete the static `CHECKPOINT_PREPARATIONS` semaphore** (named exception
   group four). Each writer machine already permits exactly one checkpoint
   preparation, and today's `Engine` owns exactly one partition writer. A
   semaphore created per `Engine::open` would therefore constrain nothing; no
   shared multi-partition runtime exists yet to own an injected aggregate
   limit honestly. Delete both `CHECKPOINT_PREPARATIONS` and
   `CHECKPOINT_PREPARATIONS_MAX`. This deliberately removes the current
   process-wide limit across independently opened engines while preserving
   the named one-preparation-per-writer bound. When router/control-plane
   composition introduces a real owner spanning several partition engines,
   it may add an aggregate resource budget at that boundary rather than
   recovering an ambient static here.
8. **A terminal step drops outstanding effect tasks** (named exception group
   five).
   Today `terminate` awaits the in-flight WAL create and checkpoint task and
   applies their completions into state that is then discarded. In the new
   shape, `Shutdown` is reachable only at quiescence (ingress closed, no
   flight, no checkpoint, pending empty), so nothing but the collector can be
   outstanding on the graceful path — matching today's collector abort. On
   fail-stop exits (`Fenced`, `Poisoned`, `Contradiction`) the interpreter
   drops the `JoinMap`, aborting in-flight effects. An aborted create may
   still land; that is exactly a crash, and the protocol already recovers
   every crash by bootstrap. This deletes the drain-and-apply logic rather
   than porting it. A Seal publication that completes successfully after
   ingress closes still installs and publishes its view, but its terminal
   step does not start leak-only collection. Today the shell starts that
   collector immediately before `terminate` aborts it; omitting the
   start-and-immediate-abort is part of this named terminal-step exception.
9. **Bootstrap stays in the engine.** Its sequential phase machine, storage
   calls, entropy capability, fence planning, table merge, replay fold, and
   tests are unchanged except for imports and the final handoff. Its private
   `Ready` typestate now contains a complete `WriterMachine` instead of a
   `WriterState`. The protocol crate exposes construction from the recovered
   facts bootstrap already owns; `Ready` remains constructible only after the
   existing claim, fence, replay, and final-refresh path. This preserves the
   current bootstrap control flow and avoids adding an effect/event vocabulary
   where there is no concurrent orchestration problem to remove.
10. **The crates come first; the compiler and CI hold the boundaries.** Step 1
    first moves encoded candidate construction into `strom-storage-domain`
    and renames the adapter's `FrozenBytes` to `PutBody`; then it moves the
    stable pure protocol vocabulary into `strom-storage-protocol`. The writer
    state moves and collapses into the machine together in step 2 rather than
    exporting its temporary multi-method seam across the crate boundary.
    Every machine step happens inside a crate that cannot accidentally reach
    for the adapter or typed stores. Protocol dependencies: `strom-domain`,
    `strom-storage-domain`, `imbl`, `thiserror`, and `tokio` with the `sync`
    feature only. The workspace-level `tokio` declaration becomes featureless;
    each existing crate names the features it uses. The protocol crate can
    name oneshot senders carried inertly by `Completion`, per `refactor.md`
    decision 3. It has no adapter or `strom-common` dependency, and a source
    lint forbids `tokio::task`, `tokio::time`, and other runtime imports in
    the crate.
    `just ci` adds an isolated `cargo check -p strom-storage-protocol` so the
    workspace's unified Tokio features cannot conceal an accidental runtime
    dependency. The crate boundary enforces absence of storage I/O authority;
    the narrow lint and isolated check enforce absence of runtime execution.
    The sender's immediate `send` method necessarily remains nameable; keeping
    it inert is a reviewed machine contract, not a compiler claim.

    The decided outcomes that cross the writer-machine seam
    (`WalEstablishment`, `SealPublication`, `TypedStoreError`) move to the
    protocol crate: the protocol owns its observation vocabulary and the
    stores implement it. `GenesisEstablishment` stays in the engine with
    bootstrap. `TableEstablishment` also stays in the engine because the
    bounded table pipeline consumes it entirely inside `PrepareCheckpoint`;
    it is not a machine event. `strom-storage-domain` adds `bytes` and owns the
    four encoded candidate types and their constructors, plus `DecodedTable`,
    the positive result of decoding either SST kind. Their byte accessors are
    public because the typed stores must convert, send, and compare them. The
    engine keeps validators, raw observed bytes, and delete proofs; bootstrap
    continues to receive only decoded domain values.
11. **The machine emits outputs; "command", "effect", and "action" each keep
    one meaning.** A command is a client operation in a `CommandEnvelope`,
    nothing else. An effect is completion-producing I/O; an action is an
    immediate interpreter mutation, not a spawned operation with its own
    success event. Cancelling an already-issued effect is an action; the
    existing task's keyed join terminates as either `CheckpointPreparationCancelled` or, if
    abort lost the physical race, its successful completion event.
    `WriterOutput` preserves total execution order. This removes the overload
    between `WriterEvent::Command` and a machine instruction and avoids calling
    publication/reply actions tasks (stromstyle §11: one name, one meaning).

### The interfaces

Sketch, not spelling. The compiler and the first test pick the final shapes.

```rust
// strom-storage-domain

impl EncodedTable {
    pub fn encode_directory(
        partition: PartitionId,
        key: TableKey,
        rows: &[(DirectoryKey, DirectoryEntry)],
    ) -> Result<Self, SstEncodeError>;

    pub fn encode_ledger(
        partition: PartitionId,
        key: TableKey,
        rows: &[(StreamUid, LedgerCell)],
    ) -> Result<Self, SstEncodeError>;
}

// strom-storage-protocol

pub struct WriterStep {
    /// Executed in list order, on terminal steps too.
    outputs: Vec<WriterOutput>,
    /// `Some` stops the interpreter after this step's outputs run.
    exit: Option<WriterExit>,
}

impl WriterStep {
    pub fn into_parts(self) -> (Vec<WriterOutput>, Option<WriterExit>);
}

// The maximum schedule is one publication, one reply group, one promoted WAL,
// and one due checkpoint. An all-idempotent pending barrier can instead make
// two reply groups, but then it emits no promoted WAL. Cancellation emits one
// action and never combines with publication, promotion, or a new checkpoint.
pub const WRITER_OUTPUTS_PER_STEP_MAX: usize = 4;

impl WriterMachine {
    /// Called by engine bootstrap only when constructing its private Ready proof.
    pub fn from_recovery(
        claim: AuthoredClaim,
        seal: Seal,
        base: Forest,
        forest: Forest,
        durable_batch: BatchId,
        next_batch: BatchId,
    ) -> Self;

    pub fn handle(&mut self, event: WriterEvent) -> WriterStep;
    pub fn durable_forest(&self) -> &Forest;
    pub fn partition(&self) -> PartitionId;
}

pub enum WriterEvent {
    Started,
    Command(CommandEnvelope),
    IngressClosed,
    WalEstablished {
        batch: BatchId,
        result: Result<WalEstablishment, TypedStoreError>,
    },
    CheckpointPrepared {
        ticket: CheckpointTicket,
        outcome: PreparationOutcome,
    },
    SealPublished {
        ticket: CheckpointTicket,
        result: Result<SealPublication, TypedStoreError>,
    },
    CollectFinished { cut: BatchId },
    CheckpointPreparationCancelled { ticket: CheckpointTicket },
}

pub enum WriterOutput {
    Effect(WriterEffect),
    Action(WriterAction),
}

pub enum WriterEffect {
    EstablishWal(EncodedWal),
    PrepareCheckpoint(CheckpointInput),
    PublishAuthority {
        ticket: CheckpointTicket,
        candidate: EncodedAuthoritySeal,
    },
    Collect(CollectionInput), // opaque validated cut/source/successor transition
}

impl WriterEffect {
    pub fn key(&self) -> EffectKey;
}

pub enum WriterAction {
    PublishView(Forest),
    SendReplies(Vec<Completion>),
    CancelCheckpointPreparation { ticket: CheckpointTicket },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CheckpointTicket {
    source: SealGeneration,
    cut: BatchId,
    attempt: AttemptId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EffectKey {
    Wal { batch: BatchId },
    CheckpointPreparation { ticket: CheckpointTicket },
    SealPublication { ticket: CheckpointTicket },
    Collection { cut: BatchId },
}

pub enum PreparationOutcome {
    Prepared(PreparedCheckpoint), // successor Seal, snapshot, encoded candidate
    Abandoned,                    // retryable/rejected table, absent after unresolved
    Contradiction { detail: String },
}

enum CheckpointMarker {
    Preparing { ticket: CheckpointTicket },
    Cancelling { ticket: CheckpointTicket },
    Publishing {
        ticket: CheckpointTicket,
        prepared: PreparedCheckpoint,
    },
}
```

Ordering is data: `Action(PublishView)` precedes `Action(SendReplies)` inside
one output list, which turns "publication precedes reply release" from a
shell-only seam claim into a core test plus one interpreter contract test.

The writer interpreter contract, in full: execute each step's outputs in list
order, a terminal step's included; execute actions inline; spawn effects under
their absent `EffectKey`; feed every successful effect completion back as
exactly one event, including a successful checkpoint preparation observed
after abort was requested; execute `CancelCheckpointPreparation` by aborting
its exact present key; map a cancelled checkpoint-preparation join to
`CheckpointPreparationCancelled`; panic with the retained `EffectKey` on every task panic or
cancellation of another effect kind; after a terminal step's outputs, stop and
drop the `JoinMap`. A missing cancellation key is an interpreter invariant
failure. A terminal step never starts a new completion-producing effect — its
outputs are actions only — and the interpreter asserts that contract.
Machine-owned step construction and the interpreter both assert that the
output vector does not exceed `WRITER_OUTPUTS_PER_STEP_MAX`.
Everything else the shell does today is either machine logic or deleted. The
interpreter first executes `machine.handle(WriterEvent::Started)`. The writer
loop's scheduling core is then:

```rust
let event = tokio::select! {
    biased;
    joined = effects.join_next(), if !effects.is_empty() => {
        writer_event(joined.expect("a nonempty JoinMap has a task"))
    }
    command = ingress.recv(), if ingress_open => {
        command.map_or(WriterEvent::IngressClosed, WriterEvent::Command)
    }
    else => panic!("a live writer has ingress or an outstanding effect"),
};
```

### What moves where

Into `strom-storage-protocol` (new crate; workspace metadata and lints
inherited):

| From | What |
| --- | --- |
| `forest.rs`, `forest/` | everything, including behavior tests |
| `writer/state.rs` | `WriterState` (renamed `WriterMachine`), admission, `CommandEnvelope`, `CreateStream`, `AdmissionRefusal`, `Completion`, `CheckpointTicket` |
| `writer.rs` | `WriterExit`; the exit maps in `complete_flight` and `complete_checkpoint`; the quiescence/drain rules from `run` and `terminate` |
| `checkpoint.rs` | `CheckpointInput`; the `SealPublication` → exit map; `PreparedCheckpoint` (as the `Prepared` event payload) |
| `bootstrap.rs` | `AuthoredClaim`, because it is part of the live writer's authority state; `Ready` and the bootstrap phase machine stay in the engine |
| `store.rs`, `store/*` | `TypedStoreError`, `WalEstablishment`, `SealPublication` (classification and I/O stay in the stores) |

Into `strom-storage-domain`:

| From | What |
| --- | --- |
| `store/wal.rs` | `EncodedWal`; its constructor calls the raw WAL codec once and retains the exact bytes |
| `store/seal.rs` | `EncodedGenesisSeal`, `EncodedAuthoritySeal`; separate constructors preserve the genesis/authority role proof while calling the raw Seal codec once |
| `store/table.rs` | `EncodedTable` and `TableRows` renamed `DecodedTable`; kind-specific constructors encode rows directly, the encoded candidate retains its key, `TableRef`, and exact bytes, and the decoded value carries checked rows of either SST kind |

The raw storage-domain codec functions remain public byte-oriented primitives
for corruption tests, fixtures, and tooling. Production effects are built
through the positive encoded candidate constructors, not by pairing raw bytes
with an identity in the protocol or engine.

Within `strom-object-store`, `FrozenBytes` is renamed `PutBody` and gains a
zero-copy `TryFrom<bytes::Bytes>` constructor. This is a vocabulary correction,
not a layer move: the adapter continues to own the proof that an opaque body is
non-empty and within `PUT_BYTES_MAX`.

Stays in `strom-storage-engine`:

- `ObjectStoreAdapter` use, the three typed stores, decode paths,
  `targeted_table_deletes`.
- `bootstrap.rs` keeps its current sequential phase machine, `Entropy`
  capability, identity minting, storage calls, planning, replay, and tests.
  Its private `Ready` contains the `WriterMachine` it constructs from the
  recovered facts after final refresh.
- `TableEstablishment`, `checkpoint/prepare.rs`, and
  `checkpoint/collect.rs`. The
  preparation effect (`PrepareCheckpoint`'s interpreter half) keeps the
  bounded table pipeline.
- `writer.rs` rewritten as the writer interpreter (`JoinMap` + ingress),
  including exact-key checkpoint-preparation cancellation. The engine adds
  `tokio-util` with its `join-map` feature.
- `engine.rs` keeps its public behavior, ingress bound, watch publication
  mechanics, and error maps.

Deleted:

- `PublicationGate`, `cancel_before_publish`, the `disallowed_types` waiver,
  the static `CHECKPOINT_PREPARATIONS`, and
  `CHECKPOINT_PREPARATIONS_MAX`.
- `Flight`, `CheckpointFlight`, `assert_effect_records`, `has_flight`,
  `active_checkpoint` (the accessor), `reap_collector`, `terminate`,
  `WriterEvent` (the shell enum), `FlightCompletion`.
- `AdmissionDecision`, `FlightDecision`, `DurableWal`, `InstalledCheckpoint`,
  `CheckpointInstall`, `CheckpointCompletion` — collapsed into `WriterStep`,
  event payloads, and the machine's checkpoint marker.

Renames:

- `WriterState` → `WriterMachine`; `take_flight`/`take_checkpoint`/
  `record_wal_durable`/`admit` become private transitions behind `handle`.

### Behavior to preserve exactly

- Everything in `refactor2.md`'s list: send-once per WAL and Seal candidate,
  one create and at most one reconcile GET for WAL, no reconcile for Seals,
  advance installs only on `Authored`, bootstrap fence re-planning strictly
  advances, table evidence rules. The stores do not change.
- Admission protocol: `Overloaded` at `WAL_RUN_FACTS_MAX` before admission;
  the suffix gate on the fact path only, a shed arming `retry_checkpoint_at`;
  idempotent settles immediately only without a flight; an all-idempotent
  pending set becomes replies, never a run; `next_batch` advances at take,
  not at commit.
- Checkpoint triggers and ticket validation, unchanged from
  `writer/state.rs` today.
- Collection remains leak-only and skip-on-busy: a checkpoint installed while
  the one collector marker is occupied starts no second collector and queues
  no deferred collection.
- Ingress closure cancels a checkpoint that is still preparing, waits for its
  exact task termination, and never publishes its Seal. Termination may be
  `CheckpointPreparationCancelled`, or `CheckpointPrepared` when physical completion beat
  abort; the latter is discarded under `Cancelling`. Once
  `PublishAuthority` is issued, closure waits for and applies its result
  instead.
- Publication precedes reply release — now an output-order claim plus one
  interpreter test.
- Shutdown exits only at machine-asserted quiescence; every admitted command
  settles first.
- Bootstrap control flow and behavior are unchanged; only encoded-candidate,
  decided-outcome, forest, and final writer-handoff imports move around it.
- The five named exception groups (decisions 2 and 5–8) are the only behavior
  changes.

## Test plan: replace, don't layer

The writer machine's tests are event scripts in the protocol crate: build a
machine, feed a schedule of events, assert on returned steps and state. No
runtime, no adapter, no tasks.

Ports (same claims, new surface):

1. Every `writer/state.rs` scenario ports to a script; the claims are
   unchanged. `admission.rs`-descended refusal axes come along.

Bootstrap unit and engine tests remain in `strom-storage-engine`; only imports
and the final `Ready` handoff change.

New writer scripts (claims the old surface could not state):

2. Ordering as data: the step that records WAL durability lists
   `Action(PublishView)` before `Action(SendReplies)`.
3. Budget discipline: a schedule that tempts a second flight, a second
   checkpoint, or a second collector gets no effect; only the matching
   completion releases each budget. A checkpoint installed while collection
   is active still installs, emits no `Collect`, and does not retroactively
   start the skipped collection when the active one completes. When a
   collector and advancing Seal are both physically complete, selecting the
   Seal first observes the still-occupied logical marker and skips collection;
   selecting `CollectFinished` first releases the marker and permits it.
4. The gate's replacement: ingress closes after `PrepareCheckpoint` is
   issued and before `CheckpointPrepared` is consumed; the machine emits
   `Action(CancelCheckpointPreparation)` and changes the exact marker to
   `Cancelling`. Either `CheckpointPreparationCancelled` for that preparation or
   `CheckpointPrepared` with any outcome for that preparation clears the
   marker. A prepared outcome is discarded and emits no `PublishAuthority`.
   A different preparation identity, or either event under a different
   checkpoint marker, is a protocol violation.
5. Interleavings: checkpoint completion during an active flight and the
   reverse; both ready at once in either completion order; `Fenced`/`Poisoned`
   WAL outcomes while a checkpoint is in each stage (preparing, awaiting
   publication, cancelling).
6. Shutdown schedules: `IngressClosed` at every distinct machine state
   reaches `Shutdown` only at quiescence, with every admitted command
   settled; the schedule that closes ingress during preparation emits exact
   cancellation and reaches `Shutdown` only after the exact preparation
   terminates, covering both `CheckpointPreparationCancelled` and a successful
   `CheckpointPrepared` discarded under `Cancelling`; if
   `CheckpointPrepared` is consumed before closure, its step issues
   publication and a later closure waits for `SealPublished`, then installs
   and publishes an `Authored` successor before `Shutdown` without starting
   collection (decision 8's named exception); fail-stop exits emit terminal
   steps immediately.

Interpreter seam tests (engine crate, real adapter):

7. Outputs execute in list order (observable through publication before
   reply release at the engine seam); actions execute inline rather than as
   new tasks. Checkpoint cancellation aborts only its exact present
   preparation key.
8. Every successful effect completion feeds back exactly once, including a
   preparation that completed before its requested abort took effect. An
   intentionally cancelled preparation retains its exact `EffectKey` and
   becomes `CheckpointPreparationCancelled`. A task panic, or cancellation of any other
   effect kind, panics at the interpreter boundary with the exact key.
   Duplicate keys are rejected before `JoinMap::spawn`; a terminal step's
   actions run, then the loop stops and drops outstanding tasks. The
   completion select arm is disabled while the `JoinMap` is empty, so an idle
   writer blocks on ingress instead of spinning; reaching the select with
   closed ingress and no effect after a nonterminal step is an invariant
   failure.
9. One end-to-end writer claim (admit → durable → reply) proves the interpreter
   wiring, not the protocol.

Moves and deletions:

- `tests/engine/*.rs` stay: they prove durable outcomes through the real
  seam (fencing, reopen, bounded suffix, evidence via fault store), which no
  script replaces. Audit after: delete any engine test that only re-checks a
  pure transition a script now proves and asserts nothing durable
  (expected: few).
- The two split-effect-record seam tests from `refactor.md` are deleted with
  the dual records they guarded.
- The existing shutdown-during-preparation engine test keeps its liveness
  claim: shutdown completes while a table create remains held, before the
  fault-store gate is released. It additionally proves that cancellation
  targeted the preparation, no Seal was published, and reopen recovers from
  WAL. The test no longer depends on a publication side channel.
- Store tests are untouched.

## Order of work

Each completed step compiles and passes `just ci` on its own.

1. Put stable storage and protocol vocabulary in its final homes, then create
   `crates/strom-storage-protocol` (workspace metadata and lint inheritance).
   Make the workspace `tokio` dependency featureless, add required Tokio
   features at each crate, and give the protocol crate only `sync`; add
   `thiserror` explicitly. Add the protocol runtime-import source lint and an
   isolated `cargo check -p strom-storage-protocol` to `just ci` in this step.
   Move encoded WAL, role-proven Seal, encoded table candidates, and
   `DecodedTable` into `strom-storage-domain` with `bytes::Bytes`; retain the
   raw byte-returning codecs and make the positive constructors the production
   seam. In particular, replace `EncodedTable::new(key, bytes)` with
   codec-owning Directory and Ledger constructors. Rename
   `strom-object-store::FrozenBytes` to `PutBody` and add the zero-copy
   conversion. Move `Forest` and `forest/`, the envelope/completion types, the
   machine-visible decided outcomes, `TypedStoreError`, and the exit types into
   the protocol crate. Keep `TableEstablishment` in the engine. Do not export
   the old multi-method `WriterState` across the crate boundary. The engine
   depends on the new crate; the isolated check and `just ci` prove the
   resulting dependency and runtime boundaries.
2. Writer machine: move `WriterState` and admission into the protocol crate
   while collapsing its methods and the shell's exit maps directly into
   `WriterMachine::handle`/`WriterStep` (still WAL-only: checkpoint effects
   keep the current one-shot execute shape behind a temporary
   `CheckpointDone`-style event). Move `AuthoredClaim` and expose only the
   recovery construction needed by bootstrap's private `Ready` handoff; do
   not expose the old transition methods. Rewrite `writer.rs` as the
   `JoinMap<EffectKey, WriterEvent>` interpreter with ordered actions and
   effects, the nonempty-map select guard, and the impossible-state `else`
   assertion. Unexpected task panic or cancellation is an interpreter panic,
   never a machine event or `WriterExit`. Centralize private `WriterStep`
   construction and assert its output and terminal contracts in the machine
   as well as the interpreter. Port the scenario tests to scripts; delete the
   dual protocol records and `terminate`. Bootstrap retains its existing
   control flow and constructs `Ready { machine }` after final refresh.
3. Checkpoint split: `PrepareCheckpoint` + `PublishAuthority`,
   `PreparationOutcome`, and the `Preparing`/`Cancelling`/`Publishing` machine
   marker. Add exact-key `CancelCheckpointPreparation` and
   `CheckpointPreparationCancelled`. Delete `PublicationGate`, both preparation-semaphore
   constants, `CheckpointCompletion`, and `CheckpointInstall`. Add scripts
   2–6, including both exact preparation terminations after cancellation:
   `CheckpointPreparationCancelled`, and `CheckpointPrepared` discarded under `Cancelling`.
   Preserve the shutdown-while-storage-is-held liveness claim.
4. Docs and closure: update `docs/architecture.md` ("published and resident
   state" and the writer program-shape prose) and the crate outline in
   `AGENTS.md`; mark the superseded decisions in
   `refactor.md` (decision 1's middle cut superseded by the concrete frictions
   recorded here) and `refactor2.md`
   (decision 4's map placement); final `just ci`.

## Follow-up, out of scope here

- Rung 2 testing: a seeded random event driver (or `proptest`) generating
  legal schedules and fault outcomes against the writer machine, with its
  asserts as the oracle. Cheap once the machine exists; its own small
  document.
- Bootstrap-machine extraction is not implied by this refactor. A future
  proposal should compare an event/effect machine with simpler sequential
  async control flow against a concrete bootstrap complexity or testing
  problem before changing the current phase machine. Multi-process simulation
  likewise waits for that independently justified seam.
- An aggregate checkpoint-preparation resource budget when a concrete runtime
  first owns multiple partition engines. The present one-preparation-per-writer
  marker remains the only bound until that owner exists.
- Implemented after this refactor: the Forest full-checkpoint-cell operation,
  the one-Seal-owned table walk, and public open-error carriage of
  `Fenced { observed }`. The public close outcome does not expose its internal
  WAL batch.
- `collect.rs` interior simplification, if any, once it is the last complex
  writer-side effect implementation left.
- Collection catch-up/coalescing: track the last collected Seal and collect
  safely toward the newest published Seal so a checkpoint skipped while the
  collector is busy does not leave permanently eligible objects. This is a
  broader collection protocol, not a queue added to the writer machine.
