# WriterState: extract the writer's pure protocol core

Status: implemented. Decision 2 was superseded by `refactor2.md`: durable
evidence classification now lives in each typed store, and the writer shell
receives decided outcomes.
Scope: `crates/strom-storage-engine`. One refactor, no behavior change (one
deliberate exception, decision 6).

## Why

The writer task (`src/writer.rs`) mixes two jobs. One job is the admission-to-
durability protocol: admit commands, queue facts, fold committed runs, release
replies, trigger checkpoints. The other job is I/O orchestration: spawn WAL
creates, classify create evidence, run the select loop, drain on shutdown.

The protocol job has no interface today, and therefore no test surface:

- The writer holds three forests — `base`, `admitted`, `durable`
  (`writer.rs`) — and the rules that connect them are implicit. `admitted`
  advances in `accept_fact`, `durable` in `commit`, `base` only in
  `complete_checkpoint`. The invariant "no flight in flight means admitted
  equals durable" lives in a single assert inside `consider`.
- `admission.rs` extracts pure helpers (`admit_create`, `decide_suffix_room`,
  `decide_successor_uid`) and unit-tests them, but the bugs hide in how the
  writer calls them: the suffix gate runs only on the fact path, idempotent
  replies gate on flight presence, a shed re-arms the checkpoint retry. None
  of that call-site protocol is testable without a real object store and a
  running task. Pure functions without locality.
- The bootstrap-to-writer seam leaks. `Ready::into_writer_seed`
  (`bootstrap.rs`) hands over six raw fields; the writer mirrors them as
  fields and re-derives their meaning. Two modules must agree on what `base`
  versus `forest` means, and nothing in the types says a `WriterSeed` only
  exists after claim, fence, replay, and refresh.

The deletion test says this logic cannot be deleted — it would reappear in
every writer branch. That is the signal to concentrate it into one deep
module: a lot of behaviour behind a small interface, placed at a clean seam,
testable through that interface.

`docs/stromstyle.md` already demands this shape:

- §5 "Separate deciding from doing. The decision half of an operation MUST be
  pure: a function from state and input to an outcome, performing no I/O."
- §7 "Protocol tests without I/O": tests feed commands, observations, and
  effect completions into the protocol; they observe state, plans, and
  requested effects as data.

## The design

Extract a pure `WriterState` module. The writer task keeps I/O only.

### Decisions taken (and the tradeoffs behind them)

1. **Middle cut.** `WriterState` owns the forests, the pending queue, the
   in-flight marker, and the checkpoint accounting, behind ~6 methods. We
   rejected the shallow cut (forests + admission gates only) because it
   leaves the call-site protocol — the bug habitat — in the shell. We
   rejected the full sans-I/O machine (`step(Event) -> Vec<Effect>`) because
   the writer has only three event sources today and the enum funnel costs
   legibility. If the writer grows more event sources (reads, subscriptions,
   timers), collapse the methods into `step` then; the middle cut keeps that
   move cheap.
2. **Superseded: evidence classification stays in the shell.** `complete_flight`'s
   Direct/DurableMatch/NotOurs/Unresolved table performs a reconcile read
   (I/O), never touches the forests, and has strong integration coverage
   through the fault store. `refactor2.md` subsequently moved the send-once
   classification and exact reconciliation into the typed stores so each
   durable kind owns its complete evidence protocol.
3. **Concrete `Completion`, no generics.** The reply tokens (`Completion`,
   pairing the decided outcome with its oneshot sender) move into the state
   module as inert data. The state never calls `send`; it returns settled
   replies as data and the shell sends them. Tests build oneshot channels (no
   runtime needed) and read with `try_recv`. A generic token parameter would
   remove one inert tokio import and add a type parameter to every signature.
   Non-negotiable property to preserve: the outcome and its sender are paired
   at admission time, so a mismatched reply stays unrepresentable.
   `CommandEnvelope` moves with `Completion`, and `admit` takes the complete
   envelope as one value. It never accepts a command and sender separately.
4. **`Ready { state: WriterState }`.** Bootstrap constructs the initial state;
   `WriterSeed` and `into_writer_seed` are deleted. `partition` is already
   owned by `WriterState`, so `Ready` does not store a second copy. Ownership
   is the proof (stromstyle §2): a `Ready` value contains a state produced
   after the claim-fence-replay-refresh path. This does not claim that no
   other crate-private code can construct a `WriterState`; the proof is the
   private construction of `Ready`. Replay itself keeps folding into a plain
   `Forest` — replay is a different protocol with its own owner rules; the
   finished forest seeds the state.
5. **Name: `WriterState`, in `writer/state.rs`.** Follows the existing
   `forest.rs`+`forest/`, `checkpoint.rs`+`checkpoint/` layout. `admission.rs`
   is deleted; its public names move.
6. **Checkpoint decision and planning are one operation.**
   `plan_checkpoint` returns `Option<CheckpointPlan>`: `None` means no
   checkpoint is due, and `Some` both builds the input and records the active
   attempt. There is no separate `should_checkpoint` check that can go stale
   or be omitted. Planning has no fallible result. Its only failure today is
   exhaustion of the `u64` attempt counter, which is process-local state — an
   `expect` per stromstyle §3, not a `Result`. This deletes one `WriterExit`
   mapping in the shell. It is the one deliberate behavior change: a counter
   overflow becomes a panic instead of a `Contradiction` exit. It is
   unreachable in practice (2^64 attempts), so it is not reachable through
   the public lifecycle in a practical execution.
7. **Effect tickets join the two owners.** A WAL run and a checkpoint each
   leave one pure record in the state and one task record in the shell. The
   checkpoint plan returns a `CheckpointTicket { source, cut, attempt }`.
   The shell retains it in `CheckpointFlight` and must return it on install or
   abandonment. The state stores the same ticket as `ActiveCheckpoint` and
   validates all three fields before it changes state. This also gives an
   `Abandoned` outcome the identity that `CheckpointOutcome::Abandoned` does
   not carry.
   The asymmetry with WAL is deliberate: a run's `BatchId` is already a full,
   unique identity, while a checkpoint attempt is not identified by its cut
   alone — do not "align" them later.
   The ticket also makes the shell's `WriterExit` mapping self-sufficient:
   `ticket.cut` supplies every batch the checkpoint arms name, so the shell
   never reads `durable_batch`, the `prepared.cut().unwrap_or(durable_batch)`
   fallback disappears, and `PreparedCheckpoint::cut()` and `advancing_batch`
   in `checkpoint.rs` are deleted as dead. A checkpoint join-error exit then
   names the plan-time cut instead of the current durable head — a
   diagnostic-text change only, and the more precise identity for the failed
   attempt.
8. **State-transition verbs describe records, not effects.** The shell
   executes and commits I/O. After it classifies durable WAL evidence, the
   pure state method is `record_wal_durable`, not `commit`. Checkpoint install
   remains named `install_checkpoint`: it installs an already-published
   checkpoint into local state and performs no I/O.
9. **The state encodes the WAL run.** `take_flight` returns an `EncodedWal`,
   not a `WalObject`. Encoding is pure, and its byte-bound `expect` ("the
   fact-count and field bounds prove every pending RUN fits") is proven by
   the fact limits the state owns; handing a `WalObject` across the seam
   would split one proof across two modules. The cost is one `EncodedWal`
   import from `store` into the state module — inert data, like
   `Completion`'s oneshot senders (decision 3).

### What moves where

Into `writer/state.rs` (pure — no I/O, no tasks, no adapter):

| From | What |
| --- | --- |
| `writer.rs` fields | `partition`, `claim`, `seal`, `base`, `admitted`, `durable`, `durable_batch`, `next_batch`, `pending`, `spare` |
| `writer.rs` fields | `checkpoint_attempt`, `last_checkpoint_attempted_cut`, `retry_checkpoint_at`, and `Option<ActiveCheckpoint>` storing the active `CheckpointTicket` |
| `writer.rs` | new in-flight marker, `Option<InFlight { batch, commands: Vec<PendingCommand> }>`: `record_wal_durable` releases the replies in admission order, so the state must own the flight's commands, not just its `BatchId`. The shell keeps only the encoded bytes and the `JoinHandle` |
| `writer.rs` | `consider`, `accept_fact`, `accept_idempotent`, `promote_pending` (as `take_flight`), `record_wal_durable`, checkpoint decision and input construction from `should_checkpoint`/`start_checkpoint`, the install half of `complete_checkpoint` |
| `writer.rs` | `CommandEnvelope`, `PendingCommand`, `Completion`, `replay_batch`/`suffix_span`/`pending_facts` helpers, and the `EncodedWal::new` call with its byte-bound `expect` (decision 9) |
| `admission.rs` | everything: `CreateStream`, `AdmissionRefusal`, `AdmittedCommand`, `admit_create`, `admit_close`, `admit_delete`, `decide_suffix_room`, `FoldContradiction::admission_refusal` |

Stays in the shell (`writer.rs`):

- The select loop, `WriterEvent`, shutdown drain (`terminate`), the
  `ingress_open` bookkeeping.
- `Flight` (`EncodedWal` + `JoinHandle`; the encoded bytes carry the batch)
  and `CheckpointFlight`. The state hands the shell an already-encoded run
  (decision 9); the shell retains the encoded bytes for the Unresolved
  reconcile compare.
  The shell's `Option<Flight>` and the state's in-flight marker are two
  records of one fact; `take_flight` and `record_wal_durable` are the only
  operations that change the marker, and a `Run` obligates the shell to spawn
  the create and hold its handle. This agreement is one new place for state
  to drift — it is on the assert and shell-seam test lists below.
- `CheckpointFlight` (`CheckpointTicket` + `PublicationGate` + `JoinHandle`).
  As with WAL, the shell task and the state's `ActiveCheckpoint` are two
  records of one active effect. Planning and completion are the only
  operations that change the marker. The shell returns the ticket on
  abandonment or install. Completion validates its source, cut, and attempt
  before it can change state.
- Evidence classification (`complete_flight`) and the `WriterExit` mapping.
- The collector spawn/reap and the `watch::Sender<PublishedView>`. The forest
  to publish comes back from the state.

Moves elsewhere:

- `decide_successor_uid` becomes a method on `Forest` (its rule belongs to the
  fold; `strict_fold` is its second caller). This deletes the
  forest→admission import.
- `engine.rs` imports `CommandEnvelope`, `CreateStream`, and
  `AdmissionRefusal` from the new module.

### The interface

Sketch, not spelling. The compiler and the first test pick the final shapes.

```rust
impl WriterState {
    /// Constructed only by bootstrap. pending empty, no flight,
    /// admitted == durable == the replayed forest, base == the Seal's forest.
    /// Asserts the handoff instead of trusting it: `next_batch` is the
    /// successor of `durable_batch`, and `durable_batch` does not precede
    /// the Seal's replay cut. The six raw parameters are acceptable because
    /// this one module now owns their meaning.
    fn new(claim: AuthoredClaim, seal: Seal, base: Forest, forest: Forest,
           durable_batch: BatchId, next_batch: BatchId) -> Self;

    /// Decide one typed command/reply envelope against admitted state.
    /// Owned mutation, no I/O.
    fn admit(&mut self, command: CommandEnvelope) -> Admitted;

    /// Take the next WAL run, or the replies that need no WAL coordinate.
    /// Callable at any time: answers `Idle` while a flight is active or
    /// pending is empty, so the shell loop calls it unconditionally and the
    /// "at most one flight" assert lives inside the take branch. A `Run`
    /// keeps the promoted commands in the state and obligates the shell to
    /// spawn the WAL create. On shutdown the shell drains by calling this
    /// until `Idle`, then asserts quiescence in the state (replaces the
    /// loop's "pending is empty" shutdown assert).
    fn take_flight(&mut self) -> FlightDecision;

    /// The shell proved that the in-flight run is durable. Fold every fact
    /// into `durable` (assert Applied), advance `durable_batch`, and release
    /// replies in admission order. Returns the replies and forest to publish.
    fn record_wal_durable(&mut self, batch: BatchId) -> DurableWal;

    /// If a checkpoint is due, builds CheckpointInput { source, base,
    /// snapshot: durable, cut, attempt }, increments the attempt counter,
    /// records the attempted cut, and stores the plan's CheckpointTicket as
    /// ActiveCheckpoint. Returns the input and the ticket for the shell.
    /// Counter exhaustion is an `expect` (decision 6).
    fn plan_checkpoint(&mut self) -> Option<CheckpointPlan>;

    /// An attempt ended before publication. Validates and clears the active
    /// ticket.
    fn abandon_checkpoint(&mut self, ticket: CheckpointTicket);

    /// A Direct advancing Seal: seal = successor, base = snapshot.
    /// `durable` does not change. Returns the (source, successor) Seal pair
    /// the shell feeds to `collect_advance` — the state consumes the
    /// install, so it must hand the collection inputs back. Validates the
    /// returned ticket and the install against the active source and cut,
    /// then clears the marker.
    fn install_checkpoint(
        &mut self,
        ticket: CheckpointTicket,
        install: CheckpointInstall,
    ) -> ...;
}

enum Admitted   { Settled(/* reply to send now */), Queued }
enum FlightDecision { Run(EncodedWal), Replies(Vec<...>), Idle }
struct CheckpointPlan { input: CheckpointInput, ticket: CheckpointTicket }
struct CheckpointTicket { source: SealGeneration, cut: BatchId, attempt: AttemptId }
```

Verb discipline (stromstyle §5): `admit` and `plan_checkpoint` decide; the
shell executes and commits I/O. `record_wal_durable` and
`install_checkpoint` record already-proven effect completions in local state.

Behavior to preserve exactly (the current call-site protocol):

- Refuse `Overloaded` when `pending.len() == WAL_RUN_FACTS_MAX`, before
  admission.
- Only a fact consumes a WAL coordinate: the suffix gate
  (`decide_suffix_room`) runs on the fact path only; a shed sets
  `retry_checkpoint_at = durable_batch` and refuses `Overloaded`.
- An idempotent reply is settled immediately only when no flight is active;
  otherwise it queues with `fact: None` and inherits the run's barrier.
- An all-idempotent pending set becomes replies, never a WAL run.
- `next_batch` advances when a run is taken, not when it commits.
- Checkpoint triggers: suffix span at least `WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER`,
  no checkpoint is active, and the durable head is unattempted or
  `retry_checkpoint_at` names it.

Invariants to assert inside the state (stromstyle §3 — liberal asserts):

- No flight and empty pending implies `admitted == durable`.
- A run is taken only when no flight is active (inside the take branch;
  `take_flight` itself is callable at any time); commit only when one is, and
  only for the batch named by that flight.
- The shell's `Option<Flight>` agrees with the state's in-flight marker:
  the shell holds a handle exactly when the state holds a flight.
- The shell's `Option<CheckpointFlight>` agrees with the state's
  `Option<ActiveCheckpoint>`; their tickets are equal, and an install or
  abandonment must return that attempt's source, cut, and attempt.
- A checkpoint attempt never names a future durable cut.
- The durable WAL head never precedes the replay cut.
- At construction: `next_batch` is the successor of `durable_batch` (in
  `new`, per the constructor contract above).

## Test plan: replace, don't layer

New protocol tests live in `writer/state.rs` with the state (stromstyle §6:
unit tests for private pure logic live in the file under test). They are
scripted scenarios at the `WriterState` interface — no object store, no
tasks, no runtime. Prefer one small scenario harness; cases as data.

Scenarios (each names its claim):

1. Admit, take flight, record durability: replies release in admission order;
   the published forest equals the folded durable forest.
2. A duplicate create behind a flight queues and replies only after WAL
   durability is recorded; without a flight it settles immediately.
3. The suffix gate sheds a new fact but still answers an idempotent command;
   the shed arms `retry_checkpoint_at`.
4. An all-idempotent pending set produces `Replies`, consuming no WAL
   coordinate.
5. Pending at `WAL_RUN_FACTS_MAX` refuses `Overloaded`.
6. Checkpoint triggering and planning: below the span trigger returns `None`;
   at it returns one plan; an attempted cut suppresses; `retry_checkpoint_at`
   re-arms; each plan advances the attempt id and returns its exact ticket.
7. `install_checkpoint` moves `seal` and `base`; `durable` and pending
   replies are untouched. An abandoned attempt clears its marker.
8. Admission refusal axes (ports of the current `admission.rs` tests):
   occupied path, config mismatch per axis, not-live close/delete,
   already-exists / already-closed idempotence.
9. Negative transitions: record durability without a flight, record it for
   the wrong batch, return the wrong checkpoint ticket, supply an install for
   the wrong source/cut, and call `take_flight` during an active flight. After
   every refusal or inactive decision, assert that unrelated state is
   unchanged. The suffix shed is the named exception: it changes only
   `retry_checkpoint_at`.

Moves and deletions:

- `admission.rs` unit tests: absorbed into scenarios above; delete the file.
- The `decide_successor_uid` test moves to `forest.rs` beside the method.
- `tests/engine/*.rs` stay as they are. They cover durability evidence,
  fencing, reopen, and the bounded suffix through the real seam
  (`Engine` + fault store); this refactor does not change that behavior.
  After the state tests exist, delete any engine test that only re-checks an
  admission outcome and nothing durable (audit, expected: none or few).
- Keep one writer-seam claim that observes publication before reply release.
  Pure state scenarios prove reply selection and order, but they cannot prove
  the shell installs `PublishedView` before it sends those replies.
- Add focused writer-seam claims for the two split effect records: every WAL
  `Run` has one shell flight until its completion clears both records, and
  every `CheckpointPlan` has one shell flight with the same ticket until
  install or abandonment clears both records.

## Order of work

Each completed step compiles and passes `just ci` on its own.

1. Move `decide_successor_uid` onto `Forest`; fix the two callers.
2. Create `writer/state.rs`, absorb `admission.rs`, and rewrite `writer.rs`
   against the new interface as one compiling change. `Writer::new`
   temporarily constructs `WriterState` from `WriterSeed`. Write the protocol
   scenarios with the state instead of deferring them. Delete `admission.rs`
   only after its callers import the new module.
3. `Ready { state }` in `bootstrap.rs`; delete `WriterSeed` and
   `into_writer_seed`; `Engine::open` takes the initial view from the state.
4. Add the publication-before-reply and split-effect seam claims, audit
   superseded engine tests, and run final `just ci`.

## Follow-up, out of scope here

- Update `docs/architecture.md`: the three moments (`AdmittedState`,
  `DurableState`, `PublishedView`) are owned by one `WriterState`; the typed
  store traits sketch should say concrete types (one adapter each — a trait
  there would be a hypothetical seam).
- Fold `forest/directory.rs` and `forest/ledger.rs` into `forest.rs`
  (pass-throughs; they fail the deletion test).
- Give `Forest` a `delta_since(&base)` so `checkpoint/prepare.rs` stops
  reading raw row maps and `imbl` diffs across the seam.
- Unify `SealStoreError`/`WalStoreError`/`TableStoreError` into one store
  failure enum; delete the per-caller mapping functions. Give
  `WalReplayPoint` a `batch()` accessor; delete the four local
  `replay_batch` copies.
- Derive the object keys in `tests/engine/support.rs` from observation
  instead of hardcoding "the first checkpoint Seal is generation three".
