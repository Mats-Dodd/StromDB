# Coding Principles

“Perfection is achieved, not when there is nothing more to add, but when there is nothing left to take away.” ― Antoine de Saint-Exupéry


The rules here use MUST and SHOULD deliberately. MUST is a hard requirement. SHOULD is a
strong default that requires a stated reason to break. Where practical, enforce every MUST
through the type system, a lint, or a test.


## North star

Code MUST support local reasoning. A reader determines what a component does
from its types, its explicit inputs and outputs, and its immediate
implementation—without tracing hidden aliases, ambient state, or unchecked
assumptions. Three ideas recur throughout this document: local reasoning,
preservation of knowledge, and owned local mutation.

When writing code, these are the top considerations:

1. Make illegal states unrepresentable, not merely unrepresented. Lean heavily on domain
   modeling to parse, not validate. Let the compiler reject entire classes of bugs through
   ADTs and typestates.

2. Determinism is non-negotiable. Non-deterministic capabilities enter through explicit shell
   dependencies; the shell passes their outputs into the pure core as ordinary values.

3. Use assertions liberally for in-process invariants that are impractical to encode in the
   type system. Assertions encode those invariants and halt the program loudly when the code
   is wrong. 

4. Simplicity and explicitness are of utmost importance. Code should be legible and
   imperative. 


When an invariant needs enforcing, escalate through representations in this
order:

1. An ordinary representation: an enum, a newtype, a non-empty collection.
2. A hidden constructor; parse at the boundary.
3. Ownership and consuming transitions (typestate).
4. A plan or witness type for an important relational fact.
5. Phantom branding, only for a demonstrated, consequential mistake.

At every step, stop when the next step costs more than it removes. Unencoded uncertainty from
outside the process remains a typed failure. An unencoded invariant controlled entirely by the
current process gets an `assert!` and a rationale. The goal is not maximum type safety—it is
maximum useful compiler knowledge per unit of cognitive complexity.

## 1. Domain modeling—parse, don't validate

- You MUST make illegal states unrepresentable in the type system before reaching for a
  runtime check. Prefer a closed `enum` over a `bool` plus an `Option`; prefer a type that
  cannot hold a bad value over a function that rejects one. 

- You MUST enforce invariants at construction and then trust them inward. A value parsed
  into a domain type is not revalidated by every consumer. 

- Judge a distinct domain meaning by the mix-up the type prevents where it is passed, never by
  whether it validates anything.

- All constructions of a domain value from raw or foreign data MUST use the standard
  conversion traits: use `TryFrom` or `FromStr` for fallible conversions, and `From` for
  infallible widening. You MUST NOT create ad hoc inherent methods like `parse_*` or `from_*`
  for this purpose. One value has one canonical parser.

- Integer conversions are fallible by default. `as` MUST NOT be used for integer
  conversions because it can silently truncate or change sign. Use `From` or `Into` for
  lossless widening and `TryFrom` or `try_into` for everything else.

- You MUST parse data where it crosses into a new layer: public API inputs,
  encoded records, storage paths, configuration, and bytes read back from durable storage.

- Where practical, check persistence invariants on both sides: before writing and after
  reading. A codec SHOULD cross-check identifiers or positions encoded in a payload against
  the durable location from which it was loaded.

- Use precise domain nouns such as `stream`, `record`, `position`, `segment`, `reader`,
  `writer`, and `checkpoint`, not generic names such as `item`, `data`, or `manager`.


When doing domain modeling in the type system, use these guiding principles:

1. Define our data's positive space, not its negative space.
2. Decouple data representation from interpretation.
3. Use types to encode obligations to write total functions.
4. Push obligations to the places best equipped to handle them.

- Before weakening a return type to `Option` or `Result`, try to strengthen a
  parameter type instead. `fn head(list: &NonEmpty<T>) -> &T` beats
  `fn head(list: &[T]) -> Option<&T>` whenever the caller can hold the proof.
- Every `Option` or `Result` variant MUST represent genuine uncertainty at that call site. If
  a fact can be established once upstream and preserved in a type, prefer requiring that
  stronger type and let the ripple push the check outward toward the system's edge.


### The lifecycle of knowledge

Data moves through the system along one path, and each step preserves what was
learned at the step before:

```
foreign bytes
    │ parse            — `TryFrom` / `FromStr` (§1)
    ▼
domain value
    │ decide / plan    — pure core (§5)
    ▼
executable plan
    │ apply / commit   — shell (§5)
    ▼
durable bytes
    │ parse again on recovery
    ▼
domain value
```


A parsing or refinement check whose fact later code relies upon MUST return a domain value or
witness, not `()` or a bare `bool`. A check that returns unit can be omitted and the program
still compiles; a check that returns the parsed value or a witness cannot. Observation
predicates may return `bool`, and effectful completion may return `Result<(), Error>`. Bytes
read back from durable storage are foreign again and re-enter the path at the top.


## 2. Typestate and witnesses

- Use typestate for small, in-process protocols where the legal operations
  depend on the current state and transitions naturally consume the previous
  state—for example, a segment that moves from open to sealed.

- Use an `enum` when the state must be decoded, persisted, selected
  dynamically, or stored heterogeneously.

- A typestate transition SHOULD consume `self` when continued use of the old
  state would be invalid. Operations that do not change state take `&self` or
  `&mut self`.

- When typestate uses a marker trait, seal it so callers cannot add states. Put
  state-specific data inside the state type, not in an `Option` on the shared struct.

- Do not use phantom types self indulgently. Prefer an enum or a
  private constructor unless typestate removes invalid call sequences or
  repeated failure handling. Phantom branding is a last resort, reserved for a
  demonstrated, consequential mix-up between two live values of the same type.


## 3. Errors and panics

- `Result` is the default. Use `expect` or `assert!` only for violated
  in-process invariants—programmer errors that cannot occur unless the code is wrong.

- Data that crossed a durability, process, or network boundary and came back contradictory
  is a **failure, not a bug**. It MUST be a `Result` variant because corruption, partial
  writes, incompatible data, and backend faults are possible. Reserve panics for state
  controlled solely by the current process.

- Error enums MUST mirror the caller's decisions, not implementation details. Separate
  variants when callers respond differently—for example invalid input, optimistic
  concurrency conflict, retryable storage failure, corrupt durable data, or a bounded-work
  limit. Merge variants callers always handle identically.  Too many error variants is just as bad, if not worse than too few.  

- Assertions run in every build. `assert!` is the default; `debug_assert!` MUST NOT be
  used except with a measured cost justification carried in an
  `#[expect(clippy::disallowed_macros, reason = "...")]`.

- Assert the negative space as well as the positive space: state what you expect to be
  true AND what you expect to be impossible. Bugs concentrate at the boundary where data
  moves between valid and invalid, so guarding only one side leaves the crossing
  unwatched.

- Assert relationships between compile-time constants with
  `const _: () = assert!(...);`. Header sizes, limits, and layout arithmetic can be
  proven consistent before the program runs, which is the strongest form of making
  illegal states unrepresentable.

- Assertions are a safety net, not a substitute for understanding. A fuzzer or simulator
  proves the presence of bugs, never their absence. Build the mental model first, encode
  it as assertions, and treat simulation as the last line of defense—not the first.


## 4. Determinism

- Wall-clock time, randomness and ID generation, task scheduling decisions, filesystem or
  storage I/O, and other non-determinism MUST enter the shell through explicit dependencies,
  never ambient globals. Use traits where the capability requires substitution; pass the
  resulting time, identifier, random choice, or observation into the pure core as data.
- Production implementations and deterministic test implementations MUST cross the same
  seam. Constructors should make the runtime dependencies explicit rather than allowing
  library internals to call `SystemTime::now`, random generators, or the filesystem
  directly.
- This rule enables deterministic end-to-end tests. Treat hidden time, randomness, direct
  I/O, or schedule dependence in library code as a defect.


## 5. State and purity

Functional style means explicit dataflow, total functions, immutable
interfaces, and effects at the boundary. Owned local mutation is an
implementation detail and is often the clearest, fastest way to implement a
pure transformation.

- Separate deciding from doing. The decision half of an operation—whether an append
  proceeds, where a record lands, what recovery does with a torn tail—MUST be pure: a
  function from state and input to an outcome, performing no I/O and touching no
  injected runtime trait. The shell gathers inputs, calls the core, and performs the
  effects. The stated exception is byte-moving hot paths, which get carefully bounded,
  locally owned mutation rather than contortions to preserve the split.

- Structure an impure operation as one sandwich: gather inputs, decide, act. If a second
  read is needed before deciding, that is two sandwiches with an explicit state handoff,
  not a function that interleaves I/O with logic.

- Hoist non-determinism to the shell. Injected clocks and identifier sources exist for
  the shell's benefit; the core SHOULD receive time, identifiers, and randomness as plain
  arguments. A core that takes a timestamp as data needs no mock at all.

- Purity is judged by what escapes, not by `mut`. A function that mutates local state or
  scratch buffers but is referentially transparent from the outside is pure. What makes
  a function impure: I/O, interior mutability, or writes through `&mut` that reach
  beyond it. Treat `&mut self` in a public signature as a design decision, not a default.

- Prefer immutable values and value semantics at public seams. When mutation is useful,
  prefer exclusive mutation through an owned value or a short-lived `&mut`; an exclusively
  owned representation MAY be updated in place rather than cloned. This is what preserves
  local reasoning.
  Interior mutability is exceptional because it lets state change through a
  shared reference; encapsulate it in the smallest module that can state and
  maintain its invariant.

- A function MUST accept the least authority it needs: `&T` rather than `&mut T`, a narrow
  storage or runtime capability rather than the entire subsystem, and plain data rather than
  a service handle when the operation is pure. Taking ownership communicates consumption or
  transformation; taking `&mut` communicates observable mutation.

- Shared mutable state MUST be enumerable. You should be able to list every piece—log
  tail, index, reader positions, in-flight batches—and name its single writer, its
  mutation protocol, its consistency invariant, and its bounded lifetime or shutdown
  path. Every use of interior mutability (`Mutex`, `RwLock`, atomics) is a deliberate
  decision; prefer a single writer fed by channels over a lock shared by
  many. The commit point is exactly where a pure decision becomes shared state, so this
  rule and the singular-commit-point rule enforce each other.

- Typed indices and handles are aliases too. An arena or slab index MUST have a
  domain type and a single owner that resolves it; raw `usize` handles do not
  travel between modules.

- Push `if`s up and `for`s down. When splitting a large function, keep branching and
  state manipulation in the parent and move non-branchy logic into helpers. The parent
  holds the state and decides; leaf functions SHOULD be pure, computing what needs to
  change rather than applying it. Pure leaves are also the functions property tests can
  reach directly.

- Make purity visible at the call site with verb pairs: `decide_*` and `plan_*` are
  pure; `apply_*`, `execute_*`, and `commit_*` belong to the shell. A pure name on an
  impure function is a defect.

- A plan is valid only for the state or generation from which it was derived. If that state
  can change between deciding and applying, the implementation MUST either retain exclusive
  ownership across both steps or carry an expected generation that the singular commit point
  atomically revalidates.

- Matches on domain enums we own MUST be exhaustive, with no `_` arm, so the
  compiler shows every location a new variant touches. A `_` arm is acceptable
  only on foreign or `#[non_exhaustive]` enums.

- A compilation error caused by a domain-model change is a list of obligations to reconsider.
  Do not silence it with an `Option`, default value, cast, clone, or wildcard match unless the
  new domain semantics justify that choice.

- All functions in this codebase must be "total", in so far as there should be no dead code arms or unreachable blocks.  Reaching for these is a code smell that points to bad domain modelling further up the callstack.  In these situations, go back to the source and model its invariants more explicitly.  

## 6. Module and crate structure

- A crate is defined by its public seam. `lib.rs` MUST expose only the names a caller needs,
  using explicit `pub use`. Everything else MUST be `pub(crate)` or tighter. Use the full
  visibility ladder (`pub`, `pub(crate)`, `pub(in path)`, private) as a design tool.

- `unsafe` code is forbidden in this workspace. 

- Each module has one responsibility. Split implementation into focused modules rather than
  concentrating it in a large `lib.rs`. Size is judged by whether the module fits in a
  reader's head, not by a fixed line count.

- Declare variables at the smallest scope and check values close to where they are used.
  A check separated from its use by time or code is a check that can go stale.

- Order files for the reader: the most important item goes at the top, and within a type,
  fields come before methods.

- Unit tests for private, pure logic MUST live in the file under test. 

- Introduce a trait seam where substitutability is required, such as a storage backend,
  clock, scheduler, or source of identifiers, or where a protocol is genuinely shared
  across implementations. Do not generalize speculatively; abstract when there is a second
  concrete case.

- Keep control-plane operations such as stream creation and configuration distinct from
  data-plane operations such as append, read, trim, flush, and recovery.

- Default to concrete signatures: borrowed parameters (`&str`, `&[u8]`, `&StreamName`) and
  owned returns. Use generic bounds such as `impl AsRef<_>` or `impl IntoIterator<Item = _>`
  only when a second caller shape exists.

- Pass options explicitly at the call site instead of relying on a library's defaults.
  Spelled-out options read better and, more importantly, do not silently change meaning
  when the library changes its defaults.


## 7. Testing

Tests are executable claims about strom's domain, protocols, and externally observable
behavior. They protect invariants and contracts, not the current shape of the implementation.
Test code follows the same standards of simplicity, explicitness, determinism, naming, and
bounded work as production code.


### Test claims and leverage

- Every permanent test MUST protect a named semantic claim. The claim SHOULD be evident from
  the test name and body. 

- A test MUST protect at least one of the following:
  - a domain invariant;
  - a protocol transition, ordering rule, or commit rule;
  - a public behavioral contract;
  - a durable-format or external-specification anchor;
  - a regression with a plausible chance of recurring;
  - an adapter contract at an external boundary; or
  - a resource, complexity, or performance bound.

- A test that cannot identify a plausible incorrect implementation it would reject does not
  justify its maintenance cost.

- Do not test getters, field assignments, derived implementations, or guarantees already
  enforced by the type system. Do not assert private call sequences, helper boundaries, mock
  interactions, internal data-structure layout, or incidental `Debug` output.

- A behavioral test SHOULD survive replacement of the implementation with a structurally
  different implementation that provides the same contract. If it fails solely because
  functions, fields, or collaborators were rearranged, it tests implementation rather than
  behavior.

- A narrow internal test MAY be useful while developing difficult machinery. It need not
  become permanent; delete it when stable boundary coverage strictly supersedes it.


### Boundaries, purity, and extent

- Prefer the hardest stable semantic boundary available. For a library this is usually its
  public API; for an internal protocol layer it is the layer's input, output, and state-machine
  seam. Test private logic directly only when the invariant is owned by that private component
  and is clearer there.

- Do not optimize for isolated "unit" tests. Optimize for pure tests with focused claims. A
  test MAY exercise many internal layers when it remains deterministic, fast, and expressed
  through a stable boundary.

- Ruthlessly minimize generalized I/O in tests: wall-clock time, sleeping, threads, files,
  sockets, processes, and environment state make tests slower, less repeatable, and more
  fragile. The amount of code exercised is not itself a reason for a test to be slow.

- Do not mock our own modules merely to reduce test extent. Replace impure external
  effects with in-memory or scripted implementations at their existing seams.

- Tests MUST NOT use sleeps, timing margins, or repeated polling to establish causality. If a
  test must wait and hope, the production interface has hidden an event that the test needs to
  observe.

- A test that intentionally uses real I/O, multiple threads, or another process MUST justify
  why a purer test cannot establish the same claim. Slow tests MUST be identifiable and kept
  out of the default local feedback loop.

### Protocol tests without I/O

- Protocol logic SHOULD be testable without real storage I/O, sleeping, task scheduling, or
  ambient time. Tests feed commands, observations, effect completions, and explicit time into
  the protocol; they observe state, plans, and requested effects as data.

- A scripted protocol scenario SHOULD state inputs and expected observations as data rather
  than duplicate control flow across tests. Inputs and expected outputs MUST be fixed and
  directly reviewable.

- Assert protocol invariants after every meaningful transition, not only at the end of a
  scenario. An accepted transition preserves all invariants; a rejected transition MUST leave
  state unchanged unless its contract explicitly says otherwise.

- Protocol coverage MUST include normal operation and the negative space around it: malformed
  input, duplicate input, retry, partial completion, stale generation, out-of-order completion,
  and boundaries immediately before and after a state becomes valid.

- Positions, durability, and visibility MUST be tested at their defined commit points. Tests
  SHOULD establish monotonicity, ordering, idempotence, and bounded-work properties wherever
  those are part of the protocol.


### Harnesses and fixtures

- When several tests exercise the same contract, centralize setup, execution, invariant
  checks, and diagnostics in one domain-specific `check` function or scenario harness. Cases
  SHOULD then be data, not repeated test code.

- A test helper is an API. Keep it narrow, use domain types, make invalid scenarios difficult
  to express, and report violated invariants in domain language. A helper used once is usually
  indirection rather than infrastructure.

- Fixtures MUST contain only facts relevant to the claim. Prefer small constructed values to
  copied production artifacts; use a real artifact only when its exact bytes or structure are
  the subject of the test.

- Failure output MUST contain enough context to diagnose the semantic mismatch without first
  reproducing it under a debugger. Invest in one high-quality diagnostic at the shared harness
  rather than bespoke assertion libraries throughout the suite.

### Properties and examples

- Properties are preferred when the domain claim is naturally universal. State the property
  independently of the implementation and generate valid inputs constructively from domain
  types.

- Exhaustively enumerate a closed, small input space. Enumeration dominates sampling for
  cases such as every truncation point, every error variant, and singleton domains.

- Generated tests MUST report enough information to reproduce a failure, including the seed
  and minimized counterexample where the framework supports them.

- A specific example MUST be justified as one of:
  - a **spec anchor**: a literal constant or known-answer vector from an external source,
    cited to that source;
  - a **minimal boundary or counterexample** that makes a transition precise;
  - a **regression reproducer** for a defect with a plausible chance of recurring;
  - a **protocol scenario** whose sequence is itself the claim; or
  - a **cost-bounded representative** of a stated property that is too expensive to exercise
    broadly.

- Do not contort a clear regression into an elaborate generator merely to avoid an example.
  Conversely, do not accumulate examples when one property or exhaustive enumeration strictly
  dominates them.

- Round trips are necessary but not sufficient for codecs: an encoder and decoder can agree on
  the same wrong format. Every durable format or externally defined bound MUST have an
  independent spec anchor.

### Durable and external boundaries

- Most protocol coverage belongs above an in-memory or scripted effect seam. Real-I/O tests
  verify the adapter and the seam itself; they MUST NOT repeat every protocol scenario through
  the filesystem.

- Each storage adapter SHOULD pass the same contract suite. Adapter-specific tests cover only
  behavior unique to that implementation.

- Durable-stream behavior MUST be tested across reopen and recovery boundaries. Acknowledged
  durable data survives reopen; visibility, trim, flush, and checkpoint behavior remain
  consistent with their contracts after reconstruction from persisted bytes.

- Tests MUST cross the boundary between valid and invalid durable state: truncated tails,
  positions past the end, contradictory identifiers, and checksums differing by one bit. The
  system MUST reject corruption without partially applying it or silently manufacturing a
  valid domain value.

### Test review

Before accepting a test, answer:

1. What domain, protocol, or system claim does it protect?
2. What plausible incorrect implementation makes it fail?
3. Does it observe stable behavior or internal arrangement?
4. Would it survive a structurally different correct implementation?
5. Could a property, exhaustive enumeration, or shared scenario replace several examples?
6. Is this the purest faithful level at which to test the claim?
7. Is its maintenance and execution cost proportionate to the risk?

If these questions do not have crisp answers, simplify the test, move it to a better boundary,
or delete it.


## 8. Concurrency and async

- Do not hold a lock across `.await` unless the lock is designed for async use and the
  critical section is intentionally small.

- All fan-out, queues, buffers, retries, and background work MUST be bounded by named
  limits, not allowed to grow with input without constraint.

- Do not do work directly in reaction to external events. Queue or shed the event and let the
  system drain at its own pace. Keeping control flow under the program's control is what
  makes bounds on work per unit time enforceable, and batching falls out for free.

- Make every protocol's linearization or commit point explicit and singular. Operations
  that can run more than once due to retries or recovery MUST be idempotent.

- Preserve per-stream ordering explicitly.

- Cancellation safety MUST be considered for async operations that mutate state or own
  resources.

- `Drop` MAY release process-local resources, but MUST NOT be the sole mechanism for flushing,
  committing, publishing, or completing a durable protocol. Correctness-critical completion
  MUST be an explicit, fallible operation.

- Clone semantics are deliberate. Use `Arc::clone` to make shared ownership visible. Do not
  contort an API to avoid a cheap clone, and do not clone in a hot path merely to escape a
  borrow.

## 9. External dependencies and adapter seams

- Wrap an external dependency in the narrow contract strom uses instead of threading its
  types throughout the codebase. Translate the dependency's error vocabulary into domain
  outcomes at the adapter boundary.

- Foreign dependency types MUST NOT appear in a crate's public API unless exposing that
  dependency is the crate's explicit purpose.

- Keep dependencies boring and justified. strom is foundational storage code, so every
  dependency adds supply-chain, determinism, build, and operational risk. Prefer the
  standard library and existing workspace dependencies.

## 10. Performance

- Write a back-of-the-envelope cost sketch before adding storage layouts, scans, caches,
  batching, or background work. Consider storage I/O, memory, CPU, and, where applicable,
  network costs in both latency and bandwidth.

- Optimize the slowest resource first. For embedded durable streams, persistence latency,
  write amplification, recovery work, and read amplification generally deserve attention
  before CPU micro-optimizations; verify with measurement. That said, where appropriate, data oriented design can be useful, have mechanical sympathy where valid.  

- Batch storage work where the durability contract permits, but keep batches bounded and
  make their visibility and durability semantics explicit.

- Make expensive operations obvious at the call site. Names should reveal scans, recovery,
  allocation, blocking I/O, flushing, synchronization, and persistence.

- Put bounds on all work: record and batch sizes, segments, scans, retries, queues, buffers,
  recovery, and background tasks have caller-imposed limits.


## 11. Naming and comments

- Get the nouns and verbs just right. Naming is domain modeling: a great name captures
  what a thing is or does and hands the reader a crisp mental model. Time spent finding
  the right name is design work, not polish.

- One name, one meaning. Do not overload a term with context-dependent meanings—if
  `position`, `offset`, `index`, and `sequence` are distinct concepts, each word means
  exactly one of them everywhere, and nothing else.

- Prefer nouns over adjectives and participles for the things you will talk about.
  A noun such as `pipeline` can be used directly in a document or conversation and
  composes for derived names (`pipeline_depth_max`); a participle such as `preparing`
  must be rephrased every time.

- A name SHOULD tell the reader something the type does not. `dir: PathBuf` is
  ceremony; `wal_dir: PathBuf` places the value in the domain.

- Comments explain **why**, never narrate **what**. Code shows what happens; comments and
  architecture decisions preserve why.


- Include units and qualifiers as suffixes, ordered by descending significance:
  `latency_ms_max`, not `max_latency_ms`. The name then starts with the most significant
  word, so related variables (`latency_ms_min`, `latency_ms_p99`) group together in
  sorted lists and line up in the source. Other examples: `flush_interval_ms`,
  `batch_bytes_max`, `segment_count`, `position_inclusive`.

- Give related names the same length where you can: `source` and `target` beat `src` and
  `dest`, because derived pairs such as `source_offset` and `target_offset` then align,
  making parallel code symmetrical and easier to check by eye.

- Do not abbreviate unless the abbreviation is standard in the domain, such as `I/O`,
  `WAL`, or `SST`. In scripts and tooling, use long-form flags: `--force`, not `-f`.
  Single-letter flags are for interactive use.
