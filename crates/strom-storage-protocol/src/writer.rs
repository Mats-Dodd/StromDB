//! Pure admission-to-durability writer machine.

use strom_domain::{
    CloseStreamOutcome, CreateOutcome, ExpiryPolicy, StreamContentType, StreamLifecycle,
};
use strom_storage_domain::{
    AttemptId, BatchId, DirectoryEntry, DirectoryKey, EncodedAuthoritySeal, EncodedWal,
    OperationFact, OwnerToken, PartitionId, Seal, SealGeneration, WAL_RUN_FACTS_MAX,
    WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER, WAL_SUFFIX_COORDINATES_MAX_V2, WalBody, WalFacts,
    WalObject, WalReplayPoint,
};
use tokio::sync::oneshot;

use crate::forest::{Applied, FoldContradiction, Forest};
use crate::outcome::{SealPublication, TypedStoreError, WalEstablishment};

/// The maximum ordered work emitted by one machine event.
pub const WRITER_OUTPUTS_PER_STEP_MAX: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStream {
    pub path: DirectoryKey,
    pub content_type: StreamContentType,
    pub expiry: ExpiryPolicy,
    pub lifecycle: StreamLifecycle,
}

/// A command that did not enter admitted state or consume a WAL coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionRefusal {
    #[error("stream path is already occupied")]
    PathOccupied,
    #[error("partition path capacity is exhausted")]
    PathCapacityExhausted,
    #[error("stream path is not live")]
    PathNotLive,
    #[error("partition writer is at a bounded capacity limit")]
    Overloaded,
}

#[derive(Debug)]
pub enum CommandEnvelope {
    Create {
        command: CreateStream,
        reply: oneshot::Sender<Result<CreateOutcome, AdmissionRefusal>>,
    },
    Close {
        path: DirectoryKey,
        reply: oneshot::Sender<Result<CloseStreamOutcome, AdmissionRefusal>>,
    },
    Delete {
        path: DirectoryKey,
        reply: oneshot::Sender<Result<(), AdmissionRefusal>>,
    },
}

#[derive(Debug)]
pub enum Completion {
    Create {
        outcome: Result<CreateOutcome, AdmissionRefusal>,
        reply: oneshot::Sender<Result<CreateOutcome, AdmissionRefusal>>,
    },
    Close {
        outcome: Result<CloseStreamOutcome, AdmissionRefusal>,
        reply: oneshot::Sender<Result<CloseStreamOutcome, AdmissionRefusal>>,
    },
    Delete {
        outcome: Result<(), AdmissionRefusal>,
        reply: oneshot::Sender<Result<(), AdmissionRefusal>>,
    },
}

/// Correlation identity for one exact checkpoint attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CheckpointTicket {
    source: SealGeneration,
    cut: BatchId,
    attempt: AttemptId,
}

impl CheckpointTicket {
    #[must_use]
    pub const fn new(source: SealGeneration, cut: BatchId, attempt: AttemptId) -> Self {
        Self {
            source,
            cut,
            attempt,
        }
    }

    #[must_use]
    pub const fn source(self) -> SealGeneration {
        self.source
    }

    #[must_use]
    pub const fn cut(self) -> BatchId {
        self.cut
    }

    #[must_use]
    pub const fn attempt(self) -> AttemptId {
        self.attempt
    }
}

/// Immutable input to one checkpoint preparation effect.
#[derive(Debug)]
pub struct CheckpointInput {
    ticket: CheckpointTicket,
    source: Seal,
    base: Forest,
    snapshot: Forest,
}

impl CheckpointInput {
    /// # Panics
    ///
    /// Panics when `ticket` does not name `source`'s exact generation.
    #[must_use]
    pub fn new(ticket: CheckpointTicket, source: Seal, base: Forest, snapshot: Forest) -> Self {
        assert_eq!(
            ticket.source,
            source.generation(),
            "checkpoint input names its exact source Seal"
        );
        Self {
            ticket,
            source,
            base,
            snapshot,
        }
    }

    #[must_use]
    pub const fn ticket(&self) -> CheckpointTicket {
        self.ticket
    }

    #[must_use]
    pub fn into_parts(self) -> (CheckpointTicket, Seal, Forest, Forest) {
        (self.ticket, self.source, self.base, self.snapshot)
    }
}

/// A fully prepared advancing checkpoint awaiting authority publication.
#[derive(Debug)]
pub struct PreparedCheckpoint {
    source: Seal,
    successor: Seal,
    snapshot: Forest,
    candidate: EncodedAuthoritySeal,
}

impl PreparedCheckpoint {
    /// # Panics
    ///
    /// Panics unless the successor, ticket, source, replay cut, owner, and
    /// encoded authority candidate describe one exact checkpoint.
    #[must_use]
    pub fn new(
        ticket: CheckpointTicket,
        source: Seal,
        successor: Seal,
        snapshot: Forest,
        candidate: EncodedAuthoritySeal,
    ) -> Self {
        assert_eq!(
            ticket.source,
            source.generation(),
            "a prepared checkpoint names its exact source Seal"
        );
        assert_eq!(
            source.partition(),
            successor.partition(),
            "a checkpoint successor retains its source partition"
        );
        assert_eq!(
            source.generation().successor(),
            Ok(successor.generation()),
            "a checkpoint successor is one exact Seal generation"
        );
        assert_eq!(
            WalReplayPoint::Through {
                batch: ticket.cut,
                owner: OwnerToken::from(ticket.attempt.owner_claim()),
            },
            successor.replay(),
            "a prepared checkpoint publishes its planned WAL cut and owner"
        );
        assert_eq!(
            candidate.seal(),
            &successor,
            "the encoded authority candidate is the prepared successor"
        );
        Self {
            source,
            successor,
            snapshot,
            candidate,
        }
    }

    #[must_use]
    pub const fn successor(&self) -> &Seal {
        &self.successor
    }
}

/// Decided result of checkpoint preparation and its bounded table pipeline.
#[derive(Debug)]
pub enum PreparationOutcome {
    Prepared(Box<PreparedCheckpoint>),
    Abandoned,
    Contradiction { detail: String },
}

/// One observation delivered to the pure writer machine.
#[derive(Debug)]
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
    CollectFinished {
        cut: BatchId,
    },
    CheckpointPreparationCancelled {
        ticket: CheckpointTicket,
    },
}

/// Correlation identity retained by the effect interpreter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EffectKey {
    Wal { batch: BatchId },
    CheckpointPreparation { ticket: CheckpointTicket },
    SealPublication { ticket: CheckpointTicket },
    Collection { cut: BatchId },
}

/// Completion-producing I/O requested by the machine.
#[derive(Debug)]
pub enum WriterEffect {
    EstablishWal(EncodedWal),
    PrepareCheckpoint(CheckpointInput),
    PublishAuthority {
        ticket: CheckpointTicket,
        candidate: EncodedAuthoritySeal,
    },
    Collect(CollectionInput),
}

/// One validated leak-only collection transition.
#[derive(Debug)]
pub struct CollectionInput {
    cut: BatchId,
    source: Seal,
    successor: Seal,
}

impl CollectionInput {
    fn new(cut: BatchId, source: Seal, successor: Seal) -> Self {
        assert_eq!(
            Some(cut),
            successor.replay().batch(),
            "collection names its advancing successor's exact replay cut"
        );
        Self {
            cut,
            source,
            successor,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (BatchId, Seal, Seal) {
        (self.cut, self.source, self.successor)
    }
}

impl WriterEffect {
    /// # Panics
    ///
    /// Panics when a collection successor lacks its required replay cut.
    #[must_use]
    pub const fn key(&self) -> EffectKey {
        match self {
            Self::EstablishWal(candidate) => EffectKey::Wal {
                batch: candidate.batch(),
            },
            Self::PrepareCheckpoint(input) => EffectKey::CheckpointPreparation {
                ticket: input.ticket,
            },
            Self::PublishAuthority {
                ticket,
                candidate: _,
            } => EffectKey::SealPublication { ticket: *ticket },
            Self::Collect(input) => EffectKey::Collection { cut: input.cut },
        }
    }
}

/// Immediate interpreter mutation requested by the machine.
#[derive(Debug)]
pub enum WriterAction {
    PublishView(Forest),
    SendReplies(Vec<Completion>),
    CancelCheckpointPreparation { ticket: CheckpointTicket },
}

/// One item in the machine's total output order.
#[derive(Debug)]
pub enum WriterOutput {
    Effect(WriterEffect),
    Action(WriterAction),
}

/// One complete synchronous machine transition.
#[derive(Debug)]
pub struct WriterStep {
    outputs: Vec<WriterOutput>,
    exit: Option<WriterExit>,
}

impl WriterStep {
    #[must_use]
    pub fn into_parts(self) -> (Vec<WriterOutput>, Option<WriterExit>) {
        (self.outputs, self.exit)
    }
}

/// Why the writer interpreter stops.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WriterExit {
    #[error("partition writer shut down after draining ingress")]
    Shutdown,
    #[error("partition writer was fenced at WAL batch {batch:?}")]
    Fenced { batch: BatchId },
    #[error("partition writer was poisoned at WAL batch {batch:?}: {detail}")]
    Poisoned { batch: BatchId, detail: String },
    #[error("partition writer found a contradiction at WAL batch {batch:?}: {detail}")]
    Contradiction { batch: BatchId, detail: String },
}

/// Proof that bootstrap directly authored the live claim Seal.
#[derive(Debug)]
#[expect(
    missing_copy_implementations,
    reason = "the authority witness is deliberately linear even though its representation is Copy"
)]
pub struct AuthoredClaim {
    generation: SealGeneration,
    owner: OwnerToken,
}

/// Complete proven handoff from bootstrap into the live writer.
#[derive(Debug)]
pub struct WriterRecovery {
    pub(crate) claim: AuthoredClaim,
    pub(crate) seal: Seal,
    pub(crate) base: Forest,
    pub(crate) durable: Forest,
    pub(crate) durable_batch: BatchId,
}

impl WriterRecovery {
    #[must_use]
    pub const fn partition(&self) -> PartitionId {
        self.seal.partition()
    }

    #[must_use]
    pub const fn durable_forest(&self) -> &Forest {
        &self.durable
    }

    #[must_use]
    pub const fn durable_batch(&self) -> BatchId {
        self.durable_batch
    }
}

impl AuthoredClaim {
    #[must_use]
    pub(crate) fn new(generation: SealGeneration) -> Self {
        Self {
            generation,
            owner: OwnerToken::from(generation),
        }
    }

    #[must_use]
    pub const fn generation(&self) -> SealGeneration {
        self.generation
    }

    #[must_use]
    pub const fn owner(&self) -> OwnerToken {
        self.owner
    }
}

#[derive(Debug)]
enum CheckpointMarker {
    Preparing {
        ticket: CheckpointTicket,
    },
    Cancelling {
        ticket: CheckpointTicket,
    },
    Publishing {
        ticket: CheckpointTicket,
        prepared: Box<PreparedCheckpoint>,
    },
}

#[derive(Debug)]
pub struct WriterMachine {
    started: bool,
    claim: AuthoredClaim,
    seal: Seal,
    base: Forest,
    admitted: Forest,
    durable: Forest,
    durable_batch: BatchId,
    next_batch: BatchId,
    pending: Vec<PendingCommand>,
    flight: Option<InFlight>,
    checkpoint_attempt: u64,
    last_checkpoint_attempted_cut: Option<BatchId>,
    retry_checkpoint_at: Option<BatchId>,
    checkpoint: Option<CheckpointMarker>,
    collector: Option<BatchId>,
    ingress_open: bool,
}

#[derive(Debug)]
struct PendingCommand {
    /// `None` when an idempotent reply inherits an earlier fact's barrier.
    fact: Option<OperationFact>,
    completion: Completion,
}

#[derive(Debug)]
struct InFlight {
    batch: BatchId,
    forest: Forest,
    replies: Vec<Completion>,
}

impl WriterMachine {
    /// Reconstitute the live writer after bootstrap proves the exact facts.
    ///
    /// # Panics
    ///
    /// Panics unless the recovery claim names its Seal and the durable head is
    /// at or beyond the Seal replay cut with a successor coordinate available.
    #[must_use]
    pub fn from_recovery(recovery: WriterRecovery) -> Self {
        let WriterRecovery {
            claim,
            seal,
            base,
            durable: forest,
            durable_batch,
        } = recovery;
        assert_eq!(
            claim.generation(),
            seal.generation(),
            "the recovered claim names the recovered Seal"
        );
        assert!(
            seal.replay().batch().is_none_or(|cut| durable_batch >= cut),
            "the durable WAL head never precedes the replay cut"
        );
        let next_batch = durable_batch
            .successor()
            .expect("bootstrap proves a successor coordinate after its durable FENCE");
        Self {
            started: false,
            claim,
            seal,
            base,
            admitted: forest.clone(),
            durable: forest,
            durable_batch,
            next_batch,
            pending: Vec::with_capacity(WAL_RUN_FACTS_MAX),
            flight: None,
            checkpoint_attempt: 0,
            last_checkpoint_attempted_cut: None,
            retry_checkpoint_at: None,
            checkpoint: None,
            collector: None,
            ingress_open: true,
        }
    }

    #[must_use]
    pub const fn partition(&self) -> PartitionId {
        self.seal.partition()
    }

    #[must_use]
    pub const fn durable_forest(&self) -> &Forest {
        &self.durable
    }

    /// Apply one observation and return all ordered work it causes.
    ///
    /// # Panics
    ///
    /// Panics when the event violates an issued-effect identity, budget, or
    /// machine-state invariant.
    pub fn handle(&mut self, event: WriterEvent) -> WriterStep {
        let is_start = matches!(&event, WriterEvent::Started);
        assert_eq!(
            !self.started, is_start,
            "the writer machine starts exactly once before receiving observations"
        );
        if is_start {
            self.started = true;
        }
        let (mut outputs, exit) = self.apply_event(event);
        let exit = match exit {
            Some(exit) => Some(exit),
            None => self.schedule(&mut outputs),
        };
        assert!(
            outputs.len() <= WRITER_OUTPUTS_PER_STEP_MAX,
            "one writer event stays inside the named output bound"
        );
        if exit.is_some() {
            assert!(
                outputs
                    .iter()
                    .all(|output| matches!(output, WriterOutput::Action(_))),
                "a terminal writer step contains immediate actions only"
            );
        }
        WriterStep { outputs, exit }
    }

    fn apply_event(&mut self, event: WriterEvent) -> (Vec<WriterOutput>, Option<WriterExit>) {
        let mut outputs = Vec::new();
        let exit = match event {
            WriterEvent::Started => None,
            WriterEvent::Command(envelope) => {
                assert!(
                    self.ingress_open,
                    "commands arrive only while ingress is open"
                );
                if let Some(completion) = self.admit(envelope) {
                    outputs.push(WriterOutput::Action(WriterAction::SendReplies(vec![
                        completion,
                    ])));
                }
                None
            }
            WriterEvent::IngressClosed => {
                assert!(self.ingress_open, "ingress closes exactly once");
                self.ingress_open = false;
                match self.checkpoint.take() {
                    Some(CheckpointMarker::Preparing { ticket }) => {
                        self.checkpoint = Some(CheckpointMarker::Cancelling { ticket });
                        outputs.push(WriterOutput::Action(
                            WriterAction::CancelCheckpointPreparation { ticket },
                        ));
                    }
                    marker @ Some(
                        CheckpointMarker::Cancelling { .. } | CheckpointMarker::Publishing { .. },
                    ) => {
                        self.checkpoint = marker;
                    }
                    None => {}
                }
                None
            }
            WriterEvent::WalEstablished { batch, result } => match result {
                Ok(WalEstablishment::Durable) => {
                    self.record_wal_durable(batch, &mut outputs);
                    None
                }
                Ok(WalEstablishment::Occupied) => {
                    self.discard_wal_flight(batch);
                    Some(WriterExit::Fenced { batch })
                }
                Ok(WalEstablishment::UnresolvedAbsent) => {
                    self.discard_wal_flight(batch);
                    Some(WriterExit::Poisoned {
                        batch,
                        detail: "unresolved WAL create is absent on its one reconciliation read"
                            .into(),
                    })
                }
                Err(
                    TypedStoreError::Retryable { detail } | TypedStoreError::Rejected { detail },
                ) => {
                    self.discard_wal_flight(batch);
                    Some(WriterExit::Poisoned { batch, detail })
                }
                Err(TypedStoreError::Contradiction { detail }) => {
                    self.discard_wal_flight(batch);
                    Some(WriterExit::Contradiction { batch, detail })
                }
            },
            WriterEvent::CheckpointPrepared { ticket, outcome } => {
                self.complete_preparation(ticket, outcome, &mut outputs)
            }
            WriterEvent::SealPublished { ticket, result } => {
                self.complete_publication(ticket, result, &mut outputs)
            }
            WriterEvent::CollectFinished { cut } => {
                assert_eq!(
                    Some(cut),
                    self.collector,
                    "collection completion names the exact issued cut"
                );
                self.collector = None;
                None
            }
            WriterEvent::CheckpointPreparationCancelled { ticket } => {
                self.assert_checkpoint_cancelling(ticket);
                self.checkpoint = None;
                None
            }
        };
        (outputs, exit)
    }

    #[expect(
        clippy::panic,
        clippy::unwrap_in_result,
        reason = "issued-effect identity violations are process-local protocol invariants"
    )]
    fn complete_preparation(
        &mut self,
        ticket: CheckpointTicket,
        outcome: PreparationOutcome,
        outputs: &mut Vec<WriterOutput>,
    ) -> Option<WriterExit> {
        let marker = self
            .checkpoint
            .as_ref()
            .expect("checkpoint preparation completion has an issued marker");
        match marker {
            CheckpointMarker::Cancelling { ticket: active } => {
                assert_eq!(
                    &ticket, active,
                    "cancelled preparation completion names the exact issued ticket"
                );
                self.checkpoint = None;
                drop(outcome);
                None
            }
            CheckpointMarker::Preparing { ticket: active } => {
                assert_eq!(
                    &ticket, active,
                    "checkpoint preparation completion names the exact issued ticket"
                );
                self.checkpoint = None;
                match outcome {
                    PreparationOutcome::Prepared(prepared) => {
                        let candidate = prepared.candidate.clone();
                        self.checkpoint = Some(CheckpointMarker::Publishing { ticket, prepared });
                        outputs.push(WriterOutput::Effect(WriterEffect::PublishAuthority {
                            ticket,
                            candidate,
                        }));
                        None
                    }
                    PreparationOutcome::Abandoned => None,
                    PreparationOutcome::Contradiction { detail } => {
                        Some(WriterExit::Contradiction {
                            batch: ticket.cut,
                            detail,
                        })
                    }
                }
            }
            CheckpointMarker::Publishing {
                ticket: _,
                prepared: _,
            } => panic!("preparation cannot complete after publication was issued"),
        }
    }

    #[expect(
        clippy::panic,
        clippy::unwrap_in_result,
        reason = "issued-effect identity violations are process-local protocol invariants"
    )]
    fn complete_publication(
        &mut self,
        ticket: CheckpointTicket,
        result: Result<SealPublication, TypedStoreError>,
        outputs: &mut Vec<WriterOutput>,
    ) -> Option<WriterExit> {
        let marker = self
            .checkpoint
            .as_ref()
            .expect("Seal publication completion has an issued marker");
        let CheckpointMarker::Publishing { ticket: active, .. } = marker else {
            panic!("Seal publication completes only while its checkpoint is publishing");
        };
        assert_eq!(
            &ticket, active,
            "Seal publication completion names the exact issued ticket"
        );
        let Some(CheckpointMarker::Publishing {
            ticket: active,
            prepared,
        }) = self.checkpoint.take()
        else {
            panic!("the validated publication marker remains present");
        };
        assert_eq!(
            ticket, active,
            "the consumed publication marker retains the validated ticket"
        );
        match result {
            Ok(SealPublication::Authored) => {
                let PreparedCheckpoint {
                    source,
                    successor,
                    snapshot,
                    candidate: _,
                } = *prepared;
                self.install_prepared(ticket, &source, &successor, snapshot);
                outputs.push(WriterOutput::Action(WriterAction::PublishView(
                    self.durable.clone(),
                )));
                if self.ingress_open && self.collector.is_none() {
                    self.collector = Some(ticket.cut);
                    outputs.push(WriterOutput::Effect(WriterEffect::Collect(
                        CollectionInput::new(ticket.cut, source, successor),
                    )));
                }
                None
            }
            Ok(SealPublication::NoAuthority) => Some(WriterExit::Fenced { batch: ticket.cut }),
            Ok(SealPublication::Unresolved) => Some(WriterExit::Poisoned {
                batch: ticket.cut,
                detail: "advancing Seal create is unresolved".into(),
            }),
            Err(TypedStoreError::Retryable { detail } | TypedStoreError::Rejected { detail }) => {
                Some(WriterExit::Poisoned {
                    batch: ticket.cut,
                    detail,
                })
            }
            Err(TypedStoreError::Contradiction { detail }) => Some(WriterExit::Contradiction {
                batch: ticket.cut,
                detail,
            }),
        }
    }

    fn schedule(&mut self, outputs: &mut Vec<WriterOutput>) -> Option<WriterExit> {
        if let Some(output) = self.take_flight() {
            outputs.push(output);
        }

        if self.ingress_open {
            if let Some(input) = self.take_checkpoint() {
                outputs.push(WriterOutput::Effect(WriterEffect::PrepareCheckpoint(input)));
            }
            None
        } else if self.flight.is_none() && self.checkpoint.is_none() && self.pending.is_empty() {
            self.assert_quiescent();
            Some(WriterExit::Shutdown)
        } else {
            None
        }
    }

    fn admit(&mut self, envelope: CommandEnvelope) -> Option<Completion> {
        if self.flight.is_none() {
            self.assert_quiescent();
        }
        if self.pending.len() == WAL_RUN_FACTS_MAX {
            return Some(envelope.refusal(AdmissionRefusal::Overloaded));
        }

        let batch = self.next_batch;
        let decision = match envelope {
            CommandEnvelope::Create { command, reply } => {
                match admit_create(&self.admitted, &command, batch) {
                    Ok(CreateAdmission::Fact(admitted)) => Admission::Fact {
                        admitted,
                        completion: Completion::Create {
                            outcome: Ok(CreateOutcome::Created),
                            reply,
                        },
                    },
                    Ok(CreateAdmission::AlreadyExists) => {
                        Admission::Idempotent(Completion::Create {
                            outcome: Ok(CreateOutcome::AlreadyExists),
                            reply,
                        })
                    }
                    Err(refusal) => Admission::Refused(Completion::Create {
                        outcome: Err(refusal),
                        reply,
                    }),
                }
            }
            CommandEnvelope::Close { path, reply } => {
                match admit_close(&self.admitted, &path, batch) {
                    Ok(CloseAdmission::Fact(admitted)) => Admission::Fact {
                        admitted,
                        completion: Completion::Close {
                            outcome: Ok(CloseStreamOutcome::Closed),
                            reply,
                        },
                    },
                    Ok(CloseAdmission::AlreadyClosed) => Admission::Idempotent(Completion::Close {
                        outcome: Ok(CloseStreamOutcome::AlreadyClosed),
                        reply,
                    }),
                    Err(refusal) => Admission::Refused(Completion::Close {
                        outcome: Err(refusal),
                        reply,
                    }),
                }
            }
            CommandEnvelope::Delete { path, reply } => {
                match admit_delete(&self.admitted, &path, batch) {
                    Ok(admitted) => Admission::Fact {
                        admitted,
                        completion: Completion::Delete {
                            outcome: Ok(()),
                            reply,
                        },
                    },
                    Err(refusal) => Admission::Refused(Completion::Delete {
                        outcome: Err(refusal),
                        reply,
                    }),
                }
            }
        };

        match decision {
            Admission::Fact {
                admitted,
                completion,
            } => self.accept_fact(admitted, completion),
            Admission::Idempotent(completion) => self.accept_idempotent(completion),
            Admission::Refused(completion) => Some(completion),
        }
    }

    fn take_flight(&mut self) -> Option<WriterOutput> {
        if self.flight.is_some() {
            return None;
        }
        if self.pending.is_empty() {
            self.assert_quiescent();
            return None;
        }
        let commands = std::mem::replace(&mut self.pending, Vec::with_capacity(WAL_RUN_FACTS_MAX));
        let mut facts = Vec::new();
        let mut replies = Vec::with_capacity(commands.len());
        for PendingCommand { fact, completion } in commands {
            if let Some(fact) = fact {
                facts.push(fact);
            }
            replies.push(completion);
        }
        if facts.is_empty() {
            assert_eq!(
                self.admitted, self.durable,
                "an all-idempotent barrier leaves admitted and durable state equal"
            );
            self.admitted = self.durable.clone();
            return Some(WriterOutput::Action(WriterAction::SendReplies(replies)));
        }

        let batch = self.next_batch;
        let encoded = EncodedWal::new(&WalObject::new(
            self.partition(),
            batch,
            self.claim.owner(),
            WalBody::Run(pending_facts(facts)),
        ))
        .expect("the fact-count and field bounds prove every pending RUN fits the WAL byte bound");
        self.next_batch = batch
            .successor()
            .expect("the suffix reserve proves a coordinate after every admitted RUN");
        self.flight = Some(InFlight {
            batch,
            forest: self.admitted.clone(),
            replies,
        });
        Some(WriterOutput::Effect(WriterEffect::EstablishWal(encoded)))
    }

    fn record_wal_durable(&mut self, batch: BatchId, outputs: &mut Vec<WriterOutput>) {
        let active = self
            .flight
            .as_ref()
            .expect("WAL durability is recorded only for an active flight");
        assert_eq!(
            batch, active.batch,
            "WAL durability names the active flight's batch"
        );
        let InFlight {
            batch: _,
            forest,
            replies,
        } = self
            .flight
            .take()
            .expect("the validated WAL flight remains active");
        self.durable = forest;
        self.durable_batch = batch;
        if self.pending.is_empty() {
            assert_eq!(
                self.admitted, self.durable,
                "WAL durability makes admitted and durable state equal at quiescence"
            );
            self.admitted = self.durable.clone();
        }
        outputs.push(WriterOutput::Action(WriterAction::PublishView(
            self.durable.clone(),
        )));
        outputs.push(WriterOutput::Action(WriterAction::SendReplies(replies)));
    }

    fn discard_wal_flight(&mut self, batch: BatchId) {
        let active = self
            .flight
            .as_ref()
            .expect("terminal WAL teardown observes an active flight");
        assert_eq!(
            batch, active.batch,
            "terminal WAL teardown names the active flight's batch"
        );
        drop(self.flight.take());
    }

    #[expect(
        clippy::unwrap_in_result,
        reason = "checkpoint attempt exhaustion is a process-local invariant, not absence"
    )]
    fn take_checkpoint(&mut self) -> Option<CheckpointInput> {
        if self.checkpoint.is_some()
            || suffix_span(self.seal.replay(), self.durable_batch)
                < WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER
        {
            return None;
        }
        let head_is_unattempted = match self.last_checkpoint_attempted_cut {
            None => true,
            Some(attempted) => {
                assert!(
                    attempted <= self.durable_batch,
                    "a checkpoint attempt never names a future durable cut"
                );
                attempted < self.durable_batch
            }
        };
        if !head_is_unattempted && self.retry_checkpoint_at != Some(self.durable_batch) {
            return None;
        }
        assert!(
            self.seal
                .replay()
                .batch()
                .is_none_or(|cut| self.durable_batch > cut),
            "a checkpoint plan names an advancing durable cut"
        );

        let attempt = AttemptId::new(self.claim.generation(), self.checkpoint_attempt);
        self.checkpoint_attempt = self
            .checkpoint_attempt
            .checked_add(1)
            .expect("the process-local checkpoint attempt counter is not exhausted");
        let ticket = CheckpointTicket::new(self.seal.generation(), self.durable_batch, attempt);
        let input = CheckpointInput::new(
            ticket,
            self.seal.clone(),
            self.base.clone(),
            self.durable.clone(),
        );
        self.last_checkpoint_attempted_cut = Some(self.durable_batch);
        self.retry_checkpoint_at = None;
        self.checkpoint = Some(CheckpointMarker::Preparing { ticket });
        Some(input)
    }

    fn install_prepared(
        &mut self,
        ticket: CheckpointTicket,
        source: &Seal,
        successor: &Seal,
        snapshot: Forest,
    ) {
        assert_eq!(
            ticket.source,
            source.generation(),
            "a checkpoint install advances its planned source Seal"
        );
        assert_eq!(
            &self.seal, source,
            "a checkpoint install returns its exact planned source Seal"
        );
        assert_eq!(
            self.partition(),
            successor.partition(),
            "a checkpoint successor retains the writer partition"
        );
        assert_eq!(
            source.generation().successor(),
            Ok(successor.generation()),
            "a checkpoint successor is one exact Seal generation"
        );
        assert_eq!(
            WalReplayPoint::Through {
                batch: ticket.cut,
                owner: self.claim.owner(),
            },
            successor.replay(),
            "a checkpoint install publishes its planned WAL cut and owner"
        );
        self.seal = successor.clone();
        self.base = snapshot;
    }

    fn accept_fact(
        &mut self,
        admitted: AdmittedCommand,
        completion: Completion,
    ) -> Option<Completion> {
        if !decide_suffix_room(self.seal.replay().batch(), self.next_batch) {
            self.retry_checkpoint_at = Some(self.durable_batch);
            return Some(completion.refusal(AdmissionRefusal::Overloaded));
        }
        self.admitted = admitted.forest;
        self.pending.push(PendingCommand {
            fact: Some(admitted.fact),
            completion,
        });
        None
    }

    fn accept_idempotent(&mut self, completion: Completion) -> Option<Completion> {
        if self.flight.is_none() {
            Some(completion)
        } else {
            self.pending.push(PendingCommand {
                fact: None,
                completion,
            });
            None
        }
    }

    #[expect(
        clippy::panic,
        reason = "a cancellation completion outside Cancelling violates the machine protocol"
    )]
    fn assert_checkpoint_cancelling(&self, ticket: CheckpointTicket) {
        let Some(CheckpointMarker::Cancelling { ticket: active }) = &self.checkpoint else {
            panic!("checkpoint preparation cancellation completes only while cancelling");
        };
        assert_eq!(
            ticket, *active,
            "checkpoint cancellation completion names the exact issued ticket"
        );
    }

    fn assert_quiescent(&self) {
        assert!(
            self.flight.is_none() && self.pending.is_empty(),
            "quiescent validation observes no flight or pending barrier"
        );
        assert!(
            self.admitted.shares_roots_with(&self.durable),
            "no flight and empty pending shares admitted and durable forest roots"
        );
    }
}

impl CommandEnvelope {
    fn refusal(self, refusal: AdmissionRefusal) -> Completion {
        match self {
            Self::Create { command: _, reply } => Completion::Create {
                outcome: Err(refusal),
                reply,
            },
            Self::Close { path: _, reply } => Completion::Close {
                outcome: Err(refusal),
                reply,
            },
            Self::Delete { path: _, reply } => Completion::Delete {
                outcome: Err(refusal),
                reply,
            },
        }
    }
}

impl Completion {
    fn refusal(self, refusal: AdmissionRefusal) -> Self {
        match self {
            Self::Create { outcome: _, reply } => Self::Create {
                outcome: Err(refusal),
                reply,
            },
            Self::Close { outcome: _, reply } => Self::Close {
                outcome: Err(refusal),
                reply,
            },
            Self::Delete { outcome: _, reply } => Self::Delete {
                outcome: Err(refusal),
                reply,
            },
        }
    }
}

enum Admission {
    Fact {
        admitted: AdmittedCommand,
        completion: Completion,
    },
    Idempotent(Completion),
    Refused(Completion),
}

struct AdmittedCommand {
    forest: Forest,
    fact: OperationFact,
}

enum CreateAdmission {
    Fact(AdmittedCommand),
    AlreadyExists,
}

enum CloseAdmission {
    Fact(AdmittedCommand),
    AlreadyClosed,
}

fn admit_create(
    admitted: &Forest,
    command: &CreateStream,
    batch: BatchId,
) -> Result<CreateAdmission, AdmissionRefusal> {
    match admitted.resolve(&command.path) {
        Some(DirectoryEntry::Tombstone(_)) => Err(AdmissionRefusal::PathOccupied),
        Some(DirectoryEntry::Live(uid)) => {
            let record = admitted
                .record(uid)
                .expect("a Live directory row has exactly one Ledger record");
            if record.content_type() == &command.content_type
                && record.expiry() == command.expiry
                && record.lifecycle() == command.lifecycle
            {
                Ok(CreateAdmission::AlreadyExists)
            } else {
                Err(AdmissionRefusal::PathOccupied)
            }
        }
        None => {
            let uid = admitted.successor_uid().map_err(|contradiction| {
                contradiction
                    .admission_refusal()
                    .expect("successor allocation only returns a caller-facing capacity refusal")
            })?;
            apply_fact(
                admitted,
                batch,
                OperationFact::StreamCreated {
                    path: command.path.clone(),
                    uid,
                    content_type: command.content_type.clone(),
                    expiry: command.expiry,
                    lifecycle: command.lifecycle,
                },
            )
            .map(CreateAdmission::Fact)
        }
    }
}

fn admit_close(
    admitted: &Forest,
    path: &DirectoryKey,
    batch: BatchId,
) -> Result<CloseAdmission, AdmissionRefusal> {
    let uid = resolve_live_uid(admitted, path)?;
    let record = admitted
        .record(uid)
        .expect("a Live directory row has exactly one Ledger record");
    if record.lifecycle().is_closed() {
        return Ok(CloseAdmission::AlreadyClosed);
    }
    apply_fact(
        admitted,
        batch,
        OperationFact::StreamClosed {
            path: path.clone(),
            uid,
        },
    )
    .map(CloseAdmission::Fact)
}

fn admit_delete(
    admitted: &Forest,
    path: &DirectoryKey,
    batch: BatchId,
) -> Result<AdmittedCommand, AdmissionRefusal> {
    apply_fact(
        admitted,
        batch,
        OperationFact::StreamDeleted {
            path: path.clone(),
            uid: resolve_live_uid(admitted, path)?,
        },
    )
}

impl FoldContradiction {
    const fn admission_refusal(self) -> Option<AdmissionRefusal> {
        match self {
            Self::PathOccupied => Some(AdmissionRefusal::PathOccupied),
            Self::PathCapacityExhausted => Some(AdmissionRefusal::PathCapacityExhausted),
            Self::PathNotLive => Some(AdmissionRefusal::PathNotLive),
            Self::StreamAlreadyClosed | Self::UidNotDenseSuccessor | Self::PathUidMismatch => None,
        }
    }
}

fn resolve_live_uid(
    forest: &Forest,
    path: &DirectoryKey,
) -> Result<strom_storage_domain::StreamUid, AdmissionRefusal> {
    match forest.resolve(path) {
        Some(DirectoryEntry::Live(uid)) => Ok(uid),
        Some(DirectoryEntry::Tombstone(_)) | None => Err(AdmissionRefusal::PathNotLive),
    }
}

fn apply_fact(
    admitted: &Forest,
    batch: BatchId,
    fact: OperationFact,
) -> Result<AdmittedCommand, AdmissionRefusal> {
    let mut candidate = admitted.clone();
    match candidate.strict_fold(batch, &fact) {
        Ok(Applied) => Ok(AdmittedCommand {
            forest: candidate,
            fact,
        }),
        Err(contradiction) => Err(contradiction
            .admission_refusal()
            .expect("admission constructs the dense uid and exact path uid carried by its fact")),
    }
}

#[must_use]
fn decide_suffix_room(cut: Option<BatchId>, proposed: BatchId) -> bool {
    let cut = cut.map_or(0, BatchId::get);
    proposed
        .successor()
        .ok()
        .and_then(|reserved_fence| reserved_fence.get().checked_sub(cut))
        .is_some_and(|span| span > 1 && span <= WAL_SUFFIX_COORDINATES_MAX_V2)
}

const fn suffix_span(replay: WalReplayPoint, durable_batch: BatchId) -> u64 {
    let cut = match replay.batch() {
        None => 0,
        Some(batch) => batch.get(),
    };
    durable_batch
        .get()
        .checked_sub(cut)
        .expect("the durable WAL head never precedes the replay cut")
}

fn pending_facts(facts: Vec<OperationFact>) -> WalFacts {
    WalFacts::try_from(facts)
        .expect("pending RUN construction enforces nonempty and fact-count bounds")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suffix_room_reserves_one_takeover_coordinate() -> Result<(), Box<dyn std::error::Error>> {
        let last_genesis_run = BatchId::try_from(WAL_SUFFIX_COORDINATES_MAX_V2 - 1)?;
        assert!(decide_suffix_room(None, last_genesis_run));
        assert!(!decide_suffix_room(None, last_genesis_run.successor()?));

        let cut = BatchId::try_from(u64::MAX - 2)?;
        assert!(decide_suffix_room(
            Some(cut),
            BatchId::try_from(u64::MAX - 1)?
        ));
        assert!(!decide_suffix_room(Some(cut), BatchId::try_from(u64::MAX)?));
        Ok(())
    }
}
