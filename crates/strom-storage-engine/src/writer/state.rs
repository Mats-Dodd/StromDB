//! Pure admission-to-durability writer protocol.

use strom_domain::{
    CloseStreamOutcome, CreateOutcome, ExpiryPolicy, StreamContentType, StreamLifecycle,
};
use strom_storage_domain::{
    AttemptId, BatchId, DirectoryEntry, DirectoryKey, OperationFact, PartitionId, Seal,
    SealGeneration, WAL_RUN_FACTS_MAX, WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER,
    WAL_SUFFIX_COORDINATES_MAX_V2, WalBody, WalFacts, WalObject, WalReplayPoint,
};
use tokio::sync::oneshot;

use crate::bootstrap::AuthoredClaim;
use crate::checkpoint::{CheckpointInput, CheckpointInstall};
use crate::store::EncodedWal;
use crate::{Applied, FoldContradiction, Forest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateStream {
    pub(crate) path: DirectoryKey,
    pub(crate) content_type: StreamContentType,
    pub(crate) expiry: ExpiryPolicy,
    pub(crate) lifecycle: StreamLifecycle,
}

/// A command that did not enter admitted state or consume a WAL coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum AdmissionRefusal {
    #[error("stream path is already occupied")]
    PathOccupied,
    #[error("partition path capacity is exhausted")]
    PathCapacityExhausted,
    #[error("stream path is not live")]
    PathNotLive,
    #[error("partition writer is at a bounded capacity limit")]
    Overloaded,
}

pub(crate) enum CommandEnvelope {
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
pub(super) enum Completion {
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

pub(super) enum AdmissionDecision {
    Settled(Completion),
    Queued,
}

pub(super) enum FlightDecision {
    Run(EncodedWal),
    Replies(Vec<Completion>),
    Idle,
}

pub(super) struct DurableWal {
    pub(super) replies: Vec<Completion>,
    pub(super) forest: Forest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CheckpointTicket {
    pub(super) source: SealGeneration,
    pub(super) cut: BatchId,
    pub(super) attempt: AttemptId,
}

pub(super) struct CheckpointPlan {
    pub(super) input: CheckpointInput,
    pub(super) ticket: CheckpointTicket,
}

pub(super) struct InstalledCheckpoint {
    pub(super) source: Seal,
    pub(super) successor: Seal,
    pub(super) forest: Forest,
}

#[derive(Debug)]
pub(crate) struct WriterState {
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
    active_checkpoint: Option<CheckpointTicket>,
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
    commands: Vec<PendingCommand>,
}

impl WriterState {
    pub(crate) fn new(
        claim: AuthoredClaim,
        seal: Seal,
        base: Forest,
        forest: Forest,
        durable_batch: BatchId,
        next_batch: BatchId,
    ) -> Self {
        assert_eq!(
            Ok(next_batch),
            durable_batch.successor(),
            "next_batch is the exact successor of the durable WAL head"
        );
        assert!(
            replay_batch(seal.replay()).is_none_or(|cut| durable_batch >= cut),
            "the durable WAL head never precedes the replay cut"
        );
        Self {
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
            active_checkpoint: None,
        }
    }

    pub(crate) const fn partition(&self) -> PartitionId {
        self.seal.partition()
    }

    pub(crate) const fn durable_forest(&self) -> &Forest {
        &self.durable
    }

    pub(super) const fn has_flight(&self) -> bool {
        self.flight.is_some()
    }

    pub(super) const fn active_checkpoint(&self) -> Option<CheckpointTicket> {
        self.active_checkpoint
    }

    pub(super) fn is_quiescent(&self) -> bool {
        let quiescent = self.flight.is_none() && self.pending.is_empty();
        if quiescent {
            self.assert_quiescent();
        }
        quiescent
    }

    pub(super) fn admit(&mut self, envelope: CommandEnvelope) -> AdmissionDecision {
        if self.flight.is_none() {
            self.assert_quiescent();
        }
        if self.pending.len() == WAL_RUN_FACTS_MAX {
            return AdmissionDecision::Settled(envelope.refusal(AdmissionRefusal::Overloaded));
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
            Admission::Refused(completion) => AdmissionDecision::Settled(completion),
        }
    }

    pub(super) fn take_flight(&mut self) -> FlightDecision {
        if self.flight.is_some() {
            return FlightDecision::Idle;
        }
        if self.pending.is_empty() {
            self.assert_quiescent();
            return FlightDecision::Idle;
        }
        let commands = std::mem::replace(&mut self.pending, Vec::with_capacity(WAL_RUN_FACTS_MAX));
        let facts: Vec<OperationFact> = commands
            .iter()
            .filter_map(|command| command.fact.clone())
            .collect();
        if facts.is_empty() {
            assert_eq!(
                self.admitted, self.durable,
                "an all-idempotent barrier leaves admitted and durable state equal"
            );
            self.admitted = self.durable.clone();
            return FlightDecision::Replies(completions(commands));
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
        self.flight = Some(InFlight { batch, commands });
        FlightDecision::Run(encoded)
    }

    pub(super) fn record_wal_durable(&mut self, batch: BatchId) -> DurableWal {
        let active = self
            .flight
            .as_ref()
            .expect("WAL durability is recorded only for an active flight");
        assert_eq!(
            batch, active.batch,
            "WAL durability names the active flight's batch"
        );
        let flight = self
            .flight
            .take()
            .expect("the validated WAL flight remains active");
        for command in &flight.commands {
            let Some(fact) = &command.fact else {
                continue;
            };
            assert_eq!(
                Ok(Applied),
                self.durable.strict_fold(batch, fact),
                "durable fold repeats facts already proven against admitted state"
            );
        }
        self.durable_batch = batch;
        if self.pending.is_empty() {
            assert_eq!(
                self.admitted, self.durable,
                "WAL durability makes admitted and durable state equal at quiescence"
            );
            self.admitted = self.durable.clone();
        }
        DurableWal {
            replies: completions(flight.commands),
            forest: self.durable.clone(),
        }
    }

    pub(super) fn discard_wal_flight(&mut self, batch: BatchId) {
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
    pub(super) fn take_checkpoint(&mut self) -> Option<CheckpointPlan> {
        if self.active_checkpoint.is_some()
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
            replay_batch(self.seal.replay()).is_none_or(|cut| self.durable_batch > cut),
            "a checkpoint plan names an advancing durable cut"
        );

        let attempt = AttemptId::new(self.claim.generation(), self.checkpoint_attempt);
        self.checkpoint_attempt = self
            .checkpoint_attempt
            .checked_add(1)
            .expect("the process-local checkpoint attempt counter is not exhausted");
        let ticket = CheckpointTicket {
            source: self.seal.generation(),
            cut: self.durable_batch,
            attempt,
        };
        let input = CheckpointInput {
            source: self.seal.clone(),
            base: self.base.clone(),
            snapshot: self.durable.clone(),
            cut: self.durable_batch,
            attempt,
        };
        self.active_checkpoint = Some(ticket);
        self.last_checkpoint_attempted_cut = Some(self.durable_batch);
        self.retry_checkpoint_at = None;
        Some(CheckpointPlan { input, ticket })
    }

    pub(super) fn abandon_checkpoint(&mut self, ticket: CheckpointTicket) {
        self.take_active_checkpoint(ticket);
    }

    pub(super) fn install_checkpoint(
        &mut self,
        ticket: CheckpointTicket,
        install: CheckpointInstall,
    ) -> InstalledCheckpoint {
        self.assert_active_checkpoint(ticket);
        assert_eq!(
            ticket.source,
            install.source.generation(),
            "a checkpoint install advances its planned source Seal"
        );
        assert_eq!(
            &self.seal, &install.source,
            "a checkpoint install returns its exact planned source Seal"
        );
        assert_eq!(
            self.partition(),
            install.successor.partition(),
            "a checkpoint successor retains the writer partition"
        );
        assert_eq!(
            install.source.generation().successor(),
            Ok(install.successor.generation()),
            "a checkpoint successor is one exact Seal generation"
        );
        assert_eq!(
            WalReplayPoint::Through {
                batch: ticket.cut,
                owner: self.claim.owner(),
            },
            install.successor.replay(),
            "a checkpoint install publishes its planned WAL cut and owner"
        );
        self.active_checkpoint = None;
        let CheckpointInstall {
            source,
            successor,
            snapshot,
        } = install;
        self.seal = successor.clone();
        self.base = snapshot;
        InstalledCheckpoint {
            source,
            successor,
            forest: self.durable.clone(),
        }
    }

    fn accept_fact(
        &mut self,
        admitted: AdmittedCommand,
        completion: Completion,
    ) -> AdmissionDecision {
        if !decide_suffix_room(replay_batch(self.seal.replay()), self.next_batch) {
            self.retry_checkpoint_at = Some(self.durable_batch);
            return AdmissionDecision::Settled(completion.refusal(AdmissionRefusal::Overloaded));
        }
        self.admitted = admitted.forest;
        self.pending.push(PendingCommand {
            fact: Some(admitted.fact),
            completion,
        });
        AdmissionDecision::Queued
    }

    fn accept_idempotent(&mut self, completion: Completion) -> AdmissionDecision {
        if self.flight.is_none() {
            AdmissionDecision::Settled(completion)
        } else {
            self.pending.push(PendingCommand {
                fact: None,
                completion,
            });
            AdmissionDecision::Queued
        }
    }

    fn take_active_checkpoint(&mut self, ticket: CheckpointTicket) {
        self.assert_active_checkpoint(ticket);
        self.active_checkpoint = None;
    }

    fn assert_active_checkpoint(&self, ticket: CheckpointTicket) {
        let active = self
            .active_checkpoint
            .as_ref()
            .expect("checkpoint completion returns an active ticket");
        assert_eq!(
            &ticket, active,
            "checkpoint completion returns its exact source, cut, and attempt"
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

const fn replay_batch(replay: WalReplayPoint) -> Option<BatchId> {
    match replay {
        WalReplayPoint::Genesis => None,
        WalReplayPoint::Through { batch, owner: _ } => Some(batch),
    }
}

const fn suffix_span(replay: WalReplayPoint, durable_batch: BatchId) -> u64 {
    let cut = match replay {
        WalReplayPoint::Genesis => 0,
        WalReplayPoint::Through { batch, owner: _ } => batch.get(),
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

fn completions(commands: Vec<PendingCommand>) -> Vec<Completion> {
    commands
        .into_iter()
        .map(|command| command.completion)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use strom_domain::StreamTtl;
    use strom_storage_domain::{OwnerToken, StreamUid, TreeVersion};

    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;
    type CreateReply = oneshot::Receiver<Result<CreateOutcome, AdmissionRefusal>>;
    type CreateEnvelope = (CommandEnvelope, CreateReply);
    const WRONG_CHECKPOINT_ATTEMPT: u64 = 99;

    #[test]
    fn durability_releases_replies_in_admission_order_and_publishes_the_fold() -> TestResult {
        let mut state = state_at(1, Forest::empty())?;
        let (first, mut first_outcome) = create("events/first")?;
        assert!(matches!(state.admit(first), AdmissionDecision::Queued));
        let first_batch = run_batch(state.take_flight())?;

        let (second, mut second_outcome) = create("events/second")?;
        assert!(matches!(state.admit(second), AdmissionDecision::Queued));
        assert!(matches!(
            first_outcome.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        let durable = state.record_wal_durable(first_batch);
        send_batch(durable.replies);
        assert_eq!(Ok(CreateOutcome::Created), first_outcome.try_recv()?);
        assert!(matches!(
            second_outcome.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        let second_batch = run_batch(state.take_flight())?;
        let durable = state.record_wal_durable(second_batch);
        assert_eq!(&durable.forest, state.durable_forest());
        send_batch(durable.replies);
        assert_eq!(Ok(CreateOutcome::Created), second_outcome.try_recv()?);
        assert!(state.is_quiescent());
        Ok(())
    }

    #[test]
    fn duplicate_behind_a_flight_waits_but_without_a_flight_settles() -> TestResult {
        let mut state = state_at(1, Forest::empty())?;
        let (first_create, mut created) = create("events/duplicate")?;
        assert!(matches!(
            state.admit(first_create),
            AdmissionDecision::Queued
        ));
        let batch = run_batch(state.take_flight())?;

        let (duplicate, mut duplicate_outcome) = create("events/duplicate")?;
        assert!(matches!(state.admit(duplicate), AdmissionDecision::Queued));
        assert!(matches!(
            duplicate_outcome.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        send_batch(state.record_wal_durable(batch).replies);
        assert_eq!(Ok(CreateOutcome::Created), created.try_recv()?);
        let FlightDecision::Replies(replies) = state.take_flight() else {
            return Err("the duplicate barrier produces replies without a WAL run".into());
        };
        send_batch(replies);
        assert_eq!(
            Ok(CreateOutcome::AlreadyExists),
            duplicate_outcome.try_recv()?
        );

        let (duplicate, mut immediate) = create("events/duplicate")?;
        settle(state.admit(duplicate))?;
        assert_eq!(Ok(CreateOutcome::AlreadyExists), immediate.try_recv()?);
        Ok(())
    }

    #[test]
    fn suffix_shed_changes_only_retry_accounting_and_idempotence_stays_answerable() -> TestResult {
        let path = path("events/existing")?;
        let mut forest = Forest::empty();
        assert_eq!(
            Ok(Applied),
            forest.strict_fold(
                BatchId::try_from(1)?,
                &OperationFact::StreamCreated {
                    path: path.clone(),
                    uid: StreamUid::try_from(1)?,
                    content_type: StreamContentType::octet_stream(),
                    expiry: ExpiryPolicy::None,
                    lifecycle: StreamLifecycle::Open,
                },
            )
        );
        let durable = BatchId::try_from(WAL_SUFFIX_COORDINATES_MAX_V2 - 1)?;
        let mut state = state_at(durable.get(), forest.clone())?;
        let admitted_before = state.admitted.clone();

        let (new_stream, mut refused) = create("events/refused")?;
        settle(state.admit(new_stream))?;
        assert_eq!(Err(AdmissionRefusal::Overloaded), refused.try_recv()?);
        assert_eq!(admitted_before, state.admitted);
        assert_eq!(Some(durable), state.retry_checkpoint_at);

        let (duplicate, mut duplicate_outcome) = create("events/existing")?;
        settle(state.admit(duplicate))?;
        assert_eq!(
            Ok(CreateOutcome::AlreadyExists),
            duplicate_outcome.try_recv()?
        );
        assert_eq!(forest, *state.durable_forest());
        Ok(())
    }

    #[test]
    fn all_idempotent_pending_produces_replies_without_consuming_a_coordinate() -> TestResult {
        let mut state = state_at(1, Forest::empty())?;
        let (first_create, mut created) = create("events/barrier")?;
        assert!(matches!(
            state.admit(first_create),
            AdmissionDecision::Queued
        ));
        let batch = run_batch(state.take_flight())?;
        let (duplicate, mut duplicate_outcome) = create("events/barrier")?;
        assert!(matches!(state.admit(duplicate), AdmissionDecision::Queued));
        send_batch(state.record_wal_durable(batch).replies);
        assert_eq!(Ok(CreateOutcome::Created), created.try_recv()?);
        let next_before = state.next_batch;

        let FlightDecision::Replies(replies) = state.take_flight() else {
            return Err("an all-idempotent pending set produces replies".into());
        };
        assert_eq!(next_before, state.next_batch);
        send_batch(replies);
        assert_eq!(
            Ok(CreateOutcome::AlreadyExists),
            duplicate_outcome.try_recv()?
        );
        Ok(())
    }

    #[test]
    fn quiescent_transitions_reject_divergent_admitted_state() -> TestResult {
        let mut barrier_state = state_at(1, Forest::empty())?;
        let (first_create, _created) = create("events/barrier-divergence")?;
        assert!(matches!(
            barrier_state.admit(first_create),
            AdmissionDecision::Queued
        ));
        let batch = run_batch(barrier_state.take_flight())?;
        let (duplicate, _duplicate_outcome) = create("events/barrier-divergence")?;
        assert!(matches!(
            barrier_state.admit(duplicate),
            AdmissionDecision::Queued
        ));
        drop(barrier_state.record_wal_durable(batch));
        barrier_state.admitted = Forest::empty();
        assert!(
            catch_unwind(AssertUnwindSafe(|| barrier_state.take_flight())).is_err(),
            "an all-idempotent barrier detects divergent admitted state"
        );

        let mut durable_state = state_at(1, Forest::empty())?;
        let (create, _outcome) = create("events/durability-divergence")?;
        assert!(matches!(
            durable_state.admit(create),
            AdmissionDecision::Queued
        ));
        let batch = run_batch(durable_state.take_flight())?;
        durable_state.admitted = Forest::empty();
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                durable_state.record_wal_durable(batch);
            }))
            .is_err(),
            "WAL durability detects divergent admitted state"
        );
        Ok(())
    }

    #[test]
    fn full_pending_barrier_refuses_before_admission() -> TestResult {
        let mut state = state_at(1, Forest::empty())?;
        let (first_create, _created) = create("events/full")?;
        assert!(matches!(
            state.admit(first_create),
            AdmissionDecision::Queued
        ));
        let _batch = run_batch(state.take_flight())?;
        let mut last = None;
        for _ in 0..WAL_RUN_FACTS_MAX {
            let (duplicate, outcome) = create("events/full")?;
            assert!(matches!(state.admit(duplicate), AdmissionDecision::Queued));
            last = Some(outcome);
        }
        let (overflow, mut overflow_outcome) = create("events/full")?;
        settle(state.admit(overflow))?;
        assert_eq!(
            Err(AdmissionRefusal::Overloaded),
            overflow_outcome.try_recv()?
        );
        assert!(last.is_some());
        assert_eq!(WAL_RUN_FACTS_MAX, state.pending.len());
        Ok(())
    }

    #[test]
    fn checkpoint_planning_suppresses_attempts_and_retry_rearms_the_same_cut() -> TestResult {
        let (shed, mut shed_outcome) = create("checkpoint/shed")?;
        let mut state = state_at(1, Forest::empty())?;
        assert!(state.take_checkpoint().is_none());
        drive_runs(&mut state, WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER - 2)?;
        assert!(state.take_checkpoint().is_none());
        drive_runs(&mut state, 1)?;

        let first = state
            .take_checkpoint()
            .ok_or("the trigger plans a checkpoint")?;
        assert_eq!(0, first.ticket.attempt.local_counter());
        assert_eq!(Some(first.ticket), state.active_checkpoint());
        assert!(state.take_checkpoint().is_none());
        state.abandon_checkpoint(first.ticket);
        assert!(
            state.take_checkpoint().is_none(),
            "an attempted cut is suppressed"
        );

        let durable = WAL_SUFFIX_COORDINATES_MAX_V2 - 1;
        let mut retry_state = state_at(durable, Forest::empty())?;
        let first = retry_state
            .take_checkpoint()
            .ok_or("the bounded suffix head plans a checkpoint")?;
        retry_state.abandon_checkpoint(first.ticket);
        settle(retry_state.admit(shed))?;
        assert_eq!(Err(AdmissionRefusal::Overloaded), shed_outcome.try_recv()?);
        let retry = retry_state
            .take_checkpoint()
            .ok_or("retry accounting rearms the cut")?;
        assert_eq!(first.ticket.cut, retry.ticket.cut);
        assert_eq!(1, retry.ticket.attempt.local_counter());
        assert_eq!(
            retry.ticket,
            retry_state.active_checkpoint().ok_or("active retry")?
        );
        Ok(())
    }

    #[test]
    fn checkpoint_install_updates_base_and_seal_without_touching_durable_or_pending() -> TestResult
    {
        let mut state = state_at(1, Forest::empty())?;
        drive_runs(&mut state, WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER)?;
        let plan = state.take_checkpoint().ok_or("checkpoint is due")?;
        let durable = state.durable.clone();
        let admitted = state.admitted.clone();
        let successor = Seal::new(
            state.partition(),
            state.seal.generation().successor()?,
            WalReplayPoint::Through {
                batch: plan.ticket.cut,
                owner: state.claim.owner(),
            },
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;
        let installed = state.install_checkpoint(
            plan.ticket,
            CheckpointInstall {
                source: state.seal.clone(),
                successor: successor.clone(),
                snapshot: durable.clone(),
            },
        );
        assert_eq!(successor, state.seal);
        assert_eq!(durable, state.base);
        assert_eq!(durable, state.durable);
        assert_eq!(admitted, state.admitted);
        assert_eq!(installed.forest, state.durable);

        drive_runs(&mut state, WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER)?;
        let plan = state.take_checkpoint().ok_or("the newer cut plans")?;
        state.abandon_checkpoint(plan.ticket);
        assert_eq!(None, state.active_checkpoint());
        Ok(())
    }

    #[test]
    fn admission_refusal_and_idempotence_axes_are_complete() -> TestResult {
        let axes_path = path("events/axes")?;
        let missing_path = path("events/missing")?;
        let mut state = state_at(1, Forest::empty())?;
        let (create, mut created) = create("events/axes")?;
        assert!(matches!(state.admit(create), AdmissionDecision::Queued));
        let batch = run_batch(state.take_flight())?;
        send_batch(state.record_wal_durable(batch).replies);
        assert_eq!(Ok(CreateOutcome::Created), created.try_recv()?);
        let forest_before_refusals = state.durable.clone();

        for command in [
            CreateStream {
                path: axes_path.clone(),
                content_type: "text/plain".parse()?,
                expiry: ExpiryPolicy::None,
                lifecycle: StreamLifecycle::Open,
            },
            CreateStream {
                path: axes_path.clone(),
                content_type: StreamContentType::octet_stream(),
                expiry: ExpiryPolicy::SlidingTtl(StreamTtl::from(
                    NonZeroU64::new(60).expect("sixty is nonzero"),
                )),
                lifecycle: StreamLifecycle::Open,
            },
            CreateStream {
                path: axes_path.clone(),
                content_type: StreamContentType::octet_stream(),
                expiry: ExpiryPolicy::None,
                lifecycle: StreamLifecycle::Closed,
            },
        ] {
            let (reply, mut outcome) = oneshot::channel();
            settle(state.admit(CommandEnvelope::Create { command, reply }))?;
            assert_eq!(Err(AdmissionRefusal::PathOccupied), outcome.try_recv()?);
            assert_eq!(forest_before_refusals, state.admitted);
            assert_eq!(forest_before_refusals, state.durable);
        }

        let (reply, mut missing_close) = oneshot::channel();
        settle(state.admit(CommandEnvelope::Close {
            path: missing_path,
            reply,
        }))?;
        assert_eq!(
            Err(AdmissionRefusal::PathNotLive),
            missing_close.try_recv()?
        );
        assert_eq!(forest_before_refusals, state.admitted);
        assert_eq!(forest_before_refusals, state.durable);

        let (reply, mut closed) = oneshot::channel();
        assert!(matches!(
            state.admit(CommandEnvelope::Close {
                path: axes_path.clone(),
                reply,
            }),
            AdmissionDecision::Queued
        ));
        let batch = run_batch(state.take_flight())?;
        send_batch(state.record_wal_durable(batch).replies);
        assert_eq!(Ok(CloseStreamOutcome::Closed), closed.try_recv()?);
        let (reply, mut already_closed) = oneshot::channel();
        settle(state.admit(CommandEnvelope::Close {
            path: axes_path.clone(),
            reply,
        }))?;
        assert_eq!(
            Ok(CloseStreamOutcome::AlreadyClosed),
            already_closed.try_recv()?
        );

        let (reply, mut deleted) = oneshot::channel();
        assert!(matches!(
            state.admit(CommandEnvelope::Delete {
                path: axes_path.clone(),
                reply,
            }),
            AdmissionDecision::Queued
        ));
        let batch = run_batch(state.take_flight())?;
        send_batch(state.record_wal_durable(batch).replies);
        assert_eq!(Ok(()), deleted.try_recv()?);
        let (reply, mut deleted_again) = oneshot::channel();
        settle(state.admit(CommandEnvelope::Delete {
            path: axes_path,
            reply,
        }))?;
        assert_eq!(
            Err(AdmissionRefusal::PathNotLive),
            deleted_again.try_recv()?
        );
        Ok(())
    }

    #[test]
    fn invalid_effect_completions_panic_before_changing_protocol_state() -> TestResult {
        let mut state = state_at(1, Forest::empty())?;
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                state.record_wal_durable(BatchId::try_from(2).expect("two is nonzero"));
            }))
            .is_err()
        );
        assert!(!state.has_flight());

        let (create, _outcome) = create("events/negative")?;
        assert!(matches!(state.admit(create), AdmissionDecision::Queued));
        let batch = run_batch(state.take_flight())?;
        let wrong = batch.successor()?;
        assert!(catch_unwind(AssertUnwindSafe(|| state.record_wal_durable(wrong))).is_err());
        assert!(state.has_flight());
        assert!(matches!(state.take_flight(), FlightDecision::Idle));
        let durable = state.record_wal_durable(batch);
        assert_eq!(1, durable.replies.len());

        drive_runs(&mut state, WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER - 1)?;
        let plan = state.take_checkpoint().ok_or("checkpoint is due")?;
        let durable_before = state.durable.clone();
        let seal_before = state.seal.clone();
        let wrong_tickets = [
            CheckpointTicket {
                source: plan.ticket.source.successor()?,
                ..plan.ticket
            },
            CheckpointTicket {
                cut: plan.ticket.cut.successor()?,
                ..plan.ticket
            },
            CheckpointTicket {
                attempt: AttemptId::new(
                    plan.ticket.attempt.owner_claim(),
                    WRONG_CHECKPOINT_ATTEMPT,
                ),
                ..plan.ticket
            },
        ];
        for wrong_ticket in wrong_tickets {
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    state.abandon_checkpoint(wrong_ticket);
                }))
                .is_err()
            );
            assert_eq!(Some(plan.ticket), state.active_checkpoint());
            assert_eq!(durable_before, state.durable);
            assert_eq!(seal_before, state.seal);
        }

        let foreign_partition = PartitionId::try_from([2; 16])?;
        let foreign_source = Seal::new(
            foreign_partition,
            state.seal.generation(),
            WalReplayPoint::Genesis,
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;
        let foreign_successor = Seal::new(
            foreign_partition,
            foreign_source.generation().successor()?,
            WalReplayPoint::Through {
                batch: plan.ticket.cut,
                owner: state.claim.owner(),
            },
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;
        let foreign_install = CheckpointInstall {
            source: foreign_source,
            successor: foreign_successor,
            snapshot: state.durable.clone(),
        };
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                state.install_checkpoint(plan.ticket, foreign_install);
            }))
            .is_err()
        );
        assert_eq!(Some(plan.ticket), state.active_checkpoint());
        assert_eq!(durable_before, state.durable);
        assert_eq!(seal_before, state.seal);

        let wrong_cut_successor = Seal::new(
            state.partition(),
            state.seal.generation().successor()?,
            WalReplayPoint::Through {
                batch: plan.ticket.cut.successor()?,
                owner: state.claim.owner(),
            },
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;
        let wrong_cut_install = CheckpointInstall {
            source: state.seal.clone(),
            successor: wrong_cut_successor,
            snapshot: state.durable.clone(),
        };
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                state.install_checkpoint(plan.ticket, wrong_cut_install);
            }))
            .is_err()
        );
        assert_eq!(Some(plan.ticket), state.active_checkpoint());
        assert_eq!(durable_before, state.durable);
        assert_eq!(seal_before, state.seal);

        let wrong_owner_successor = Seal::new(
            state.partition(),
            state.seal.generation().successor()?,
            WalReplayPoint::Through {
                batch: plan.ticket.cut,
                owner: OwnerToken::from(SealGeneration::genesis()),
            },
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;
        let wrong_owner_install = CheckpointInstall {
            source: state.seal.clone(),
            successor: wrong_owner_successor,
            snapshot: state.durable.clone(),
        };
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                state.install_checkpoint(plan.ticket, wrong_owner_install);
            }))
            .is_err()
        );
        assert_eq!(Some(plan.ticket), state.active_checkpoint());
        assert_eq!(durable_before, state.durable);
        assert_eq!(seal_before, state.seal);
        state.abandon_checkpoint(plan.ticket);
        Ok(())
    }

    #[test]
    fn suffix_gate_reserves_exactly_one_takeover_coordinate() -> TestResult {
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

    fn path(raw: &str) -> Result<DirectoryKey, Box<dyn std::error::Error>> {
        Ok(DirectoryKey::try_from(Box::<[u8]>::from(raw.as_bytes()))?)
    }

    fn state_at(
        durable_batch: u64,
        forest: Forest,
    ) -> Result<WriterState, Box<dyn std::error::Error>> {
        let partition = PartitionId::try_from([1; 16])?;
        let claim_generation = SealGeneration::try_from(2)?;
        let seal = Seal::new(
            partition,
            claim_generation,
            WalReplayPoint::Genesis,
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;
        let durable_batch = BatchId::try_from(durable_batch)?;
        let next_batch = durable_batch.successor()?;
        Ok(WriterState::new(
            AuthoredClaim::new(claim_generation),
            seal,
            Forest::empty(),
            forest,
            durable_batch,
            next_batch,
        ))
    }

    fn create(raw: &str) -> Result<CreateEnvelope, Box<dyn std::error::Error>> {
        let (reply, outcome) = oneshot::channel();
        Ok((
            CommandEnvelope::Create {
                command: CreateStream {
                    path: path(raw)?,
                    content_type: StreamContentType::octet_stream(),
                    expiry: ExpiryPolicy::None,
                    lifecycle: StreamLifecycle::Open,
                },
                reply,
            },
            outcome,
        ))
    }

    fn run_batch(decision: FlightDecision) -> Result<BatchId, Box<dyn std::error::Error>> {
        match decision {
            FlightDecision::Run(encoded) => Ok(encoded.batch()),
            FlightDecision::Replies(_) | FlightDecision::Idle => {
                Err("a queued fact produces one WAL run".into())
            }
        }
    }

    fn drive_runs(state: &mut WriterState, count: u64) -> TestResult {
        for ordinal in 0..count {
            let (command, _outcome) =
                create(&format!("checkpoint/{ordinal}-{}", state.next_batch.get()))?;
            assert!(matches!(state.admit(command), AdmissionDecision::Queued));
            let batch = run_batch(state.take_flight())?;
            send_batch(state.record_wal_durable(batch).replies);
        }
        Ok(())
    }

    fn send_batch(replies: Vec<Completion>) {
        for reply in replies {
            super::super::send_completion(reply);
        }
    }

    fn settle(admitted: AdmissionDecision) -> TestResult {
        match admitted {
            AdmissionDecision::Settled(completion) => {
                super::super::send_completion(completion);
                Ok(())
            }
            AdmissionDecision::Queued => Err("the command settles without a WAL barrier".into()),
        }
    }
}
