//! Single-owner writer, bounded group commit, and publication ordering.

use strom_domain::{CloseStreamOutcome, CreateOutcome};
use strom_object_store::CreateEvidence;
use strom_storage_domain::{
    AttemptId, BatchId, DirectoryKey, OperationFact, PartitionId, Seal, WAL_RUN_FACTS_MAX,
    WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER, WalBody, WalFacts, WalObject, WalReplayPoint,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::admission::{
    AdmissionRefusal, AdmittedCommand, CloseAdmission, CreateAdmission, CreateStream, admit_close,
    admit_create, admit_delete, decide_suffix_room,
};
use crate::bootstrap::{AuthoredClaim, Ready, WriterSeed};
use crate::checkpoint::{CheckpointInput, CheckpointOutcome, PublicationGate, execute_checkpoint};
use crate::collection::collect_advance;
use crate::engine::PublishedView;
use crate::store::{EncodedWal, WalStore, WalStoreError};
use crate::{Applied, Forest};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum WriterExit {
    #[error("partition writer shut down after draining ingress")]
    Shutdown,
    #[error("partition writer was fenced at WAL batch {batch:?}")]
    Fenced { batch: BatchId },
    #[error("partition writer was poisoned at WAL batch {batch:?}: {detail}")]
    Poisoned { batch: BatchId, detail: String },
    #[error("partition writer found a contradiction at WAL batch {batch:?}: {detail}")]
    Contradiction { batch: BatchId, detail: String },
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

struct Writer {
    partition: PartitionId,
    claim: AuthoredClaim,
    seal: Seal,
    base: Forest,
    admitted: Forest,
    durable: Forest,
    durable_batch: BatchId,
    pending: Vec<PendingCommand>,
    spare: Vec<PendingCommand>,
    flight: Option<Flight>,
    checkpoint: Option<CheckpointFlight>,
    collector: Option<JoinHandle<()>>,
    checkpoint_attempt: u64,
    last_checkpoint_attempted_cut: Option<BatchId>,
    retry_checkpoint_at: Option<BatchId>,
    next_batch: BatchId,
    adapter: strom_object_store::ObjectStoreAdapter,
    wal_store: WalStore,
    view: watch::Sender<PublishedView>,
}

struct PendingCommand {
    /// `None` when the reply is idempotent and inherits an earlier fact's barrier.
    fact: Option<OperationFact>,
    completion: Completion,
}

enum Completion {
    Create {
        outcome: CreateOutcome,
        reply: oneshot::Sender<Result<CreateOutcome, AdmissionRefusal>>,
    },
    Close {
        outcome: CloseStreamOutcome,
        reply: oneshot::Sender<Result<CloseStreamOutcome, AdmissionRefusal>>,
    },
    Delete {
        reply: oneshot::Sender<Result<(), AdmissionRefusal>>,
    },
}

struct Flight {
    batch: BatchId,
    commands: Vec<PendingCommand>,
    encoded: EncodedWal,
    task: JoinHandle<Result<CreateEvidence, WalStoreError>>,
}

struct CheckpointFlight {
    publication: PublicationGate,
    task: JoinHandle<CheckpointOutcome>,
}

enum WriterEvent {
    Flight(Result<Result<CreateEvidence, WalStoreError>, tokio::task::JoinError>),
    Checkpoint(Result<CheckpointOutcome, tokio::task::JoinError>),
    Command(Option<CommandEnvelope>),
}

pub(crate) fn spawn_writer(
    adapter: strom_object_store::ObjectStoreAdapter,
    ready: Ready,
    ingress: mpsc::Receiver<CommandEnvelope>,
    view: watch::Sender<PublishedView>,
) -> JoinHandle<WriterExit> {
    let writer = Writer::new(adapter, ready, view);
    tokio::spawn(writer.run(ingress))
}

impl Writer {
    fn new(
        adapter: strom_object_store::ObjectStoreAdapter,
        ready: Ready,
        view: watch::Sender<PublishedView>,
    ) -> Self {
        let partition = ready.partition();
        let WriterSeed {
            claim,
            seal,
            base,
            forest,
            durable_batch,
            next_batch,
        } = ready.into_writer_seed();
        Self {
            partition,
            claim,
            seal,
            base,
            admitted: forest.clone(),
            durable: forest,
            durable_batch,
            pending: Vec::with_capacity(WAL_RUN_FACTS_MAX),
            spare: Vec::with_capacity(WAL_RUN_FACTS_MAX),
            flight: None,
            checkpoint: None,
            collector: None,
            checkpoint_attempt: 0,
            last_checkpoint_attempted_cut: None,
            retry_checkpoint_at: None,
            next_batch,
            wal_store: WalStore::new(adapter.clone()),
            adapter,
            view,
        }
    }

    async fn run(mut self, mut ingress: mpsc::Receiver<CommandEnvelope>) -> WriterExit {
        let mut ingress_open = true;
        loop {
            self.reap_collector();
            if self.flight.is_none() && !self.pending.is_empty() {
                self.promote_pending();
            }
            if ingress_open
                && self.should_checkpoint()
                && let Err(exit) = self.start_checkpoint()
            {
                return self.terminate(exit).await;
            }
            if self.flight.is_none() && self.checkpoint.is_none() && !ingress_open {
                assert!(
                    self.pending.is_empty(),
                    "shutdown exits only after every admitted command is promoted"
                );
                return self.terminate(WriterExit::Shutdown).await;
            }

            let event = match (self.flight.as_mut(), self.checkpoint.as_mut(), ingress_open) {
                (Some(flight), Some(checkpoint), true) => tokio::select! {
                    biased;
                    completion = &mut flight.task => WriterEvent::Flight(completion),
                    completion = &mut checkpoint.task => WriterEvent::Checkpoint(completion),
                    command = ingress.recv() => WriterEvent::Command(command),
                },
                (Some(flight), None, true) => tokio::select! {
                    biased;
                    completion = &mut flight.task => WriterEvent::Flight(completion),
                    command = ingress.recv() => WriterEvent::Command(command),
                },
                (None, Some(checkpoint), true) => tokio::select! {
                    biased;
                    completion = &mut checkpoint.task => WriterEvent::Checkpoint(completion),
                    command = ingress.recv() => WriterEvent::Command(command),
                },
                (None, None, true) => WriterEvent::Command(ingress.recv().await),
                (Some(flight), Some(checkpoint), false) => tokio::select! {
                    biased;
                    completion = &mut flight.task => WriterEvent::Flight(completion),
                    completion = &mut checkpoint.task => WriterEvent::Checkpoint(completion),
                },
                (Some(flight), None, false) => WriterEvent::Flight((&mut flight.task).await),
                (None, Some(checkpoint), false) => {
                    WriterEvent::Checkpoint((&mut checkpoint.task).await)
                }
                (None, None, false) => {
                    return self.terminate(WriterExit::Shutdown).await;
                }
            };

            match event {
                WriterEvent::Flight(completion) => {
                    let evidence = match completion {
                        Ok(evidence) => evidence,
                        Err(join_error) => {
                            let flight = self
                                .flight
                                .take()
                                .expect("a WAL completion retains its flight");
                            let batch = flight.batch;
                            let exit = WriterExit::Contradiction {
                                batch,
                                detail: format!("WAL create task failed: {join_error}"),
                            };
                            return self.terminate(exit).await;
                        }
                    };
                    if let Err(exit) = self.complete_flight(evidence).await {
                        return self.terminate(exit).await;
                    }
                }
                WriterEvent::Checkpoint(completion) => {
                    let outcome = match completion {
                        Ok(outcome) => outcome,
                        Err(join_error) => {
                            let exit = WriterExit::Contradiction {
                                batch: self.durable_batch,
                                detail: format!("checkpoint task failed: {join_error}"),
                            };
                            self.checkpoint = None;
                            return self.terminate(exit).await;
                        }
                    };
                    self.checkpoint = None;
                    if let Err(exit) = self.complete_checkpoint(outcome) {
                        return self.terminate(exit).await;
                    }
                }
                WriterEvent::Command(Some(envelope)) => self.consider(envelope),
                WriterEvent::Command(None) => {
                    ingress_open = false;
                    if let Some(checkpoint) = &self.checkpoint {
                        checkpoint.publication.cancel_before_publish();
                    }
                }
            }
        }
    }

    fn consider(&mut self, envelope: CommandEnvelope) {
        assert!(
            self.flight.is_some() || self.pending.is_empty(),
            "when no flight is active, consider observes empty pending so admitted equals durable"
        );
        let batch = self.next_batch;
        if self.pending.len() == WAL_RUN_FACTS_MAX {
            envelope.refuse(AdmissionRefusal::Overloaded);
            return;
        }

        match envelope {
            CommandEnvelope::Create { command, reply } => {
                match admit_create(&self.admitted, &command, batch) {
                    Ok(CreateAdmission::Fact(admitted)) => self.accept_fact(
                        admitted,
                        Completion::Create {
                            outcome: CreateOutcome::Created,
                            reply,
                        },
                    ),
                    Ok(CreateAdmission::AlreadyExists) => {
                        self.accept_idempotent(Completion::Create {
                            outcome: CreateOutcome::AlreadyExists,
                            reply,
                        });
                    }
                    Err(refusal) => {
                        let _receiver_may_be_gone = reply.send(Err(refusal));
                    }
                }
            }
            CommandEnvelope::Close { path, reply } => {
                match admit_close(&self.admitted, &path, batch) {
                    Ok(CloseAdmission::Fact(admitted)) => self.accept_fact(
                        admitted,
                        Completion::Close {
                            outcome: CloseStreamOutcome::Closed,
                            reply,
                        },
                    ),
                    Ok(CloseAdmission::AlreadyClosed) => {
                        self.accept_idempotent(Completion::Close {
                            outcome: CloseStreamOutcome::AlreadyClosed,
                            reply,
                        });
                    }
                    Err(refusal) => {
                        let _receiver_may_be_gone = reply.send(Err(refusal));
                    }
                }
            }
            CommandEnvelope::Delete { path, reply } => {
                match admit_delete(&self.admitted, &path, batch) {
                    Ok(admitted) => self.accept_fact(admitted, Completion::Delete { reply }),
                    Err(refusal) => {
                        let _receiver_may_be_gone = reply.send(Err(refusal));
                    }
                }
            }
        }
    }

    fn accept_fact(&mut self, admitted: AdmittedCommand, completion: Completion) {
        // Only a fact consumes a WAL coordinate, so the suffix gate
        // sheds new mutations while idempotent replies stay answerable.
        if !decide_suffix_room(replay_batch(self.seal.replay()), self.next_batch) {
            self.retry_checkpoint_at = Some(self.durable_batch);
            completion.refuse(AdmissionRefusal::Overloaded);
            return;
        }
        self.admitted = admitted.forest;
        self.pending.push(PendingCommand {
            fact: Some(admitted.fact),
            completion,
        });
        if self.flight.is_none() {
            self.promote_pending();
        }
    }

    fn accept_idempotent(&mut self, completion: Completion) {
        if self.flight.is_none() {
            completion.send();
        } else {
            self.pending.push(PendingCommand {
                fact: None,
                completion,
            });
        }
    }

    fn promote_pending(&mut self) {
        assert!(
            self.flight.is_none(),
            "only one WAL create may be in flight"
        );
        assert!(
            !self.pending.is_empty(),
            "only a nonempty pending set is promoted"
        );
        let commands = std::mem::replace(&mut self.pending, std::mem::take(&mut self.spare));
        let facts: Vec<OperationFact> = commands
            .iter()
            .filter_map(|command| command.fact.clone())
            .collect();
        if facts.is_empty() {
            let mut commands = commands;
            for command in commands.drain(..) {
                command.completion.send();
            }
            self.spare = commands;
            return;
        }
        let batch = self.next_batch;
        let facts = pending_facts(facts);
        let encoded = EncodedWal::new(&WalObject::new(
            self.partition,
            batch,
            self.claim.owner(),
            WalBody::Run(facts),
        ))
        .expect("the fact-count and field bounds prove every pending RUN fits the WAL byte bound");
        self.next_batch = batch
            .successor()
            .expect("the suffix reserve proves a coordinate after every admitted RUN");
        let store = self.wal_store.clone();
        let candidate = encoded.clone();
        let task = tokio::spawn(async move { store.create_wal(&candidate).await });
        self.flight = Some(Flight {
            batch,
            commands,
            encoded,
            task,
        });
    }

    async fn complete_flight(
        &mut self,
        evidence: Result<CreateEvidence, WalStoreError>,
    ) -> Result<(), WriterExit> {
        let flight = self
            .flight
            .take()
            .expect("only an active flight can complete");
        match evidence {
            Ok(CreateEvidence::Direct | CreateEvidence::DurableMatch) => {
                self.commit(flight);
                Ok(())
            }
            Ok(CreateEvidence::NotOurs) => Err(WriterExit::Fenced {
                batch: flight.batch,
            }),
            Ok(CreateEvidence::Unresolved) => {
                let observed = self
                    .wal_store
                    .read_wal(self.partition, flight.encoded.batch())
                    .await;
                match observed {
                    Ok(Some(observed)) if observed.as_slice() == flight.encoded.as_slice() => {
                        self.commit(flight);
                        Ok(())
                    }
                    Ok(Some(_foreign)) => Err(WriterExit::Fenced {
                        batch: flight.batch,
                    }),
                    Ok(None) => Err(WriterExit::Poisoned {
                        batch: flight.batch,
                        detail: "unresolved WAL create is absent on its one reconciliation read"
                            .into(),
                    }),
                    Err(
                        WalStoreError::Retryable { detail } | WalStoreError::Rejected { detail },
                    ) => Err(WriterExit::Poisoned {
                        batch: flight.batch,
                        detail,
                    }),
                    Err(WalStoreError::Contradiction { detail }) => {
                        Err(WriterExit::Contradiction {
                            batch: flight.batch,
                            detail,
                        })
                    }
                }
            }
            Err(WalStoreError::Retryable { detail } | WalStoreError::Rejected { detail }) => {
                Err(WriterExit::Poisoned {
                    batch: flight.batch,
                    detail,
                })
            }
            Err(WalStoreError::Contradiction { detail }) => Err(WriterExit::Contradiction {
                batch: flight.batch,
                detail,
            }),
        }
    }

    fn start_checkpoint(&mut self) -> Result<(), WriterExit> {
        assert!(
            self.checkpoint.is_none(),
            "only one checkpoint may be in flight"
        );
        assert!(
            replay_batch(self.seal.replay()).is_none_or(|cut| self.durable_batch > cut),
            "a checkpoint trigger names an advancing durable cut"
        );
        let attempt = AttemptId::new(self.claim.generation(), self.checkpoint_attempt);
        self.checkpoint_attempt =
            self.checkpoint_attempt
                .checked_add(1)
                .ok_or_else(|| WriterExit::Contradiction {
                    batch: self.durable_batch,
                    detail: "checkpoint attempt counter is exhausted".into(),
                })?;
        let input = CheckpointInput {
            source: self.seal.clone(),
            base: self.base.clone(),
            snapshot: self.durable.clone(),
            cut: self.durable_batch,
            attempt,
        };
        let publication = PublicationGate::new();
        let task = tokio::spawn(execute_checkpoint(
            self.adapter.clone(),
            input,
            publication.clone(),
        ));
        self.checkpoint = Some(CheckpointFlight { publication, task });
        self.last_checkpoint_attempted_cut = Some(self.durable_batch);
        self.retry_checkpoint_at = None;
        Ok(())
    }

    fn should_checkpoint(&self) -> bool {
        if self.checkpoint.is_some()
            || suffix_span(self.seal.replay(), self.durable_batch)
                < WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER
        {
            return false;
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
        head_is_unattempted || self.retry_checkpoint_at == Some(self.durable_batch)
    }

    fn complete_checkpoint(&mut self, outcome: CheckpointOutcome) -> Result<(), WriterExit> {
        match outcome {
            CheckpointOutcome::Abandoned => Ok(()),
            CheckpointOutcome::Contradiction { cut, detail } => {
                Err(WriterExit::Contradiction { batch: cut, detail })
            }
            CheckpointOutcome::Seal { prepared, evidence } => match evidence {
                Ok(CreateEvidence::Direct) => {
                    let install = (*prepared).into_install();
                    let source = install.source;
                    let successor = install.successor;
                    self.seal = successor.clone();
                    self.base = install.snapshot;
                    self.view
                        .send_replace(PublishedView::new(self.durable.clone()));
                    if self.collector.is_none() {
                        let adapter = self.adapter.clone();
                        self.collector =
                            Some(tokio::spawn(collect_advance(adapter, source, successor)));
                    }
                    Ok(())
                }
                Ok(CreateEvidence::DurableMatch | CreateEvidence::NotOurs) => {
                    Err(WriterExit::Fenced {
                        batch: prepared.cut().unwrap_or(self.durable_batch),
                    })
                }
                Ok(CreateEvidence::Unresolved) => Err(WriterExit::Poisoned {
                    batch: prepared.cut().unwrap_or(self.durable_batch),
                    detail: "advancing Seal create is unresolved".into(),
                }),
                Err(crate::store::SealStoreError::Contradiction { detail }) => {
                    Err(WriterExit::Contradiction {
                        batch: prepared.cut().unwrap_or(self.durable_batch),
                        detail,
                    })
                }
                Err(
                    crate::store::SealStoreError::Retryable { detail }
                    | crate::store::SealStoreError::Rejected { detail },
                ) => Err(WriterExit::Poisoned {
                    batch: prepared.cut().unwrap_or(self.durable_batch),
                    detail,
                }),
            },
        }
    }

    async fn terminate(&mut self, exit: WriterExit) -> WriterExit {
        if let Some(collector) = self.collector.take() {
            collector.abort();
        }

        if let Some(checkpoint) = self.checkpoint.take() {
            checkpoint.publication.cancel_before_publish();
            let _resolved_publication = checkpoint.task.await;
        }

        if self.flight.is_some() {
            let completion = {
                let flight = self
                    .flight
                    .as_mut()
                    .expect("terminal draining observes the active WAL flight");
                (&mut flight.task).await
            };
            if let Ok(evidence) = completion {
                let _classified_wal = self.complete_flight(evidence).await;
            }
        }

        exit
    }

    fn reap_collector(&mut self) {
        if self.collector.as_ref().is_some_and(JoinHandle::is_finished) {
            drop(self.collector.take());
        }
    }

    fn commit(&mut self, mut flight: Flight) {
        for command in &flight.commands {
            let Some(fact) = &command.fact else {
                continue;
            };
            assert_eq!(
                Ok(Applied),
                self.durable.strict_fold(flight.batch, fact),
                "durable fold repeats facts already proven against admitted state"
            );
        }
        self.durable_batch = flight.batch;
        self.view
            .send_replace(PublishedView::new(self.durable.clone()));
        for command in flight.commands.drain(..) {
            command.completion.send();
        }
        self.spare = flight.commands;
    }
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

impl CommandEnvelope {
    fn refuse(self, refusal: AdmissionRefusal) {
        match self {
            Self::Create { command: _, reply } => {
                let _receiver_may_be_gone = reply.send(Err(refusal));
            }
            Self::Close { path: _, reply } => {
                let _receiver_may_be_gone = reply.send(Err(refusal));
            }
            Self::Delete { path: _, reply } => {
                let _receiver_may_be_gone = reply.send(Err(refusal));
            }
        }
    }
}

impl Completion {
    fn send(self) {
        match self {
            Self::Create { outcome, reply } => {
                let _receiver_may_be_gone = reply.send(Ok(outcome));
            }
            Self::Close { outcome, reply } => {
                let _receiver_may_be_gone = reply.send(Ok(outcome));
            }
            Self::Delete { reply } => {
                let _receiver_may_be_gone = reply.send(Ok(()));
            }
        }
    }

    fn refuse(self, refusal: AdmissionRefusal) {
        match self {
            Self::Create { outcome: _, reply } => {
                let _receiver_may_be_gone = reply.send(Err(refusal));
            }
            Self::Close { outcome: _, reply } => {
                let _receiver_may_be_gone = reply.send(Err(refusal));
            }
            Self::Delete { reply } => {
                let _receiver_may_be_gone = reply.send(Err(refusal));
            }
        }
    }
}

fn pending_facts(facts: Vec<OperationFact>) -> WalFacts {
    WalFacts::try_from(facts)
        .expect("pending RUN construction enforces nonempty and fact-count bounds")
}
