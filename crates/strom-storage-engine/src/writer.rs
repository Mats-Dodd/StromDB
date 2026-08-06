//! Single-owner writer, bounded group commit, and publication ordering.

use strom_object_store::CreateEvidence;
use strom_storage_domain::{
    AttemptId, BatchId, OperationFact, PartitionId, Seal, WAL_RUN_FACTS_MAX, WalBody, WalFacts,
    WalObject, WalReplayPoint,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::admission::{Admission, admit, decide_suffix_room};
use crate::bootstrap::{AuthoredClaim, Ready, WriterSeed};
use crate::checkpoint::{
    CheckpointInput, CheckpointOutcome, PublicationGate, WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER,
    collect_advance, execute_checkpoint,
};
use crate::engine::PublishedView;
use crate::store::{EncodedWal, WalStore, WalStoreError};
use crate::{AdmissionRefusal, Applied, Forest, StreamCommand, StreamReply};

/// Commands waiting to be considered by the single writer.
pub(crate) const WRITER_INGRESS_COMMANDS_MAX: usize = 1024;

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

pub(crate) struct CommandEnvelope {
    pub(crate) command: StreamCommand,
    pub(crate) reply: oneshot::Sender<Result<StreamReply, AdmissionRefusal>>,
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
    checkpoint_requested: bool,
    next_batch: BatchId,
    adapter: strom_object_store::ObjectStoreAdapter,
    wal_store: WalStore,
    view: watch::Sender<PublishedView>,
}

struct PendingCommand {
    /// `None` when the reply is idempotent and inherits an earlier fact's barrier.
    fact: Option<OperationFact>,
    reply: StreamReply,
    waiter: oneshot::Sender<Result<StreamReply, AdmissionRefusal>>,
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
        let checkpoint_requested =
            suffix_span(seal.replay(), durable_batch) >= WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER;
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
            checkpoint_requested,
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
                && self.checkpoint.is_none()
                && self.checkpoint_requested
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
            send_refusal(envelope.reply, AdmissionRefusal::Overloaded);
            return;
        }

        match admit(&self.admitted, &envelope.command, batch) {
            Ok(Admission::Fact(admitted)) => {
                // Only a fact consumes a WAL coordinate, so the suffix gate
                // sheds new mutations while idempotent replies stay answerable.
                if !decide_suffix_room(replay_batch(self.seal.replay()), batch) {
                    if self.checkpoint.is_none() {
                        self.checkpoint_requested = true;
                    }
                    send_refusal(envelope.reply, AdmissionRefusal::Overloaded);
                    return;
                }
                self.admitted = admitted.forest;
                self.pending.push(PendingCommand {
                    fact: Some(admitted.fact),
                    reply: admitted.reply,
                    waiter: envelope.reply,
                });
                if self.flight.is_none() {
                    self.promote_pending();
                }
            }
            Ok(Admission::Idempotent(reply)) => {
                if self.flight.is_none() {
                    let _receiver_may_be_gone = envelope.reply.send(Ok(reply));
                } else {
                    self.pending.push(PendingCommand {
                        fact: None,
                        reply,
                        waiter: envelope.reply,
                    });
                }
            }
            Err(refusal) => {
                send_refusal(envelope.reply, refusal);
            }
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
                let _receiver_may_be_gone = command.waiter.send(Ok(command.reply));
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
        self.checkpoint_requested = false;
        Ok(())
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
            let _receiver_may_be_gone = command.waiter.send(Ok(command.reply));
        }
        self.spare = flight.commands;
        if self.checkpoint.is_none()
            && suffix_span(self.seal.replay(), self.durable_batch)
                >= WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER
        {
            self.checkpoint_requested = true;
        }
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

fn send_refusal(
    waiter: oneshot::Sender<Result<StreamReply, AdmissionRefusal>>,
    refusal: AdmissionRefusal,
) {
    let _receiver_may_be_gone = waiter.send(Err(refusal));
}

fn pending_facts(facts: Vec<OperationFact>) -> WalFacts {
    WalFacts::try_from(facts)
        .expect("pending RUN construction enforces nonempty and fact-count bounds")
}

#[cfg(test)]
mod tests {
    use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};
    use strom_object_store::ObjectStoreAdapter;
    use strom_storage_domain::{
        DirectoryEntry, DirectoryKey, StreamUid, WAL_SUFFIX_COORDINATES_MAX_V2,
    };

    use super::*;
    use crate::bootstrap::bootstrap;

    struct TestAdmission {
        command: PendingCommand,
        candidate: EncodedWal,
        reply: oneshot::Receiver<Result<StreamReply, AdmissionRefusal>>,
    }

    #[tokio::test]
    async fn commands_behind_one_flight_freeze_into_one_ordered_run()
    -> Result<(), Box<dyn std::error::Error>> {
        let adapter = ObjectStoreAdapter::in_memory();
        let (mut writer, _view) = writer(adapter.clone()).await?;
        let partition = writer.partition;
        let batch_2 = writer.next_batch;
        let initial = create_command("events/a")?;
        let initial = admitted_command(&mut writer, &initial, batch_2)?;
        assert_eq!(
            CreateEvidence::Direct,
            writer.wal_store.create_wal(&initial.candidate).await?
        );
        install_flight(
            &mut writer,
            batch_2,
            initial.command,
            initial.candidate,
            CreateEvidence::DurableMatch,
        );
        let _initial_reply = initial.reply;

        let reply_b = consider(&mut writer, create_command("events/b")?);
        let reply_c = consider(&mut writer, create_command("events/c")?);
        assert_eq!(
            2,
            writer.pending.len(),
            "both commands accumulate behind the active flight"
        );

        let (sender, ingress) = mpsc::channel(WRITER_INGRESS_COMMANDS_MAX);
        drop(sender);
        assert_eq!(WriterExit::Shutdown, writer.run(ingress).await);
        assert_eq!(
            StreamReply::Created {
                uid: StreamUid::try_from(2)?
            },
            reply_b.await??
        );
        assert_eq!(
            StreamReply::Created {
                uid: StreamUid::try_from(3)?
            },
            reply_c.await??
        );

        let grouped = WalStore::new(adapter)
            .read_wal(partition, BatchId::try_from(3)?)
            .await?
            .expect("the grouped pending RUN was created");
        let WalBody::Run(facts) = grouped.body() else {
            return Err("the grouped coordinate is not a RUN".into());
        };
        assert_eq!(
            2,
            facts.as_slice().len(),
            "the frozen RUN preserves both admissions in one object"
        );
        Ok(())
    }

    #[tokio::test]
    async fn flight_evidence_classes_commit_fence_and_poison_without_resend()
    -> Result<(), Box<dyn std::error::Error>> {
        let exact_adapter = ObjectStoreAdapter::in_memory();
        let (mut exact, _view) = writer(exact_adapter).await?;
        let batch = exact.next_batch;
        let command = create_command("events/exact")?;
        let admission = admitted_command(&mut exact, &command, batch)?;
        assert_eq!(
            CreateEvidence::Direct,
            exact.wal_store.create_wal(&admission.candidate).await?
        );
        install_flight(
            &mut exact,
            batch,
            admission.command,
            admission.candidate,
            CreateEvidence::Unresolved,
        );
        assert_eq!(
            Ok(()),
            exact.complete_flight(Ok(CreateEvidence::Unresolved)).await,
            "one exact read reconciles an ambiguous create"
        );
        assert!(matches!(
            admission.reply.await?,
            Ok(StreamReply::Created { .. })
        ));

        let foreign_adapter = ObjectStoreAdapter::in_memory();
        let (mut foreign, _view) = writer(foreign_adapter).await?;
        let batch = foreign.next_batch;
        let command = create_command("events/foreign")?;
        let admission = admitted_command(&mut foreign, &command, batch)?;
        let occupant = EncodedWal::new(&WalObject::new(
            foreign.partition,
            batch,
            foreign.claim.owner(),
            WalBody::Fence,
        ))?;
        assert_eq!(
            CreateEvidence::Direct,
            foreign.wal_store.create_wal(&occupant).await?
        );
        install_flight(
            &mut foreign,
            batch,
            admission.command,
            admission.candidate,
            CreateEvidence::Unresolved,
        );
        let _reply = admission.reply;
        assert_eq!(
            Err(WriterExit::Fenced { batch }),
            foreign
                .complete_flight(Ok(CreateEvidence::Unresolved))
                .await
        );

        let absent_adapter = ObjectStoreAdapter::in_memory();
        let (mut absent, _view) = writer(absent_adapter).await?;
        let batch = absent.next_batch;
        let command = create_command("events/absent")?;
        let admission = admitted_command(&mut absent, &command, batch)?;
        install_flight(
            &mut absent,
            batch,
            admission.command,
            admission.candidate,
            CreateEvidence::Unresolved,
        );
        let _reply = admission.reply;
        assert!(matches!(
            absent.complete_flight(Ok(CreateEvidence::Unresolved)).await,
            Err(WriterExit::Poisoned {
                batch: poisoned_batch,
                detail: _,
            }) if poisoned_batch == batch
        ));

        let fenced_adapter = ObjectStoreAdapter::in_memory();
        let (mut fenced, _view) = writer(fenced_adapter).await?;
        let batch = fenced.next_batch;
        let command = create_command("events/fenced")?;
        let admission = admitted_command(&mut fenced, &command, batch)?;
        install_flight(
            &mut fenced,
            batch,
            admission.command,
            admission.candidate,
            CreateEvidence::NotOurs,
        );
        let _reply = admission.reply;
        assert_eq!(
            Err(WriterExit::Fenced { batch }),
            fenced.complete_flight(Ok(CreateEvidence::NotOurs)).await
        );
        Ok(())
    }

    #[tokio::test]
    async fn direct_checkpoint_reopens_to_the_materialized_forest()
    -> Result<(), Box<dyn std::error::Error>> {
        let adapter = ObjectStoreAdapter::in_memory();
        let (mut writer, _view) = writer(adapter.clone()).await?;
        let path = DirectoryKey::try_from(Box::<[u8]>::from(b"events/checkpoint".as_slice()))?;
        let reply = consider(&mut writer, create_command("events/checkpoint")?);
        let completion = (&mut writer
            .flight
            .as_mut()
            .expect("considering the first command starts a WAL flight")
            .task)
            .await
            .expect("the in-memory WAL task completes");
        writer.complete_flight(completion).await?;
        assert!(matches!(reply.await?, Ok(StreamReply::Created { .. })));

        writer.checkpoint_requested = true;
        writer.start_checkpoint()?;
        complete_active_checkpoint(&mut writer).await?;

        let ready = bootstrap(adapter, crate::test_entropy()).await?;
        assert_eq!(
            Some(DirectoryEntry::Live(StreamUid::try_from(1)?)),
            ready.forest().resolve(&path),
            "recovery loads the checkpoint tables before replay"
        );
        assert_eq!(
            replay_batch(writer.seal.replay()),
            replay_batch(ready.replay()),
            "the reopened claim inherits the advancing checkpoint cut"
        );
        Ok(())
    }

    #[tokio::test]
    async fn first_takeover_checkpoint_materializes_the_replayed_older_owner_suffix()
    -> Result<(), Box<dyn std::error::Error>> {
        let adapter = ObjectStoreAdapter::in_memory();
        let (mut first, _view) = writer(adapter.clone()).await?;
        let path = DirectoryKey::try_from(Box::<[u8]>::from(b"events/takeover".as_slice()))?;
        let reply = consider(&mut first, create_command("events/takeover")?);
        complete_active_wal(&mut first).await?;
        assert!(matches!(reply.await?, Ok(StreamReply::Created { .. })));
        drop(first);

        let ready = bootstrap(adapter.clone(), crate::test_entropy()).await?;
        assert_eq!(
            Some(DirectoryEntry::Live(StreamUid::try_from(1)?)),
            ready.forest().resolve(&path),
            "takeover replays the older owner's RUN before Ready"
        );
        let initial = PublishedView::new(ready.forest().clone());
        let (view, _receiver) = watch::channel(initial);
        let mut takeover = Writer::new(adapter.clone(), ready, view);
        takeover.checkpoint_requested = true;
        takeover.start_checkpoint()?;
        complete_active_checkpoint(&mut takeover).await?;

        let reopened = bootstrap(adapter, crate::test_entropy()).await?;
        assert_eq!(
            Some(DirectoryEntry::Live(StreamUid::try_from(1)?)),
            reopened.forest().resolve(&path),
            "the first takeover checkpoint diffs against the pre-replay base"
        );
        assert_eq!(
            replay_batch(takeover.seal.replay()),
            replay_batch(reopened.replay())
        );
        Ok(())
    }

    #[tokio::test]
    async fn fence_only_checkpoint_advances_the_cut_without_creating_tables()
    -> Result<(), Box<dyn std::error::Error>> {
        let adapter = ObjectStoreAdapter::in_memory();
        let (mut writer, _view) = writer(adapter).await?;
        assert!(
            writer.base == writer.durable,
            "the takeover suffix has no facts"
        );
        writer.checkpoint_requested = true;
        writer.start_checkpoint()?;
        complete_active_checkpoint(&mut writer).await?;
        assert_eq!(
            Some(writer.durable_batch),
            replay_batch(writer.seal.replay())
        );
        assert!(writer.seal.directory().is_empty());
        assert!(writer.seal.ledger().is_empty());
        assert!(
            decide_suffix_room(replay_batch(writer.seal.replay()), writer.next_batch),
            "advancing through the FENCE restores suffix admission room"
        );
        Ok(())
    }

    #[tokio::test]
    async fn terminal_drain_cancels_checkpoint_and_resolves_the_active_wal()
    -> Result<(), Box<dyn std::error::Error>> {
        let adapter = ObjectStoreAdapter::in_memory();
        let (mut writer, _view) = writer(adapter).await?;
        let batch = writer.next_batch;
        let command = create_command("events/drain")?;
        let admission = admitted_command(&mut writer, &command, batch)?;
        install_flight(
            &mut writer,
            batch,
            admission.command,
            admission.candidate,
            CreateEvidence::NotOurs,
        );
        let _reply = admission.reply;

        let (wal_release, wal_wait) = oneshot::channel();
        let (wal_resolved, resolved) = oneshot::channel();
        writer
            .flight
            .as_mut()
            .expect("the test installed one WAL flight")
            .task = tokio::spawn(async move {
            let _release_sender_exists = wal_wait.await;
            let _observer_may_be_gone = wal_resolved.send(());
            Ok(CreateEvidence::NotOurs)
        });

        let publication = PublicationGate::new();
        let observed_publication = publication.clone();
        let task_publication = publication.clone();
        let (checkpoint_release, checkpoint_wait) = oneshot::channel();
        let (checkpoint_cancelled, cancelled) = oneshot::channel();
        let checkpoint_task = tokio::spawn(async move {
            let _release_sender_exists = checkpoint_wait.await;
            let cancelled_before_publish = !task_publication.test_begin_publish();
            let _observer_may_be_gone = checkpoint_cancelled.send(cancelled_before_publish);
            CheckpointOutcome::Abandoned
        });
        writer.checkpoint = Some(CheckpointFlight {
            publication,
            task: checkpoint_task,
        });

        let terminal_batch = writer.durable_batch;
        let termination = tokio::spawn(async move {
            writer
                .terminate(WriterExit::Poisoned {
                    batch: terminal_batch,
                    detail: "injected terminal evidence".into(),
                })
                .await
        });
        while !observed_publication.test_is_claimed() {
            tokio::task::yield_now().await;
        }
        checkpoint_release
            .send(())
            .expect("the checkpoint waits for its release");
        assert!(
            cancelled.await?,
            "terminal draining wins the pre-publication boundary"
        );
        assert!(
            !termination.is_finished(),
            "terminal draining waits for the already-sent WAL create"
        );
        wal_release.send(()).expect("the WAL waits for its release");
        resolved.await?;
        assert!(matches!(
            termination.await?,
            WriterExit::Poisoned { batch, detail: _ } if batch == terminal_batch
        ));
        Ok(())
    }

    #[tokio::test]
    async fn terminal_drain_waits_when_checkpoint_publication_already_started()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut writer, _view) = writer(ObjectStoreAdapter::in_memory()).await?;
        let publication = PublicationGate::new();
        assert!(publication.test_begin_publish());
        let (release, wait) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _release_sender_exists = wait.await;
            CheckpointOutcome::Abandoned
        });
        writer.checkpoint = Some(CheckpointFlight { publication, task });

        let terminal_batch = writer.durable_batch;
        let termination = tokio::spawn(async move {
            writer
                .terminate(WriterExit::Poisoned {
                    batch: terminal_batch,
                    detail: "injected terminal evidence".into(),
                })
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !termination.is_finished(),
            "a begun Seal create is resolved before writer exit"
        );
        release
            .send(())
            .expect("the checkpoint publication waits for its release");
        assert!(matches!(
            termination.await?,
            WriterExit::Poisoned { batch, detail: _ } if batch == terminal_batch
        ));
        Ok(())
    }

    #[tokio::test]
    async fn wal_join_error_exits_contradiction_without_repolling_the_completed_task()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut writer, _view) = writer(ObjectStoreAdapter::in_memory()).await?;
        let batch = writer.next_batch;
        let command = create_command("events/join-error")?;
        let admission = admitted_command(&mut writer, &command, batch)?;
        install_flight(
            &mut writer,
            batch,
            admission.command,
            admission.candidate,
            CreateEvidence::Direct,
        );
        let _reply = admission.reply;
        writer
            .flight
            .as_ref()
            .expect("the test installed one WAL flight")
            .task
            .abort();

        let (_sender, ingress) = mpsc::channel(WRITER_INGRESS_COMMANDS_MAX);
        assert!(matches!(
            writer.run(ingress).await,
            WriterExit::Contradiction {
                batch: failed_batch,
                detail: _,
            } if failed_batch == batch
        ));
        Ok(())
    }

    async fn complete_active_checkpoint(writer: &mut Writer) -> Result<(), WriterExit> {
        let outcome = (&mut writer
            .checkpoint
            .as_mut()
            .expect("the test has one active checkpoint")
            .task)
            .await
            .expect("the in-memory checkpoint task completes");
        writer.checkpoint = None;
        writer.complete_checkpoint(outcome)
    }

    async fn complete_active_wal(writer: &mut Writer) -> Result<(), WriterExit> {
        let completion = (&mut writer
            .flight
            .as_mut()
            .expect("the test has one active WAL flight")
            .task)
            .await
            .expect("the in-memory WAL task completes");
        writer.complete_flight(completion).await
    }

    async fn writer(
        adapter: ObjectStoreAdapter,
    ) -> Result<(Writer, watch::Receiver<PublishedView>), Box<dyn std::error::Error>> {
        let ready = bootstrap(adapter.clone(), crate::test_entropy()).await?;
        let initial = PublishedView::new(ready.forest().clone());
        let (view, receiver) = watch::channel(initial);
        Ok((Writer::new(adapter, ready, view), receiver))
    }

    fn admitted_command(
        writer: &mut Writer,
        command: &StreamCommand,
        batch: BatchId,
    ) -> Result<TestAdmission, Box<dyn std::error::Error>> {
        let Admission::Fact(admitted) = admit(&writer.admitted, command, batch)? else {
            return Err("test admission helper requires a new fact".into());
        };
        writer.admitted = admitted.forest;
        let candidate = EncodedWal::new(&WalObject::new(
            writer.partition,
            batch,
            writer.claim.owner(),
            WalBody::Run(WalFacts::try_from(vec![admitted.fact.clone()])?),
        ))?;
        let (waiter, receiver) = oneshot::channel();
        Ok(TestAdmission {
            command: PendingCommand {
                fact: Some(admitted.fact),
                reply: admitted.reply,
                waiter,
            },
            candidate,
            reply: receiver,
        })
    }

    fn install_flight(
        writer: &mut Writer,
        batch: BatchId,
        command: PendingCommand,
        encoded: EncodedWal,
        evidence: CreateEvidence,
    ) {
        let task = tokio::spawn(async move { Ok::<CreateEvidence, WalStoreError>(evidence) });
        writer.next_batch = batch
            .successor()
            .expect("the small test coordinate has a successor");
        writer.flight = Some(Flight {
            batch,
            commands: vec![command],
            encoded,
            task,
        });
    }

    fn consider(
        writer: &mut Writer,
        command: StreamCommand,
    ) -> oneshot::Receiver<Result<StreamReply, AdmissionRefusal>> {
        let (reply, outcome) = oneshot::channel();
        writer.consider(CommandEnvelope { command, reply });
        outcome
    }

    fn create_command(raw: &str) -> Result<StreamCommand, Box<dyn std::error::Error>> {
        Ok(StreamCommand::Create {
            path: DirectoryKey::try_from(Box::<[u8]>::from(raw.as_bytes()))?,
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Open,
        })
    }

    #[tokio::test]
    async fn idempotent_duplicate_behind_flight_waits_for_that_run()
    -> Result<(), Box<dyn std::error::Error>> {
        let adapter = ObjectStoreAdapter::in_memory();
        let (mut writer, _view) = writer(adapter.clone()).await?;
        let partition = writer.partition;
        let batch_2 = writer.next_batch;
        let initial = create_command("events/a")?;
        let initial = admitted_command(&mut writer, &initial, batch_2)?;
        assert_eq!(
            CreateEvidence::Direct,
            writer.wal_store.create_wal(&initial.candidate).await?
        );
        install_flight(
            &mut writer,
            batch_2,
            initial.command,
            initial.candidate,
            CreateEvidence::DurableMatch,
        );
        let _initial_reply = initial.reply;

        let mut duplicate = consider(&mut writer, create_command("events/a")?);
        assert_eq!(
            1,
            writer.pending.len(),
            "the idempotent duplicate queues behind the active flight"
        );
        assert!(
            duplicate.try_recv().is_err(),
            "an idempotent reply depending on an uncommitted fact inherits that fact's barrier"
        );

        let (sender, ingress) = mpsc::channel(WRITER_INGRESS_COMMANDS_MAX);
        drop(sender);
        assert_eq!(WriterExit::Shutdown, writer.run(ingress).await);
        assert_eq!(
            StreamReply::AlreadyCreated {
                uid: StreamUid::try_from(1)?
            },
            duplicate.await??
        );

        let grouped = WalStore::new(adapter)
            .read_wal(partition, BatchId::try_from(3)?)
            .await?;
        assert!(
            grouped.is_none(),
            "an idempotent-only pending set creates no second WAL coordinate"
        );
        Ok(())
    }

    #[tokio::test]
    async fn suffix_exhaustion_sheds_new_facts_but_answers_idempotent_retries()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut writer, _view) = writer(ObjectStoreAdapter::in_memory()).await?;
        let created = consider(&mut writer, create_command("events/a")?);
        complete_active_wal(&mut writer).await?;
        assert!(matches!(created.await?, Ok(StreamReply::Created { .. })));

        writer.next_batch = BatchId::try_from(WAL_SUFFIX_COORDINATES_MAX_V2)?;
        let shed = consider(&mut writer, create_command("events/b")?);
        assert_eq!(
            Err(AdmissionRefusal::Overloaded),
            shed.await?,
            "a new fact needs a WAL coordinate the exhausted suffix cannot grant"
        );
        assert!(
            writer.checkpoint_requested,
            "shedding at the suffix bound requests the recovering checkpoint"
        );

        let duplicate = consider(&mut writer, create_command("events/a")?);
        assert_eq!(
            Ok(StreamReply::AlreadyCreated {
                uid: StreamUid::try_from(1)?
            }),
            duplicate.await?,
            "an idempotent retry consumes no WAL coordinate and stays answerable"
        );
        Ok(())
    }
}
