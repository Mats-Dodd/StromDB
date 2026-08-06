//! Single-owner writer, bounded group commit, and publication ordering.

use strom_object_store::CreateEvidence;
use strom_storage_domain::{
    BatchId, OperationFact, PartitionId, WAL_RUN_FACTS_MAX, WalBody, WalFacts, WalObject,
    WalReplayPoint,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::admission::{admit, decide_suffix_room};
use crate::bootstrap::{AuthoredClaim, Ready};
use crate::partition::PublishedView;
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
    cut: WalReplayPoint,
    admitted: Forest,
    durable: Forest,
    pending: Vec<PendingCommand>,
    spare: Vec<PendingCommand>,
    flight: Option<Flight>,
    next_batch: BatchId,
    wal_store: WalStore,
    view: watch::Sender<PublishedView>,
}

struct PendingCommand {
    fact: OperationFact,
    reply: StreamReply,
    waiter: oneshot::Sender<Result<StreamReply, AdmissionRefusal>>,
}

struct Flight {
    batch: BatchId,
    commands: Vec<PendingCommand>,
    encoded: EncodedWal,
    task: JoinHandle<Result<CreateEvidence, WalStoreError>>,
}

enum WriterEvent {
    Flight(Result<Result<CreateEvidence, WalStoreError>, tokio::task::JoinError>),
    Command(Option<CommandEnvelope>),
}

pub(crate) fn spawn_writer(
    adapter: strom_object_store::ObjectStoreAdapter,
    partition: PartitionId,
    ready: Ready,
    ingress: mpsc::Receiver<CommandEnvelope>,
    view: watch::Sender<PublishedView>,
) -> JoinHandle<WriterExit> {
    let writer = Writer::new(adapter, partition, ready, view);
    tokio::spawn(writer.run(ingress))
}

impl Writer {
    fn new(
        adapter: strom_object_store::ObjectStoreAdapter,
        partition: PartitionId,
        ready: Ready,
        view: watch::Sender<PublishedView>,
    ) -> Self {
        let (claim, cut, forest, next_batch) = ready.into_writer_parts();
        Self {
            partition,
            claim,
            cut,
            admitted: forest.clone(),
            durable: forest,
            pending: Vec::with_capacity(WAL_RUN_FACTS_MAX),
            spare: Vec::with_capacity(WAL_RUN_FACTS_MAX),
            flight: None,
            next_batch,
            wal_store: WalStore::new(adapter),
            view,
        }
    }

    async fn run(mut self, mut ingress: mpsc::Receiver<CommandEnvelope>) -> WriterExit {
        let mut ingress_open = true;
        loop {
            if self.flight.is_none() && !self.pending.is_empty() {
                self.promote_pending();
            }
            if self.flight.is_none() && !ingress_open {
                assert!(
                    self.pending.is_empty(),
                    "shutdown exits only after every admitted command is promoted"
                );
                return WriterExit::Shutdown;
            }

            let event = if let Some(flight) = self.flight.as_mut() {
                if ingress_open {
                    tokio::select! {
                        biased;
                        completion = &mut flight.task => WriterEvent::Flight(completion),
                        command = ingress.recv() => WriterEvent::Command(command),
                    }
                } else {
                    WriterEvent::Flight((&mut flight.task).await)
                }
            } else {
                WriterEvent::Command(ingress.recv().await)
            };

            match event {
                WriterEvent::Flight(completion) => {
                    let evidence = completion
                        .expect("the WAL create task completes without cancellation or panic");
                    if let Err(exit) = self.complete_flight(evidence).await {
                        return exit;
                    }
                }
                WriterEvent::Command(Some(envelope)) => self.consider(envelope),
                WriterEvent::Command(None) => ingress_open = false,
            }
        }
    }

    fn consider(&mut self, envelope: CommandEnvelope) {
        let batch = self.next_batch;
        if self.pending.len() == WAL_RUN_FACTS_MAX
            || !decide_suffix_room(replay_batch(self.cut), batch)
        {
            send_refusal(envelope.reply, AdmissionRefusal::Overloaded);
            return;
        }

        let admitted = match admit(&self.admitted, &envelope.command, batch) {
            Ok(admitted) => admitted,
            Err(refusal) => {
                send_refusal(envelope.reply, refusal);
                return;
            }
        };
        self.admitted = admitted.forest;
        self.pending.push(PendingCommand {
            fact: admitted.fact,
            reply: admitted.reply,
            waiter: envelope.reply,
        });
        if self.flight.is_none() {
            self.promote_pending();
        }
    }

    fn promote_pending(&mut self) {
        assert!(
            self.flight.is_none(),
            "only one WAL create may be in flight"
        );
        assert!(!self.pending.is_empty(), "only a nonempty RUN is promoted");
        let batch = self.next_batch;
        let commands = std::mem::replace(&mut self.pending, std::mem::take(&mut self.spare));
        let facts = pending_facts(
            commands
                .iter()
                .map(|command| command.fact.clone())
                .collect(),
        );
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
                let observed = self.wal_store.read_wal(flight.encoded.identity()).await;
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

    fn commit(&mut self, mut flight: Flight) {
        for command in &flight.commands {
            assert_eq!(
                Ok(Applied),
                self.durable.strict_fold(flight.batch, &command.fact),
                "durable fold repeats facts already proven against admitted state"
            );
        }
        self.view.send_replace(PublishedView::new(
            self.claim.identity(),
            self.cut,
            self.durable.clone(),
        ));
        for command in flight.commands.drain(..) {
            let _receiver_may_be_gone = command.waiter.send(Ok(command.reply));
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
    use strom_domain::{ExpiryPolicy, StreamContentType};
    use strom_object_store::ObjectStoreAdapter;
    use strom_storage_domain::{DirectoryKey, StreamUid, WalIdentity};

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
            .read_wal(WalIdentity::new(partition(), BatchId::try_from(3)?))
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

    async fn writer(
        adapter: ObjectStoreAdapter,
    ) -> Result<(Writer, watch::Receiver<PublishedView>), Box<dyn std::error::Error>> {
        let ready = bootstrap(adapter.clone(), partition()).await?;
        let initial = PublishedView::new(
            ready.claim().identity(),
            ready.replay(),
            ready.forest().clone(),
        );
        let (view, receiver) = watch::channel(initial);
        Ok((Writer::new(adapter, partition(), ready, view), receiver))
    }

    fn admitted_command(
        writer: &mut Writer,
        command: &StreamCommand,
        batch: BatchId,
    ) -> Result<TestAdmission, Box<dyn std::error::Error>> {
        let admitted = admit(&writer.admitted, command, batch)?;
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
                fact: admitted.fact,
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
        })
    }

    fn partition() -> PartitionId {
        "00112233-4455-6677-8899-aabbccddeeff"
            .parse()
            .expect("test partition is canonical")
    }
}
