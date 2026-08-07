//! Single-owner writer I/O orchestration and publication ordering.

mod state;

use strom_object_store::CreateEvidence;
use strom_storage_domain::BatchId;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::bootstrap::Ready;
use crate::checkpoint::{CheckpointOutcome, PublicationGate, execute_checkpoint};
use crate::collection::collect_advance;
use crate::engine::PublishedView;
use crate::store::{EncodedWal, TypedStoreError, WalStore};

use state::{AdmissionDecision, CheckpointPlan, CheckpointTicket, Completion, FlightDecision};
pub(crate) use state::{AdmissionRefusal, CommandEnvelope, CreateStream, WriterState};

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

struct Writer {
    state: WriterState,
    flight: Option<Flight>,
    checkpoint: Option<CheckpointFlight>,
    collector: Option<JoinHandle<()>>,
    adapter: strom_object_store::ObjectStoreAdapter,
    wal_store: WalStore,
    view: watch::Sender<PublishedView>,
}

struct Flight {
    encoded: EncodedWal,
    task: JoinHandle<Result<CreateEvidence, TypedStoreError>>,
}

struct CheckpointFlight {
    ticket: CheckpointTicket,
    publication: PublicationGate,
    task: JoinHandle<CheckpointOutcome>,
}

enum WriterEvent {
    Flight(Result<Result<CreateEvidence, TypedStoreError>, tokio::task::JoinError>),
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
        Self {
            state: ready.into_state(),
            flight: None,
            checkpoint: None,
            collector: None,
            wal_store: WalStore::new(adapter.clone()),
            adapter,
            view,
        }
    }

    async fn run(mut self, mut ingress: mpsc::Receiver<CommandEnvelope>) -> WriterExit {
        let mut ingress_open = true;
        loop {
            self.assert_effect_records();
            self.reap_collector();
            self.take_flight();
            if ingress_open {
                self.start_checkpoint();
            }
            if self.flight.is_none() && self.checkpoint.is_none() && !ingress_open {
                assert!(
                    self.state.is_quiescent(),
                    "shutdown exits only after every admitted command is settled"
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
                (None, None, false) => return self.terminate(WriterExit::Shutdown).await,
            };

            match event {
                WriterEvent::Flight(completion) => {
                    let evidence = match completion {
                        Ok(evidence) => evidence,
                        Err(join_error) => {
                            let flight = self
                                .flight
                                .take()
                                .expect("a WAL completion retains its shell flight");
                            let batch = flight.encoded.batch();
                            self.state.discard_wal_flight(batch);
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
                    let checkpoint = self
                        .checkpoint
                        .take()
                        .expect("a checkpoint completion retains its shell flight");
                    let outcome = match completion {
                        Ok(outcome) => outcome,
                        Err(join_error) => {
                            self.state.abandon_checkpoint(checkpoint.ticket);
                            let exit = WriterExit::Contradiction {
                                batch: checkpoint.ticket.cut,
                                detail: format!("checkpoint task failed: {join_error}"),
                            };
                            return self.terminate(exit).await;
                        }
                    };
                    if let Err(exit) = self.complete_checkpoint(checkpoint.ticket, outcome) {
                        return self.terminate(exit).await;
                    }
                }
                WriterEvent::Command(Some(envelope)) => match self.state.admit(envelope) {
                    AdmissionDecision::Settled(completion) => send_completion(completion),
                    AdmissionDecision::Queued => {}
                },
                WriterEvent::Command(None) => {
                    ingress_open = false;
                    if let Some(checkpoint) = &self.checkpoint {
                        checkpoint.publication.cancel_before_publish();
                    }
                }
            }
        }
    }

    fn take_flight(&mut self) {
        match self.state.take_flight() {
            FlightDecision::Run(encoded) => {
                assert!(
                    self.flight.is_none(),
                    "a state WAL run has no existing shell flight"
                );
                let store = self.wal_store.clone();
                let candidate = encoded.clone();
                let task = tokio::spawn(async move { store.create_wal(&candidate).await });
                self.flight = Some(Flight { encoded, task });
            }
            FlightDecision::Replies(replies) => send_replies(replies),
            FlightDecision::Idle => {}
        }
        assert_eq!(
            self.flight.is_some(),
            self.state.has_flight(),
            "the shell and state agree on the active WAL flight"
        );
    }

    async fn complete_flight(
        &mut self,
        evidence: Result<CreateEvidence, TypedStoreError>,
    ) -> Result<(), WriterExit> {
        let flight = self
            .flight
            .take()
            .expect("only an active shell WAL flight can complete");
        let batch = flight.encoded.batch();
        let outcome = match evidence {
            Ok(CreateEvidence::Direct | CreateEvidence::DurableMatch) => {
                self.record_wal_durable(batch);
                Ok(())
            }
            Ok(CreateEvidence::NotOurs) => Err(WriterExit::Fenced { batch }),
            Ok(CreateEvidence::Unresolved) => {
                let observed = self.wal_store.read_wal(self.state.partition(), batch).await;
                match observed {
                    Ok(Some(observed)) if observed.as_slice() == flight.encoded.as_slice() => {
                        self.record_wal_durable(batch);
                        Ok(())
                    }
                    Ok(Some(_foreign)) => Err(WriterExit::Fenced { batch }),
                    Ok(None) => Err(WriterExit::Poisoned {
                        batch,
                        detail: "unresolved WAL create is absent on its one reconciliation read"
                            .into(),
                    }),
                    Err(
                        TypedStoreError::Retryable { detail }
                        | TypedStoreError::Rejected { detail },
                    ) => Err(WriterExit::Poisoned { batch, detail }),
                    Err(TypedStoreError::Contradiction { detail }) => {
                        Err(WriterExit::Contradiction { batch, detail })
                    }
                }
            }
            Err(TypedStoreError::Retryable { detail } | TypedStoreError::Rejected { detail }) => {
                Err(WriterExit::Poisoned { batch, detail })
            }
            Err(TypedStoreError::Contradiction { detail }) => {
                Err(WriterExit::Contradiction { batch, detail })
            }
        };
        match outcome {
            Ok(()) => Ok(()),
            Err(exit) => {
                self.state.discard_wal_flight(batch);
                Err(exit)
            }
        }
    }

    fn record_wal_durable(&mut self, batch: BatchId) {
        let durable = self.state.record_wal_durable(batch);
        self.view.send_replace(PublishedView::new(durable.forest));
        assert_eq!(
            self.view.borrow().forest(),
            self.state.durable_forest(),
            "the durable forest is published before its replies are released"
        );
        send_replies(durable.replies);
        assert!(
            !self.state.has_flight(),
            "recording WAL durability clears the state flight"
        );
    }

    fn start_checkpoint(&mut self) {
        let Some(CheckpointPlan { input, ticket }) = self.state.take_checkpoint() else {
            return;
        };
        assert!(
            self.checkpoint.is_none(),
            "a checkpoint plan has no existing shell flight"
        );
        let publication = PublicationGate::new();
        let task = tokio::spawn(execute_checkpoint(
            self.adapter.clone(),
            input,
            publication.clone(),
        ));
        self.checkpoint = Some(CheckpointFlight {
            ticket,
            publication,
            task,
        });
        self.assert_effect_records();
    }

    fn complete_checkpoint(
        &mut self,
        ticket: CheckpointTicket,
        outcome: CheckpointOutcome,
    ) -> Result<(), WriterExit> {
        let completion = match outcome {
            CheckpointOutcome::Abandoned => {
                self.state.abandon_checkpoint(ticket);
                Ok(())
            }
            CheckpointOutcome::Contradiction { cut, detail } => {
                assert_eq!(
                    ticket.cut, cut,
                    "checkpoint contradiction names its plan cut"
                );
                Err(WriterExit::Contradiction { batch: cut, detail })
            }
            CheckpointOutcome::Seal { prepared, evidence } => match evidence {
                Ok(CreateEvidence::Direct) => {
                    let installed = self
                        .state
                        .install_checkpoint(ticket, (*prepared).into_install());
                    self.view.send_replace(PublishedView::new(installed.forest));
                    if self.collector.is_none() {
                        let adapter = self.adapter.clone();
                        self.collector = Some(tokio::spawn(collect_advance(
                            adapter,
                            installed.source,
                            installed.successor,
                        )));
                    }
                    Ok(())
                }
                Ok(CreateEvidence::DurableMatch | CreateEvidence::NotOurs) => {
                    Err(WriterExit::Fenced { batch: ticket.cut })
                }
                Ok(CreateEvidence::Unresolved) => Err(WriterExit::Poisoned {
                    batch: ticket.cut,
                    detail: "advancing Seal create is unresolved".into(),
                }),
                Err(TypedStoreError::Contradiction { detail }) => Err(WriterExit::Contradiction {
                    batch: ticket.cut,
                    detail,
                }),
                Err(
                    TypedStoreError::Retryable { detail } | TypedStoreError::Rejected { detail },
                ) => Err(WriterExit::Poisoned {
                    batch: ticket.cut,
                    detail,
                }),
            },
        };
        match completion {
            Ok(()) => Ok(()),
            Err(exit) => {
                self.state.abandon_checkpoint(ticket);
                Err(exit)
            }
        }
    }

    async fn terminate(&mut self, exit: WriterExit) -> WriterExit {
        if let Some(collector) = self.collector.take() {
            collector.abort();
        }

        if let Some(checkpoint) = self.checkpoint.take() {
            checkpoint.publication.cancel_before_publish();
            let _resolved_publication = checkpoint.task.await;
            self.state.abandon_checkpoint(checkpoint.ticket);
        }

        if self.flight.is_some() {
            let completion = {
                let flight = self
                    .flight
                    .as_mut()
                    .expect("terminal draining observes the active WAL flight");
                (&mut flight.task).await
            };
            match completion {
                Ok(evidence) => {
                    let _classified_wal = self.complete_flight(evidence).await;
                }
                Err(_join_error) => {
                    let flight = self
                        .flight
                        .take()
                        .expect("terminal WAL join failure retains its shell flight");
                    self.state.discard_wal_flight(flight.encoded.batch());
                }
            }
        }

        exit
    }

    fn reap_collector(&mut self) {
        if self.collector.as_ref().is_some_and(JoinHandle::is_finished) {
            drop(self.collector.take());
        }
    }

    fn assert_effect_records(&self) {
        assert_eq!(
            self.flight.is_some(),
            self.state.has_flight(),
            "the shell and state agree on the active WAL flight"
        );
        assert_eq!(
            self.checkpoint.as_ref().map(|flight| flight.ticket),
            self.state.active_checkpoint(),
            "the shell and state agree on the active checkpoint ticket"
        );
    }
}

fn send_replies(replies: Vec<Completion>) {
    for reply in replies {
        send_completion(reply);
    }
}

fn send_completion(completion: Completion) {
    match completion {
        Completion::Create { outcome, reply } => {
            let _receiver_may_be_gone = reply.send(outcome);
        }
        Completion::Close { outcome, reply } => {
            let _receiver_may_be_gone = reply.send(outcome);
        }
        Completion::Delete { outcome, reply } => {
            let _receiver_may_be_gone = reply.send(outcome);
        }
    }
}
