//! Thin async interpreter for the pure writer machine.

use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::Arc;

use strom_common::MonotonicClock;
use strom_object_store::ObjectStoreAdapter;
use strom_storage_protocol::{
    Completion, EffectKey, WRITER_OUTPUTS_PER_STEP_MAX, WriterAction, WriterEffect, WriterEvent,
    WriterExit, WriterMachine, WriterOutput, WriterRecovery, WriterStep,
};
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinError, JoinHandle};
use tokio_util::task::JoinMap;

use crate::checkpoint;
use crate::engine::{Options, PublishedView};
use crate::store::{SealStore, TableStore, WalStore};

struct Writer {
    machine: WriterMachine,
    seal_store: SealStore,
    wal_store: WalStore,
    table_store: TableStore,
    view: watch::Sender<PublishedView>,
    effects: JoinMap<EffectKey, WriterEvent>,
    clock: Arc<dyn MonotonicClock>,
    flush_timer: Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
}

pub(crate) fn spawn_writer(
    adapter: ObjectStoreAdapter,
    recovery: WriterRecovery,
    ingress: mpsc::Receiver<strom_storage_protocol::CommandEnvelope>,
    view: watch::Sender<PublishedView>,
    clock: Arc<dyn MonotonicClock>,
    options: Options,
) -> JoinHandle<WriterExit> {
    let writer = Writer::new(adapter, recovery, view, clock, options);
    tokio::spawn(writer.run(ingress))
}

impl Writer {
    fn new(
        adapter: ObjectStoreAdapter,
        recovery: WriterRecovery,
        view: watch::Sender<PublishedView>,
        clock: Arc<dyn MonotonicClock>,
        options: Options,
    ) -> Self {
        Self {
            machine: WriterMachine::from_recovery(
                recovery,
                options.flush_interval_min(),
                options.flush_buffer_bytes(),
            ),
            seal_store: SealStore::new(adapter.clone()),
            wal_store: WalStore::new(adapter.clone()),
            table_store: TableStore::new(adapter),
            view,
            effects: JoinMap::new(),
            clock,
            flush_timer: None,
        }
    }

    async fn run(
        mut self,
        mut ingress: mpsc::Receiver<strom_storage_protocol::CommandEnvelope>,
    ) -> WriterExit {
        let startup = self.machine.handle(self.clock.now(), WriterEvent::Started);
        if let Some(exit) = self.execute_step(startup) {
            return exit;
        }
        loop {
            let event = tokio::select! {
                biased;
                joined = self.effects.join_next(), if !self.effects.is_empty() => {
                    let joined = joined.expect("a nonempty JoinMap has a task");
                    joined_event(joined)
                }
                () = wait_for_flush(&mut self.flush_timer) => {
                    drop(self.flush_timer.take());
                    WriterEvent::FlushDue
                }
                command = ingress.recv(), if self.machine.admission_open() => if let Some(command) = command {
                    WriterEvent::Command(command)
                } else {
                    WriterEvent::IngressClosed
                },
                else => panic!("a live writer has ingress or an outstanding effect"),
            };
            let step = self.machine.handle(self.clock.now(), event);
            if let Some(exit) = self.execute_step(step) {
                return exit;
            }
        }
    }

    fn execute_step(&mut self, step: WriterStep) -> Option<WriterExit> {
        let (outputs, exit) = step.into_parts();
        assert!(
            outputs.len() <= WRITER_OUTPUTS_PER_STEP_MAX,
            "the interpreter receives a step inside the named output bound"
        );
        if exit.is_some() {
            assert!(
                outputs
                    .iter()
                    .all(|output| matches!(output, WriterOutput::Action(_))),
                "a terminal step never starts completion-producing work"
            );
        }
        for output in outputs {
            match output {
                WriterOutput::Effect(effect) => self.spawn_effect(effect),
                WriterOutput::Action(action) => self.execute_action(action),
            }
        }
        exit
    }

    fn spawn_effect(&mut self, effect: WriterEffect) {
        let key = effect.key();
        assert!(
            !self.effects.contains_key(&key),
            "an effect key is absent before the machine issues it"
        );
        match effect {
            WriterEffect::EstablishWal(candidate) => {
                let store = self.wal_store.clone();
                let batch = candidate.batch();
                self.effects.spawn(key, async move {
                    WriterEvent::WalEstablished {
                        batch,
                        result: store.establish_wal(&candidate).await,
                    }
                });
            }
            WriterEffect::PrepareCheckpoint(input) => {
                let store = self.table_store.clone();
                let ticket = input.ticket();
                self.effects.spawn(key, async move {
                    WriterEvent::CheckpointPrepared {
                        ticket,
                        outcome: checkpoint::prepare(store, input).await,
                    }
                });
            }
            WriterEffect::PublishAuthority { ticket, candidate } => {
                let store = self.seal_store.clone();
                self.effects.spawn(key, async move {
                    WriterEvent::SealPublished {
                        ticket,
                        result: store.publish_authority(&candidate).await,
                    }
                });
            }
            WriterEffect::Collect(input) => {
                let cut = input.cut();
                let wal_store = self.wal_store.clone();
                let table_store = self.table_store.clone();
                self.effects.spawn(key, async move {
                    checkpoint::collect(wal_store, table_store, input).await;
                    WriterEvent::CollectFinished { cut }
                });
            }
        }
    }

    fn execute_action(&mut self, action: WriterAction) {
        match action {
            WriterAction::ArmFlush { deadline } => {
                assert!(
                    self.flush_timer.is_none(),
                    "the writer interpreter owns at most one flush timer"
                );
                self.flush_timer = Some(self.clock.sleep_until(deadline));
            }
            WriterAction::PublishView(forest) => {
                self.view.send_replace(PublishedView::new(forest));
            }
            WriterAction::SendReplies(replies) => send_replies(replies),
            WriterAction::CancelCheckpointPreparation { ticket } => {
                let key = EffectKey::CheckpointPreparation { ticket };
                assert!(
                    self.effects.abort(&key),
                    "checkpoint cancellation targets its exact present preparation"
                );
            }
        }
    }
}

async fn wait_for_flush(timer: &mut Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>) {
    match timer {
        Some(timer) => timer.await,
        None => pending::<()>().await,
    }
}

#[expect(
    clippy::panic,
    reason = "task panic and unexpected cancellation are interpreter invariant failures"
)]
fn joined_event((key, joined): (EffectKey, Result<WriterEvent, JoinError>)) -> WriterEvent {
    match (key, joined) {
        (_, Ok(event)) => event,
        (EffectKey::CheckpointPreparation { ticket }, Err(error)) if error.is_cancelled() => {
            WriterEvent::CheckpointPreparationCancelled { ticket }
        }
        (
            failed @ (EffectKey::Wal { .. }
            | EffectKey::CheckpointPreparation { .. }
            | EffectKey::SealPublication { .. }
            | EffectKey::Collection { .. }),
            Err(error),
        ) => panic!("writer effect {failed:?} failed: {error}"),
    }
}

fn send_replies(replies: Vec<Completion>) {
    for reply in replies {
        match reply {
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
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::time::Duration;

    use strom_common::MonotonicInstant;
    use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle, StreamPath};
    use strom_storage_domain::{
        BatchId, OwnerToken, Seal, SealGeneration, TreeVersion, WalBody, WalObject, WalReplayPoint,
    };
    use strom_storage_protocol::{
        BootstrapEffect, BootstrapEvent, BootstrapMachine, BootstrapStep, CommandEnvelope,
        CreateStream, SealPublication, WalEstablishment,
    };
    use tokio::sync::oneshot;

    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    mod scripts {
        use super::*;

        #[tokio::test]
        async fn cancelled_preparation_join_retains_its_exact_key() -> TestResult {
            let ticket = preparation_ticket()?;
            let key = EffectKey::CheckpointPreparation { ticket };
            let mut effects = JoinMap::new();
            effects.spawn(key, pending::<WriterEvent>());
            assert!(effects.abort(&key));
            let joined = effects
                .join_next()
                .await
                .expect("the cancelled preparation remains joinable");
            assert!(matches!(
                joined_event(joined),
                WriterEvent::CheckpointPreparationCancelled { ticket: observed }
                    if observed == ticket
            ));
            assert!(effects.is_empty());
            Ok(())
        }

        #[tokio::test]
        #[should_panic(expected = "writer effect Wal")]
        async fn cancellation_of_a_non_preparation_effect_panics_with_its_key() {
            let key = EffectKey::Wal {
                batch: BatchId::try_from(7).expect("seven is nonzero"),
            };
            let mut effects = JoinMap::new();
            effects.spawn(key, pending::<WriterEvent>());
            assert!(effects.abort(&key));
            let joined = effects
                .join_next()
                .await
                .expect("the cancelled WAL remains joinable");
            drop(joined_event(joined));
        }

        #[test]
        fn successful_preparation_completion_survives_an_abort_lost_race() -> TestResult {
            let ticket = preparation_ticket()?;
            let key = EffectKey::CheckpointPreparation { ticket };
            let event = joined_event((
                key,
                Ok(WriterEvent::CheckpointPrepared {
                    ticket,
                    outcome: strom_storage_protocol::PreparationOutcome::Abandoned,
                }),
            ));
            assert!(matches!(
                event,
                WriterEvent::CheckpointPrepared {
                    ticket: observed,
                    outcome: strom_storage_protocol::PreparationOutcome::Abandoned,
                } if observed == ticket
            ));
            Ok(())
        }

        #[tokio::test]
        #[should_panic(expected = "SealPublication")]
        #[expect(
            clippy::panic,
            reason = "the interpreter test injects one panicked task"
        )]
        async fn task_panic_reports_its_exact_effect_key() {
            let ticket = preparation_ticket().expect("the fixture reaches a checkpoint");
            let key = EffectKey::SealPublication { ticket };
            let mut effects = JoinMap::new();
            effects.spawn(key, async move {
                panic!("scripted effect panic");
            });
            let joined = effects
                .join_next()
                .await
                .expect("the panicked task remains joinable");
            drop(joined_event(joined));
        }
    }

    fn preparation_ticket()
    -> Result<strom_storage_protocol::CheckpointTicket, Box<dyn std::error::Error>> {
        let mut machine = machine()?;
        for ordinal in 0..strom_storage_domain::WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER {
            let (reply, _outcome) = oneshot::channel();
            let step = machine.handle(
                MonotonicInstant::ZERO,
                WriterEvent::Command(CommandEnvelope::Create {
                    command: CreateStream {
                        path: path(&format!("events/ticket-{ordinal}"))?,
                        content_type: StreamContentType::octet_stream(),
                        expiry: ExpiryPolicy::None,
                        lifecycle: StreamLifecycle::Open,
                    },
                    reply,
                }),
            );
            let batch = wal_batch(step).ok_or("each create issues one WAL")?;
            let step = machine.handle(
                MonotonicInstant::ZERO,
                WriterEvent::WalEstablished {
                    batch,
                    result: Ok(WalEstablishment::Durable),
                },
            );
            if let Some(ticket) = preparation_key(step) {
                return Ok(ticket);
            }
        }
        Err("the checkpoint threshold issues a preparation".into())
    }

    fn machine() -> Result<WriterMachine, Box<dyn std::error::Error>> {
        let mut machine = recovered_machine()?;
        let (outputs, exit) = machine
            .handle(MonotonicInstant::ZERO, WriterEvent::Started)
            .into_parts();
        assert!(outputs.is_empty());
        assert_eq!(None, exit);
        Ok(machine)
    }

    fn recovered_machine() -> Result<WriterMachine, Box<dyn std::error::Error>> {
        let partition = "00112233-4455-6677-8899-aabbccddeeff".parse()?;
        let generation = SealGeneration::genesis();
        let claim_generation = generation.successor()?;
        let durable = BatchId::try_from(1)?;
        let mut bootstrap = BootstrapMachine::new();
        drop(bootstrap.handle(BootstrapEvent::Started {
            genesis_partition: partition,
        }));
        drop(bootstrap.handle(BootstrapEvent::HeadObserved(Some(generation))));
        let seal = Seal::new(
            partition,
            generation,
            WalReplayPoint::Genesis,
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;
        drop(bootstrap.handle(BootstrapEvent::SealRead(Some(seal))));
        let step = bootstrap.handle(BootstrapEvent::ClaimPublished(SealPublication::Authored));
        assert!(matches!(
            step,
            BootstrapStep::Effect(BootstrapEffect::ObserveWalTail)
        ));
        drop(bootstrap.handle(BootstrapEvent::WalTailObserved(None)));
        drop(bootstrap.handle(BootstrapEvent::FenceEstablished(WalEstablishment::Durable)));
        drop(
            bootstrap.handle(BootstrapEvent::WalRead(Some(WalObject::new(
                partition,
                durable,
                OwnerToken::from(claim_generation),
                WalBody::Fence,
            )))),
        );
        let BootstrapStep::Complete(recovery) =
            bootstrap.handle(BootstrapEvent::HeadObserved(Some(claim_generation)))
        else {
            return Err("writer fixture reaches complete bootstrap".into());
        };
        Ok(WriterMachine::from_recovery(
            recovery,
            Duration::ZERO,
            strom_storage_domain::WAL_ENCODED_BYTES_MAX,
        ))
    }

    fn path(raw: &str) -> Result<StreamPath, Box<dyn std::error::Error>> {
        Ok(raw.parse()?)
    }

    fn wal_batch(step: WriterStep) -> Option<BatchId> {
        let (outputs, exit) = step.into_parts();
        assert_eq!(None, exit);
        outputs.into_iter().find_map(|output| match output {
            WriterOutput::Effect(WriterEffect::EstablishWal(candidate)) => Some(candidate.batch()),
            WriterOutput::Effect(
                WriterEffect::PrepareCheckpoint(_)
                | WriterEffect::PublishAuthority { .. }
                | WriterEffect::Collect(_),
            )
            | WriterOutput::Action(_) => None,
        })
    }

    fn preparation_key(step: WriterStep) -> Option<strom_storage_protocol::CheckpointTicket> {
        let (outputs, exit) = step.into_parts();
        assert_eq!(None, exit);
        outputs.into_iter().find_map(|output| match output {
            WriterOutput::Effect(WriterEffect::PrepareCheckpoint(input)) => Some(input.ticket()),
            WriterOutput::Effect(
                WriterEffect::EstablishWal(_)
                | WriterEffect::PublishAuthority { .. }
                | WriterEffect::Collect(_),
            )
            | WriterOutput::Action(_) => None,
        })
    }
}
