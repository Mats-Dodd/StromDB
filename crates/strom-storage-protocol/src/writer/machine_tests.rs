//! Event-script claims for the writer machine seam.

#![expect(
    clippy::panic,
    reason = "test destructuring panics identify malformed machine outputs"
)]

use std::num::NonZeroU64;
use std::panic::{AssertUnwindSafe, catch_unwind};

use strom_domain::{
    CloseStreamOutcome, ExpiryPolicy, StreamContentType, StreamLifecycle, StreamTtl,
};
use strom_storage_domain::{
    AttemptId, BatchId, DirectoryKey, EncodedAuthoritySeal, OwnerToken, Seal, SealGeneration,
    StreamUid, TreeVersion, WAL_RUN_FACTS_MAX, WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER,
    WAL_SUFFIX_COORDINATES_MAX_V2, WalReplayPoint,
};
use tokio::sync::oneshot;

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const STALE_ATTEMPT_COUNTER: u64 = 99;

#[derive(Clone, Copy)]
enum CheckpointStage {
    Preparation,
    Publication,
    Cancellation,
}

#[derive(Clone, Copy)]
enum WalFailure {
    Occupied,
    Unresolved,
    Contradiction,
}

#[derive(Clone, Copy)]
enum SealFailure {
    NoAuthority,
    Unresolved,
    Retryable,
    Rejected,
    Contradiction,
}

impl WalFailure {
    fn result(self) -> Result<WalEstablishment, TypedStoreError> {
        match self {
            Self::Occupied => Ok(WalEstablishment::Occupied),
            Self::Unresolved => Ok(WalEstablishment::UnresolvedAbsent),
            Self::Contradiction => Err(TypedStoreError::Contradiction {
                detail: "scripted WAL contradiction".into(),
            }),
        }
    }
}

impl SealFailure {
    fn result(self) -> Result<SealPublication, TypedStoreError> {
        match self {
            Self::NoAuthority => Ok(SealPublication::NoAuthority),
            Self::Unresolved => Ok(SealPublication::Unresolved),
            Self::Retryable => Err(TypedStoreError::Retryable {
                detail: "scripted retryable publication".into(),
            }),
            Self::Rejected => Err(TypedStoreError::Rejected {
                detail: "scripted rejected publication".into(),
            }),
            Self::Contradiction => Err(TypedStoreError::Contradiction {
                detail: "scripted publication contradiction".into(),
            }),
        }
    }
}

mod scripts {
    use super::*;

    #[test]
    fn wal_durability_orders_publication_before_reply_release() -> TestResult {
        let mut machine = machine_at(1)?;
        let (command, mut reply) = create("events/ordered")?;
        let step = machine.handle(WriterEvent::Command(command));
        let batch = establish_wal(step)?;
        assert!(matches!(
            reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        let (outputs, exit) = machine
            .handle(WriterEvent::WalEstablished {
                batch,
                result: Ok(WalEstablishment::Durable),
            })
            .into_parts();
        assert_eq!(None, exit);
        assert!(matches!(
            outputs.as_slice(),
            [
                WriterOutput::Action(WriterAction::PublishView(_)),
                WriterOutput::Action(WriterAction::SendReplies(_))
            ]
        ));
        assert!(matches!(
            reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        Ok(())
    }

    #[test]
    fn closing_during_preparation_accepts_both_exact_termination_races() -> TestResult {
        let (mut cancelled, ticket, _input) = machine_with_preparation()?;
        let (outputs, exit) = cancelled.handle(WriterEvent::IngressClosed).into_parts();
        assert_eq!(None, exit);
        assert!(matches!(
            outputs.as_slice(),
            [WriterOutput::Action(
                WriterAction::CancelCheckpointPreparation { ticket: observed }
            )] if *observed == ticket
        ));
        let (outputs, exit) = cancelled
            .handle(WriterEvent::CheckpointPreparationCancelled { ticket })
            .into_parts();
        assert!(outputs.is_empty());
        assert_eq!(Some(WriterExit::Shutdown), exit);

        let (mut completed, ticket, input) = machine_with_preparation()?;
        let (outputs, exit) = completed.handle(WriterEvent::IngressClosed).into_parts();
        assert_eq!(None, exit);
        assert!(matches!(
            outputs.as_slice(),
            [WriterOutput::Action(WriterAction::CancelCheckpointPreparation {
                ticket: observed
            })] if *observed == ticket
        ));
        let prepared = prepared(input)?;
        let (outputs, exit) = completed
            .handle(WriterEvent::CheckpointPrepared {
                ticket,
                outcome: PreparationOutcome::Prepared(Box::new(prepared)),
            })
            .into_parts();
        assert!(
            outputs.is_empty(),
            "the raced preparation is never published"
        );
        assert_eq!(Some(WriterExit::Shutdown), exit);
        Ok(())
    }

    #[test]
    fn closure_after_preparation_waits_for_publication_then_installs_without_collection()
    -> TestResult {
        let (mut machine, ticket, input) = machine_with_preparation()?;
        let (outputs, exit) = machine
            .handle(WriterEvent::CheckpointPrepared {
                ticket,
                outcome: PreparationOutcome::Prepared(Box::new(prepared(input)?)),
            })
            .into_parts();
        assert_eq!(None, exit);
        assert!(matches!(
            outputs.as_slice(),
            [WriterOutput::Effect(WriterEffect::PublishAuthority { ticket: observed, .. })]
                if *observed == ticket
        ));
        let (outputs, exit) = machine.handle(WriterEvent::IngressClosed).into_parts();
        assert!(outputs.is_empty());
        assert_eq!(None, exit);

        let (outputs, exit) = machine
            .handle(WriterEvent::SealPublished {
                ticket,
                result: Ok(SealPublication::Authored),
            })
            .into_parts();
        assert!(matches!(
            outputs.as_slice(),
            [WriterOutput::Action(WriterAction::PublishView(_))]
        ));
        assert_eq!(Some(WriterExit::Shutdown), exit);
        Ok(())
    }

    #[test]
    fn wal_and_checkpoint_completions_are_legal_in_either_order() -> TestResult {
        for checkpoint_first in [false, true] {
            let (mut machine, ticket, input) = machine_with_preparation()?;
            let (command, _reply) = create(if checkpoint_first {
                "events/checkpoint-first"
            } else {
                "events/wal-first"
            })?;
            let batch = establish_wal(machine.handle(WriterEvent::Command(command)))?;

            if checkpoint_first {
                let publication = machine.handle(WriterEvent::CheckpointPrepared {
                    ticket,
                    outcome: PreparationOutcome::Prepared(Box::new(prepared(input)?)),
                });
                assert!(has_publication(publication, ticket));
                let durable = machine.handle(WriterEvent::WalEstablished {
                    batch,
                    result: Ok(WalEstablishment::Durable),
                });
                assert!(has_publish_then_replies(&durable));
            } else {
                let durable = machine.handle(WriterEvent::WalEstablished {
                    batch,
                    result: Ok(WalEstablishment::Durable),
                });
                assert!(has_publish_then_replies(&durable));
                let publication = machine.handle(WriterEvent::CheckpointPrepared {
                    ticket,
                    outcome: PreparationOutcome::Prepared(Box::new(prepared(input)?)),
                });
                assert!(has_publication(publication, ticket));
            }
        }
        Ok(())
    }

    #[test]
    fn occupied_collector_skips_a_new_advance_without_deferring_it() -> TestResult {
        let (mut machine, first_cut, ticket) = machine_with_collector_and_publication()?;
        let (outputs, exit) = machine
            .handle(WriterEvent::SealPublished {
                ticket,
                result: Ok(SealPublication::Authored),
            })
            .into_parts();
        assert_eq!(None, exit);
        assert!(matches!(
            outputs.as_slice(),
            [WriterOutput::Action(WriterAction::PublishView(_))]
        ));

        let (outputs, exit) = machine
            .handle(WriterEvent::CollectFinished { cut: first_cut })
            .into_parts();
        assert!(
            outputs.is_empty(),
            "a skipped collection is not queued later"
        );
        assert_eq!(None, exit);
        Ok(())
    }

    #[test]
    fn completed_collection_releases_budget_before_an_advancing_seal_is_selected() -> TestResult {
        let (mut machine, first_cut, ticket) = machine_with_collector_and_publication()?;
        assert_empty_step(machine.handle(WriterEvent::CollectFinished { cut: first_cut }));
        let cut = collection_cut(machine.handle(WriterEvent::SealPublished {
            ticket,
            result: Ok(SealPublication::Authored),
        }))?;
        assert_eq!(ticket.cut(), cut);
        assert_eq!(Some(cut), machine.collector);
        Ok(())
    }

    #[test]
    fn stale_cancellation_identity_is_a_protocol_violation() -> TestResult {
        let (mut machine, ticket, _input) = machine_with_preparation()?;
        let (outputs, exit) = machine.handle(WriterEvent::IngressClosed).into_parts();
        assert_eq!(None, exit);
        assert!(matches!(
            outputs.as_slice(),
            [WriterOutput::Action(WriterAction::CancelCheckpointPreparation {
                ticket: observed
            })] if *observed == ticket
        ));
        let wrong = CheckpointTicket {
            source: ticket.source,
            cut: ticket.cut,
            attempt: AttemptId::new(ticket.attempt.owner_claim(), STALE_ATTEMPT_COUNTER),
        };
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                machine.handle(WriterEvent::CheckpointPreparationCancelled { ticket: wrong });
            }))
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn checkpoint_attempts_are_suppressed_until_suffix_shedding_rearms_the_cut() -> TestResult {
        let (mut suppressed, first, _input) = machine_with_preparation()?;
        let (outputs, exit) = suppressed
            .handle(WriterEvent::CheckpointPrepared {
                ticket: first,
                outcome: PreparationOutcome::Abandoned,
            })
            .into_parts();
        assert!(outputs.is_empty());
        assert_eq!(None, exit);
        let (outputs, exit) = suppressed
            .handle(WriterEvent::Command(checkpoint_probe()?))
            .into_parts();
        assert_eq!(None, exit);
        assert!(take_preparation(outputs).is_none());

        let (mut retry, first, _input) =
            machine_with_preparation_at(WAL_SUFFIX_COORDINATES_MAX_V2 - 1)?;
        assert_empty_step(retry.handle(WriterEvent::CheckpointPrepared {
            ticket: first,
            outcome: PreparationOutcome::Abandoned,
        }));

        let (command, _outcome) = create("events/shed")?;
        let (outputs, exit) = retry.handle(WriterEvent::Command(command)).into_parts();
        assert_eq!(None, exit);
        let (second, _input) =
            take_preparation(outputs).ok_or("suffix shedding rearms the exact cut")?;
        assert_eq!(first.cut(), second.cut());
        assert_eq!(0, first.attempt().local_counter());
        assert_eq!(1, second.attempt().local_counter());
        Ok(())
    }

    #[test]
    fn duplicate_behind_a_flight_inherits_its_barrier_without_a_second_wal() -> TestResult {
        let mut machine = machine_at(1)?;
        let (first, mut created) = create("events/duplicate")?;
        let first_batch = establish_wal(machine.handle(WriterEvent::Command(first)))?;
        let (duplicate, mut duplicate_outcome) = create("events/duplicate")?;
        let (outputs, exit) = machine.handle(WriterEvent::Command(duplicate)).into_parts();
        assert!(outputs.is_empty());
        assert_eq!(None, exit);
        let next_batch = machine.next_batch;

        let step = machine.handle(WriterEvent::WalEstablished {
            batch: first_batch,
            result: Ok(WalEstablishment::Durable),
        });
        assert!(!has_wal(&step));
        execute_actions(step);
        assert_eq!(Ok(CreateOutcome::Created), created.try_recv()?);
        assert_eq!(
            Ok(CreateOutcome::AlreadyExists),
            duplicate_outcome.try_recv()?
        );
        assert_eq!(
            next_batch, machine.next_batch,
            "an idempotent barrier consumes no WAL coordinate"
        );

        let (duplicate, mut immediate) = create("events/duplicate")?;
        execute_actions(machine.handle(WriterEvent::Command(duplicate)));
        assert_eq!(Ok(CreateOutcome::AlreadyExists), immediate.try_recv()?);
        assert_eq!(next_batch, machine.next_batch);
        Ok(())
    }

    #[test]
    fn full_pending_barrier_refuses_before_admission() -> TestResult {
        let mut machine = machine_at(1)?;
        let (first, _created) = create("events/full")?;
        let _batch = establish_wal(machine.handle(WriterEvent::Command(first)))?;
        for _ordinal in 0..WAL_RUN_FACTS_MAX {
            let (duplicate, _outcome) = create("events/full")?;
            let (outputs, exit) = machine.handle(WriterEvent::Command(duplicate)).into_parts();
            assert!(outputs.is_empty());
            assert_eq!(None, exit);
        }

        let (overflow, mut outcome) = create("events/full")?;
        execute_actions(machine.handle(WriterEvent::Command(overflow)));
        assert_eq!(Err(AdmissionRefusal::Overloaded), outcome.try_recv()?);
        Ok(())
    }

    #[test]
    fn unissued_and_wrong_wal_completions_are_protocol_violations() -> TestResult {
        let mut machine = machine_at(1)?;
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                machine.handle(WriterEvent::WalEstablished {
                    batch: BatchId::try_from(2).expect("two is nonzero"),
                    result: Ok(WalEstablishment::Durable),
                });
            }))
            .is_err()
        );

        let (command, _outcome) = create("events/wrong-completion")?;
        let batch = establish_wal(machine.handle(WriterEvent::Command(command)))?;
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                machine.handle(WriterEvent::WalEstablished {
                    batch: batch
                        .successor()
                        .expect("the fixture batch has a successor"),
                    result: Ok(WalEstablishment::Durable),
                });
            }))
            .is_err()
        );
        let step = machine.handle(WriterEvent::WalEstablished {
            batch,
            result: Ok(WalEstablishment::Durable),
        });
        assert!(has_publish_then_replies(&step));
        Ok(())
    }

    #[test]
    fn startup_schedules_recovered_work_once_before_ingress() -> TestResult {
        let mut below = recovered_machine_at(WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER - 1)?;
        let (outputs, exit) = below.handle(WriterEvent::Started).into_parts();
        assert!(outputs.is_empty());
        assert_eq!(None, exit);

        let mut due = recovered_machine_at(WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER)?;
        let (outputs, exit) = due.handle(WriterEvent::Started).into_parts();
        assert_eq!(None, exit);
        let (ticket, _input) =
            take_preparation(outputs).ok_or("startup issues the due checkpoint")?;
        assert!(matches!(
            due.checkpoint,
            Some(CheckpointMarker::Preparing { ticket: active }) if active == ticket
        ));
        assert!(
            catch_unwind(AssertUnwindSafe(|| due.handle(WriterEvent::Started))).is_err(),
            "startup is a one-shot machine observation"
        );
        assert!(matches!(
            due.checkpoint,
            Some(CheckpointMarker::Preparing { ticket: active }) if active == ticket
        ));
        Ok(())
    }

    #[test]
    fn consecutive_wal_runs_publish_and_release_only_their_own_barriers() -> TestResult {
        let mut machine = machine_at(1)?;
        let (first, mut first_outcome) = create("events/first")?;
        let first_batch = establish_wal(machine.handle(WriterEvent::Command(first)))?;
        let (second, mut second_outcome) = create("events/second")?;
        let (outputs, exit) = machine.handle(WriterEvent::Command(second)).into_parts();
        assert!(outputs.is_empty());
        assert_eq!(None, exit);

        let (outputs, exit) = machine
            .handle(WriterEvent::WalEstablished {
                batch: first_batch,
                result: Ok(WalEstablishment::Durable),
            })
            .into_parts();
        assert_eq!(None, exit);
        assert!(matches!(
            outputs.as_slice(),
            [
                WriterOutput::Action(WriterAction::PublishView(_)),
                WriterOutput::Action(WriterAction::SendReplies(_)),
                WriterOutput::Effect(WriterEffect::EstablishWal(_))
            ]
        ));
        let mut effects = execute_outputs(outputs);
        let WriterEffect::EstablishWal(second_candidate) = effects
            .pop()
            .ok_or("the pending fact is promoted after first durability")?
        else {
            return Err("the promoted effect is a WAL".into());
        };
        assert!(effects.is_empty());
        assert_eq!(Ok(CreateOutcome::Created), first_outcome.try_recv()?);
        assert!(matches!(
            second_outcome.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(
            machine
                .durable_forest()
                .resolve(&path("events/first")?)
                .is_some()
        );
        assert!(
            machine
                .durable_forest()
                .resolve(&path("events/second")?)
                .is_none()
        );

        let step = machine.handle(WriterEvent::WalEstablished {
            batch: second_candidate.batch(),
            result: Ok(WalEstablishment::Durable),
        });
        assert!(has_publish_then_replies(&step));
        execute_actions(step);
        assert_eq!(Ok(CreateOutcome::Created), second_outcome.try_recv()?);
        assert!(
            machine
                .durable_forest()
                .resolve(&path("events/second")?)
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn admission_refusal_and_idempotence_axes_are_complete() -> TestResult {
        let mut machine = machine_at(1)?;
        let axes = path("events/axes")?;
        let (create, mut created) = create("events/axes")?;
        let batch = establish_wal(machine.handle(WriterEvent::Command(create)))?;
        execute_actions(machine.handle(WriterEvent::WalEstablished {
            batch,
            result: Ok(WalEstablishment::Durable),
        }));
        assert_eq!(Ok(CreateOutcome::Created), created.try_recv()?);
        let durable = machine.durable.clone();

        for command in [
            CreateStream {
                path: axes.clone(),
                content_type: "text/plain".parse()?,
                expiry: ExpiryPolicy::None,
                lifecycle: StreamLifecycle::Open,
            },
            CreateStream {
                path: axes.clone(),
                content_type: StreamContentType::octet_stream(),
                expiry: ExpiryPolicy::SlidingTtl(StreamTtl::from(
                    NonZeroU64::new(60).expect("sixty is nonzero"),
                )),
                lifecycle: StreamLifecycle::Open,
            },
            CreateStream {
                path: axes.clone(),
                content_type: StreamContentType::octet_stream(),
                expiry: ExpiryPolicy::None,
                lifecycle: StreamLifecycle::Closed,
            },
        ] {
            let (reply, mut outcome) = oneshot::channel();
            execute_actions(
                machine.handle(WriterEvent::Command(CommandEnvelope::Create {
                    command,
                    reply,
                })),
            );
            assert_eq!(Err(AdmissionRefusal::PathOccupied), outcome.try_recv()?);
            assert_eq!(durable, machine.admitted);
            assert_eq!(durable, machine.durable);
        }

        let (reply, mut missing_close) = oneshot::channel();
        execute_actions(machine.handle(WriterEvent::Command(CommandEnvelope::Close {
            path: path("events/missing")?,
            reply,
        })));
        assert_eq!(
            Err(AdmissionRefusal::PathNotLive),
            missing_close.try_recv()?
        );

        let (reply, mut closed) = oneshot::channel();
        let batch = establish_wal(machine.handle(WriterEvent::Command(CommandEnvelope::Close {
            path: axes.clone(),
            reply,
        })))?;
        execute_actions(machine.handle(WriterEvent::WalEstablished {
            batch,
            result: Ok(WalEstablishment::Durable),
        }));
        assert_eq!(Ok(CloseStreamOutcome::Closed), closed.try_recv()?);

        let (reply, mut already_closed) = oneshot::channel();
        execute_actions(machine.handle(WriterEvent::Command(CommandEnvelope::Close {
            path: axes.clone(),
            reply,
        })));
        assert_eq!(
            Ok(CloseStreamOutcome::AlreadyClosed),
            already_closed.try_recv()?
        );

        let (reply, mut deleted) = oneshot::channel();
        let batch = establish_wal(
            machine.handle(WriterEvent::Command(CommandEnvelope::Delete {
                path: axes.clone(),
                reply,
            })),
        )?;
        execute_actions(machine.handle(WriterEvent::WalEstablished {
            batch,
            result: Ok(WalEstablishment::Durable),
        }));
        assert_eq!(Ok(()), deleted.try_recv()?);

        let (reply, mut deleted_again) = oneshot::channel();
        execute_actions(
            machine.handle(WriterEvent::Command(CommandEnvelope::Delete {
                path: axes,
                reply,
            })),
        );
        assert_eq!(
            Err(AdmissionRefusal::PathNotLive),
            deleted_again.try_recv()?
        );
        Ok(())
    }

    #[test]
    fn suffix_shedding_changes_only_retry_accounting() -> TestResult {
        let existing = path("events/existing")?;
        let mut forest = Forest::empty();
        assert_eq!(
            Ok(Applied),
            forest.strict_fold(
                BatchId::try_from(1)?,
                &OperationFact::StreamCreated {
                    path: existing.clone(),
                    uid: StreamUid::try_from(1)?,
                    content_type: StreamContentType::octet_stream(),
                    expiry: ExpiryPolicy::None,
                    lifecycle: StreamLifecycle::Open,
                },
            )
        );
        let durable = WAL_SUFFIX_COORDINATES_MAX_V2 - 1;
        let mut machine = recovered_machine_at_with_forest(durable, forest.clone())?;
        let (outputs, exit) = machine.handle(WriterEvent::Started).into_parts();
        assert_eq!(None, exit);
        let (first, _input) = take_preparation(outputs).ok_or("the bounded suffix is due")?;
        assert_empty_step(machine.handle(WriterEvent::CheckpointPrepared {
            ticket: first,
            outcome: PreparationOutcome::Abandoned,
        }));

        let (new_stream, mut refused) = create("events/refused")?;
        let (outputs, exit) = machine
            .handle(WriterEvent::Command(new_stream))
            .into_parts();
        assert_eq!(None, exit);
        let mut effects = execute_outputs(outputs);
        let WriterEffect::PrepareCheckpoint(retry) =
            effects.pop().ok_or("shed rearms checkpoint")?
        else {
            return Err("the retry effect is checkpoint preparation".into());
        };
        assert!(effects.is_empty());
        assert_eq!(Err(AdmissionRefusal::Overloaded), refused.try_recv()?);
        assert_eq!(first.cut(), retry.ticket().cut());
        assert_eq!(forest, machine.admitted);
        assert_eq!(forest, machine.durable);

        let (duplicate, mut duplicate_outcome) = create("events/existing")?;
        execute_actions(machine.handle(WriterEvent::Command(duplicate)));
        assert_eq!(
            Ok(CreateOutcome::AlreadyExists),
            duplicate_outcome.try_recv()?
        );
        assert_eq!(forest, machine.durable);
        Ok(())
    }

    #[test]
    fn quiescent_transitions_reject_divergent_admitted_state() -> TestResult {
        let mut barrier = machine_at(1)?;
        let (first, _reply) = create("events/barrier-divergence")?;
        let batch = establish_wal(barrier.handle(WriterEvent::Command(first)))?;
        let (duplicate, _reply) = create("events/barrier-divergence")?;
        assert_empty_step(barrier.handle(WriterEvent::Command(duplicate)));
        barrier.admitted = Forest::empty();
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                barrier.handle(WriterEvent::WalEstablished {
                    batch,
                    result: Ok(WalEstablishment::Durable),
                });
            }))
            .is_err()
        );

        let mut durability = machine_at(1)?;
        let (command, _reply) = create("events/durability-divergence")?;
        let batch = establish_wal(durability.handle(WriterEvent::Command(command)))?;
        durability.admitted = Forest::empty();
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                durability.handle(WriterEvent::WalEstablished {
                    batch,
                    result: Ok(WalEstablishment::Durable),
                });
            }))
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn checkpoint_install_changes_only_published_and_base_state() -> TestResult {
        let (mut machine, ticket, input) = machine_with_preparation()?;
        let durable = machine.durable.clone();
        let admitted = machine.admitted.clone();
        let pending = machine.pending.len();
        let prepared = prepared(input)?;
        let successor = prepared.successor.clone();
        assert_publication(
            machine.handle(WriterEvent::CheckpointPrepared {
                ticket,
                outcome: PreparationOutcome::Prepared(Box::new(prepared)),
            }),
            ticket,
        );
        let (outputs, exit) = machine
            .handle(WriterEvent::SealPublished {
                ticket,
                result: Ok(SealPublication::Authored),
            })
            .into_parts();
        assert_eq!(None, exit);
        let effects = execute_outputs(outputs);
        assert!(
            matches!(effects.as_slice(), [WriterEffect::Collect(input)] if input.cut == ticket.cut())
        );
        assert_eq!(successor, machine.seal);
        assert_eq!(durable, machine.base);
        assert_eq!(durable, machine.durable);
        assert_eq!(admitted, machine.admitted);
        assert_eq!(pending, machine.pending.len());
        Ok(())
    }

    #[test]
    fn invalid_completions_panic_before_mutating_checkpoint_state() -> TestResult {
        let (mut preparing, ticket, input) = machine_with_preparation()?;
        let seal = preparing.seal.clone();
        let durable = preparing.durable.clone();
        let wrong =
            CheckpointTicket::new(ticket.source().successor()?, ticket.cut(), ticket.attempt());
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                preparing.handle(WriterEvent::CheckpointPrepared {
                    ticket: wrong,
                    outcome: PreparationOutcome::Abandoned,
                });
            }))
            .is_err()
        );
        assert!(matches!(
            preparing.checkpoint,
            Some(CheckpointMarker::Preparing { ticket: active }) if active == ticket
        ));
        assert_eq!(seal, preparing.seal);
        assert_eq!(durable, preparing.durable);

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                preparing.handle(WriterEvent::SealPublished {
                    ticket,
                    result: Ok(SealPublication::Authored),
                });
            }))
            .is_err()
        );
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                preparing.handle(WriterEvent::CheckpointPreparationCancelled { ticket });
            }))
            .is_err()
        );
        assert!(matches!(
            preparing.checkpoint,
            Some(CheckpointMarker::Preparing { ticket: active }) if active == ticket
        ));

        let prepared = prepared(input)?;
        assert_publication(
            preparing.handle(WriterEvent::CheckpointPrepared {
                ticket,
                outcome: PreparationOutcome::Prepared(Box::new(prepared)),
            }),
            ticket,
        );
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                preparing.handle(WriterEvent::SealPublished {
                    ticket: wrong,
                    result: Ok(SealPublication::Authored),
                });
            }))
            .is_err()
        );
        assert!(matches!(
            preparing.checkpoint,
            Some(CheckpointMarker::Publishing { ticket: active, .. }) if active == ticket
        ));
        assert_eq!(seal, preparing.seal);
        assert_eq!(durable, preparing.durable);

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                preparing.handle(WriterEvent::CheckpointPrepared {
                    ticket,
                    outcome: PreparationOutcome::Abandoned,
                });
            }))
            .is_err()
        );

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                preparing.handle(WriterEvent::CheckpointPreparationCancelled { ticket });
            }))
            .is_err()
        );
        assert!(matches!(
            preparing.checkpoint,
            Some(CheckpointMarker::Publishing { ticket: active, .. }) if active == ticket
        ));
        let mut idle = machine_at(1)?;
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                idle.handle(WriterEvent::CheckpointPreparationCancelled { ticket });
            }))
            .is_err()
        );
        assert!(idle.checkpoint.is_none());
        Ok(())
    }

    #[test]
    fn malformed_prepared_checkpoints_fail_before_they_become_events() -> TestResult {
        let (_machine, _ticket, input) = machine_with_preparation()?;
        let (ticket, source, _base, snapshot) = input.into_parts();
        let wrong_successor = Seal::new(
            source.partition(),
            source.generation().successor()?,
            WalReplayPoint::Through {
                batch: ticket.cut().successor()?,
                owner: OwnerToken::from(ticket.attempt().owner_claim()),
            },
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;
        let candidate = EncodedAuthoritySeal::try_from(&wrong_successor)?;
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _prepared =
                    PreparedCheckpoint::new(ticket, source, wrong_successor, snapshot, candidate);
            }))
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn cancellation_discards_every_preparation_outcome() -> TestResult {
        for outcome in [
            PreparationOutcome::Abandoned,
            PreparationOutcome::Contradiction {
                detail: "prepared contradiction lost the shutdown race".into(),
            },
        ] {
            let (mut machine, ticket, _input) = machine_with_preparation()?;
            let (outputs, exit) = machine.handle(WriterEvent::IngressClosed).into_parts();
            assert_eq!(None, exit);
            assert!(matches!(
                outputs.as_slice(),
                [WriterOutput::Action(WriterAction::CancelCheckpointPreparation {
                    ticket: observed
                })] if *observed == ticket
            ));
            let (outputs, exit) = machine
                .handle(WriterEvent::CheckpointPrepared { ticket, outcome })
                .into_parts();
            assert!(outputs.is_empty());
            assert_eq!(Some(WriterExit::Shutdown), exit);
        }
        Ok(())
    }

    #[test]
    fn preparation_contradiction_is_an_immediate_terminal_step() -> TestResult {
        let (mut machine, ticket, _input) = machine_with_preparation()?;
        let (outputs, exit) = machine
            .handle(WriterEvent::CheckpointPrepared {
                ticket,
                outcome: PreparationOutcome::Contradiction {
                    detail: "scripted preparation contradiction".into(),
                },
            })
            .into_parts();
        assert!(outputs.is_empty());
        assert!(matches!(
            exit,
            Some(WriterExit::Contradiction { batch, .. }) if batch == ticket.cut()
        ));
        assert!(machine.checkpoint.is_none());
        Ok(())
    }

    #[test]
    fn fail_stop_wal_outcomes_are_terminal_during_every_checkpoint_stage() -> TestResult {
        for stage in [
            CheckpointStage::Preparation,
            CheckpointStage::Publication,
            CheckpointStage::Cancellation,
        ] {
            for outcome in [
                WalFailure::Occupied,
                WalFailure::Unresolved,
                WalFailure::Contradiction,
            ] {
                let (mut machine, batch) = machine_with_wal_and_checkpoint(stage)?;
                let (outputs, exit) = machine
                    .handle(WriterEvent::WalEstablished {
                        batch,
                        result: outcome.result(),
                    })
                    .into_parts();
                assert!(
                    outputs.is_empty(),
                    "fail-stop emits no completion-producing work"
                );
                match outcome {
                    WalFailure::Occupied => assert_eq!(Some(WriterExit::Fenced { batch }), exit),
                    WalFailure::Unresolved => assert!(matches!(
                        exit,
                        Some(WriterExit::Poisoned { batch: observed, .. }) if observed == batch
                    )),
                    WalFailure::Contradiction => assert!(matches!(
                        exit,
                        Some(WriterExit::Contradiction { batch: observed, .. }) if observed == batch
                    )),
                }
            }
        }
        Ok(())
    }

    #[test]
    fn every_non_authored_seal_publication_is_terminal() -> TestResult {
        for failure in [
            SealFailure::NoAuthority,
            SealFailure::Unresolved,
            SealFailure::Retryable,
            SealFailure::Rejected,
            SealFailure::Contradiction,
        ] {
            let (mut machine, ticket, input) = machine_with_preparation()?;
            assert_publication(
                machine.handle(WriterEvent::CheckpointPrepared {
                    ticket,
                    outcome: PreparationOutcome::Prepared(Box::new(prepared(input)?)),
                }),
                ticket,
            );
            let (outputs, exit) = machine
                .handle(WriterEvent::SealPublished {
                    ticket,
                    result: failure.result(),
                })
                .into_parts();
            assert!(outputs.is_empty());
            match failure {
                SealFailure::NoAuthority => {
                    assert_eq!(
                        Some(WriterExit::Fenced {
                            batch: ticket.cut()
                        }),
                        exit
                    );
                }
                SealFailure::Unresolved | SealFailure::Retryable | SealFailure::Rejected => {
                    assert!(matches!(
                        exit,
                        Some(WriterExit::Poisoned { batch, .. }) if batch == ticket.cut()
                    ));
                }
                SealFailure::Contradiction => assert!(matches!(
                    exit,
                    Some(WriterExit::Contradiction { batch, .. }) if batch == ticket.cut()
                )),
            }
        }
        Ok(())
    }

    #[test]
    fn shutdown_waits_for_wal_barriers_but_not_leak_only_collection() -> TestResult {
        let mut idle = machine_at(1)?;
        let (outputs, exit) = idle.handle(WriterEvent::IngressClosed).into_parts();
        assert!(outputs.is_empty());
        assert_eq!(Some(WriterExit::Shutdown), exit);

        let mut draining = machine_at(1)?;
        let (first, mut first_reply) = create("events/drain-first")?;
        let first_batch = establish_wal(draining.handle(WriterEvent::Command(first)))?;
        let (second, mut second_reply) = create("events/drain-second")?;
        assert_empty_step(draining.handle(WriterEvent::Command(second)));
        assert_empty_step(draining.handle(WriterEvent::IngressClosed));
        let (outputs, exit) = draining
            .handle(WriterEvent::WalEstablished {
                batch: first_batch,
                result: Ok(WalEstablishment::Durable),
            })
            .into_parts();
        assert_eq!(None, exit);
        let mut effects = execute_outputs(outputs);
        let WriterEffect::EstablishWal(second_candidate) = effects.pop().ok_or("second WAL")?
        else {
            return Err("drain promotion emits a WAL".into());
        };
        assert!(effects.is_empty());
        assert_eq!(Ok(CreateOutcome::Created), first_reply.try_recv()?);
        assert!(matches!(
            second_reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        let step = draining.handle(WriterEvent::WalEstablished {
            batch: second_candidate.batch(),
            result: Ok(WalEstablishment::Durable),
        });
        let (outputs, exit) = step.into_parts();
        assert_eq!(Some(WriterExit::Shutdown), exit);
        assert!(execute_outputs(outputs).is_empty());
        assert_eq!(Ok(CreateOutcome::Created), second_reply.try_recv()?);

        let (mut collecting, ticket, input) = machine_with_preparation()?;
        assert_publication(
            collecting.handle(WriterEvent::CheckpointPrepared {
                ticket,
                outcome: PreparationOutcome::Prepared(Box::new(prepared(input)?)),
            }),
            ticket,
        );
        let step = collecting.handle(WriterEvent::SealPublished {
            ticket,
            result: Ok(SealPublication::Authored),
        });
        let cut = collection_cut(step)?;
        assert_eq!(Some(cut), collecting.collector);
        let (outputs, exit) = collecting.handle(WriterEvent::IngressClosed).into_parts();
        assert!(outputs.is_empty());
        assert_eq!(Some(WriterExit::Shutdown), exit);
        Ok(())
    }

    #[test]
    fn suffix_gate_reserves_one_takeover_coordinate() -> TestResult {
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

fn collection_cut(step: WriterStep) -> TestResult<BatchId> {
    let (outputs, exit) = step.into_parts();
    assert_eq!(None, exit);
    outputs
        .into_iter()
        .find_map(|output| match output {
            WriterOutput::Effect(WriterEffect::Collect(input)) => Some(input.into_parts().0),
            WriterOutput::Effect(
                WriterEffect::EstablishWal(_)
                | WriterEffect::PrepareCheckpoint(_)
                | WriterEffect::PublishAuthority { .. },
            )
            | WriterOutput::Action(_) => None,
        })
        .ok_or_else(|| "an authored open-ingress advance starts collection".into())
}

fn machine_with_preparation() -> TestResult<(WriterMachine, CheckpointTicket, CheckpointInput)> {
    machine_with_preparation_at(WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER)
}

fn machine_with_preparation_at(
    durable: u64,
) -> TestResult<(WriterMachine, CheckpointTicket, CheckpointInput)> {
    let mut machine = recovered_machine_at(durable)?;
    let (outputs, exit) = machine.handle(WriterEvent::Started).into_parts();
    assert_eq!(None, exit);
    let (ticket, input) = take_preparation(outputs).ok_or("a due checkpoint is issued")?;
    Ok((machine, ticket, input))
}

mod collector_fixture {
    use super::*;

    pub(super) fn machine_with_collector_and_publication()
    -> TestResult<(WriterMachine, BatchId, CheckpointTicket)> {
        let (mut machine, first_ticket, first_input) = machine_with_preparation()?;
        assert_publication(
            machine.handle(WriterEvent::CheckpointPrepared {
                ticket: first_ticket,
                outcome: PreparationOutcome::Prepared(Box::new(prepared(first_input)?)),
            }),
            first_ticket,
        );
        let first_cut = collection_cut(machine.handle(WriterEvent::SealPublished {
            ticket: first_ticket,
            result: Ok(SealPublication::Authored),
        }))?;

        let mut next_preparation = None;
        for ordinal in 0..WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER {
            let (command, _reply) = create(&format!("events/advance-{ordinal}"))?;
            let batch = establish_wal(machine.handle(WriterEvent::Command(command)))?;
            let (outputs, exit) = machine
                .handle(WriterEvent::WalEstablished {
                    batch,
                    result: Ok(WalEstablishment::Durable),
                })
                .into_parts();
            assert_eq!(None, exit);
            if let Some(preparation) = take_preparation(outputs) {
                next_preparation = Some(preparation);
            }
        }
        let (ticket, input) = next_preparation.ok_or("the next checkpoint becomes due")?;
        assert_publication(
            machine.handle(WriterEvent::CheckpointPrepared {
                ticket,
                outcome: PreparationOutcome::Prepared(Box::new(prepared(input)?)),
            }),
            ticket,
        );
        Ok((machine, first_cut, ticket))
    }
}

use collector_fixture::machine_with_collector_and_publication;

mod wal_stage_fixture {
    use super::*;

    pub(super) fn machine_with_wal_and_checkpoint(
        stage: CheckpointStage,
    ) -> TestResult<(WriterMachine, BatchId)> {
        let (mut machine, ticket, input) = machine_with_preparation()?;
        let (command, _reply) = create("events/fail-stop-stage")?;
        let batch = establish_wal(machine.handle(WriterEvent::Command(command)))?;
        match stage {
            CheckpointStage::Preparation => drop(input),
            CheckpointStage::Publication => assert_publication(
                machine.handle(WriterEvent::CheckpointPrepared {
                    ticket,
                    outcome: PreparationOutcome::Prepared(Box::new(prepared(input)?)),
                }),
                ticket,
            ),
            CheckpointStage::Cancellation => {
                drop(input);
                let (outputs, exit) = machine.handle(WriterEvent::IngressClosed).into_parts();
                assert_eq!(None, exit);
                assert!(matches!(
                    outputs.as_slice(),
                    [WriterOutput::Action(WriterAction::CancelCheckpointPreparation {
                        ticket: observed
                    })] if *observed == ticket
                ));
            }
        }
        Ok((machine, batch))
    }
}

use wal_stage_fixture::machine_with_wal_and_checkpoint;

fn checkpoint_probe() -> TestResult<CommandEnvelope> {
    let (reply, _outcome) = oneshot::channel();
    Ok(CommandEnvelope::Delete {
        path: path("events/missing")?,
        reply,
    })
}

fn create(
    raw: &str,
) -> TestResult<(
    CommandEnvelope,
    oneshot::Receiver<Result<CreateOutcome, AdmissionRefusal>>,
)> {
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

fn path(raw: &str) -> TestResult<DirectoryKey> {
    Ok(DirectoryKey::try_from(Box::<[u8]>::from(raw.as_bytes()))?)
}

fn establish_wal(step: WriterStep) -> TestResult<BatchId> {
    let (outputs, exit) = step.into_parts();
    assert_eq!(None, exit);
    outputs
        .into_iter()
        .find_map(|output| match output {
            WriterOutput::Effect(WriterEffect::EstablishWal(candidate)) => Some(candidate.batch()),
            WriterOutput::Effect(
                WriterEffect::PrepareCheckpoint(_)
                | WriterEffect::PublishAuthority { .. }
                | WriterEffect::Collect(_),
            )
            | WriterOutput::Action(_) => None,
        })
        .ok_or_else(|| "the command issues a WAL effect".into())
}

fn prepared(input: CheckpointInput) -> TestResult<PreparedCheckpoint> {
    let (ticket, source, _base, snapshot) = input.into_parts();
    let successor = Seal::new(
        source.partition(),
        source.generation().successor()?,
        WalReplayPoint::Through {
            batch: ticket.cut(),
            owner: OwnerToken::from(ticket.attempt().owner_claim()),
        },
        TreeVersion::empty(),
        TreeVersion::empty(),
    )?;
    let candidate = EncodedAuthoritySeal::try_from(&successor)?;
    Ok(PreparedCheckpoint::new(
        ticket, source, successor, snapshot, candidate,
    ))
}

fn machine_at(durable: u64) -> TestResult<WriterMachine> {
    let mut machine = recovered_machine_at(durable)?;
    let (outputs, exit) = machine.handle(WriterEvent::Started).into_parts();
    assert!(
        outputs.is_empty(),
        "the fixture starts below the checkpoint threshold"
    );
    assert_eq!(None, exit);
    Ok(machine)
}

fn recovered_machine_at(durable: u64) -> TestResult<WriterMachine> {
    recovered_machine_at_with_forest(durable, Forest::empty())
}

fn recovered_machine_at_with_forest(durable: u64, forest: Forest) -> TestResult<WriterMachine> {
    let partition = "00112233-4455-6677-8899-aabbccddeeff".parse()?;
    let generation = SealGeneration::genesis().successor()?;
    let durable_batch = BatchId::try_from(durable)?;
    let next_batch = durable_batch.successor()?;
    Ok(WriterMachine::from_recovery(
        AuthoredClaim::new(generation),
        Seal::new(
            partition,
            generation,
            WalReplayPoint::Genesis,
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?,
        Forest::empty(),
        forest,
        durable_batch,
        next_batch,
    ))
}

fn take_preparation(outputs: Vec<WriterOutput>) -> Option<(CheckpointTicket, CheckpointInput)> {
    outputs.into_iter().find_map(|output| match output {
        WriterOutput::Effect(effect @ WriterEffect::PrepareCheckpoint(_)) => {
            let EffectKey::CheckpointPreparation { ticket } = effect.key() else {
                panic!("a preparation has a preparation key");
            };
            let WriterEffect::PrepareCheckpoint(input) = effect else {
                panic!("the matched effect remains a preparation");
            };
            Some((ticket, input))
        }
        WriterOutput::Effect(
            WriterEffect::EstablishWal(_)
            | WriterEffect::PublishAuthority { .. }
            | WriterEffect::Collect(_),
        )
        | WriterOutput::Action(_) => None,
    })
}

fn assert_publication(step: WriterStep, ticket: CheckpointTicket) {
    assert!(has_publication(step, ticket));
}

fn has_publication(step: WriterStep, ticket: CheckpointTicket) -> bool {
    let (outputs, exit) = step.into_parts();
    exit.is_none()
        && matches!(
            outputs.as_slice(),
            [WriterOutput::Effect(WriterEffect::PublishAuthority { ticket: observed, .. })]
                if *observed == ticket
        )
}

fn has_publish_then_replies(step: &WriterStep) -> bool {
    step.exit.is_none()
        && matches!(
            step.outputs.as_slice(),
            [
                WriterOutput::Action(WriterAction::PublishView(_)),
                WriterOutput::Action(WriterAction::SendReplies(_))
            ]
        )
}

fn has_wal(step: &WriterStep) -> bool {
    step.outputs
        .iter()
        .any(|output| matches!(output, WriterOutput::Effect(WriterEffect::EstablishWal(_))))
}

fn assert_empty_step(step: WriterStep) {
    let (outputs, exit) = step.into_parts();
    assert!(outputs.is_empty());
    assert_eq!(None, exit);
}

fn execute_actions(step: WriterStep) {
    let (outputs, exit) = step.into_parts();
    assert_eq!(None, exit);
    assert!(
        execute_outputs(outputs).is_empty(),
        "the scripted step contains immediate actions only"
    );
}

fn execute_outputs(outputs: Vec<WriterOutput>) -> Vec<WriterEffect> {
    let mut effects = Vec::new();
    for output in outputs {
        match output {
            WriterOutput::Action(WriterAction::SendReplies(replies)) => {
                for completion in replies {
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
            }
            WriterOutput::Action(
                WriterAction::PublishView(_) | WriterAction::CancelCheckpointPreparation { .. },
            ) => {}
            WriterOutput::Effect(effect) => effects.push(effect),
        }
    }
    effects
}
