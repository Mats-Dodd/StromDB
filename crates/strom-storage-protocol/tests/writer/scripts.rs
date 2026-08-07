use std::num::NonZeroU64;
use std::panic::{AssertUnwindSafe, catch_unwind};

use strom_domain::{
    CloseStreamOutcome, CreateOutcome, ExpiryPolicy, StreamContentType, StreamLifecycle, StreamTtl,
};
use strom_storage_domain::{
    AttemptId, BatchId, OperationFact, Seal, SealGeneration, StreamUid, TreeVersion,
    WAL_RUN_FACTS_MAX, WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER, WAL_SUFFIX_COORDINATES_MAX_V2,
    WalReplayPoint,
};
use strom_storage_protocol::{
    AdmissionRefusal, Applied, CheckpointTicket, CommandEnvelope, CreateStream, Forest,
    PreparationOutcome, PreparedCheckpoint, SealPublication, TypedStoreError, WalEstablishment,
    WriterAction, WriterEffect, WriterEvent, WriterExit, WriterMachine, WriterOutput,
};
use tokio::sync::oneshot;

use super::fixtures::*;

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

#[test]
fn wal_durability_orders_publication_before_reply_release() -> TestResult {
    let mut machine = machine_at(1)?;
    let (command, mut reply) = create("events/ordered")?;
    let batch = establish_wal(machine.handle(WriterEvent::Command(command)))?;
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
        [WriterOutput::Action(
            WriterAction::CancelCheckpointPreparation { ticket: observed }
        )] if *observed == ticket
    ));
    let (outputs, exit) = completed
        .handle(WriterEvent::CheckpointPrepared {
            ticket,
            outcome: PreparationOutcome::Prepared(Box::new(prepared(input)?)),
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
fn closure_after_preparation_waits_for_publication_then_skips_collection() -> TestResult {
    let (mut machine, ticket, input) = machine_with_preparation()?;
    assert_publication(
        machine.handle(WriterEvent::CheckpointPrepared {
            ticket,
            outcome: PreparationOutcome::Prepared(Box::new(prepared(input)?)),
        }),
        ticket,
    );
    assert_empty_step(machine.handle(WriterEvent::IngressClosed));

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
            assert_publication(
                machine.handle(WriterEvent::CheckpointPrepared {
                    ticket,
                    outcome: PreparationOutcome::Prepared(Box::new(prepared(input)?)),
                }),
                ticket,
            );
            assert!(has_publish_then_replies(machine.handle(
                WriterEvent::WalEstablished {
                    batch,
                    result: Ok(WalEstablishment::Durable),
                }
            )));
        } else {
            assert!(has_publish_then_replies(machine.handle(
                WriterEvent::WalEstablished {
                    batch,
                    result: Ok(WalEstablishment::Durable),
                }
            )));
            assert_publication(
                machine.handle(WriterEvent::CheckpointPrepared {
                    ticket,
                    outcome: PreparationOutcome::Prepared(Box::new(prepared(input)?)),
                }),
                ticket,
            );
        }
    }
    Ok(())
}

#[test]
fn collector_budget_skips_an_occupied_advance_and_releases_on_completion() -> TestResult {
    let (mut occupied, first_cut, ticket) = machine_with_collector_and_publication()?;
    let (outputs, exit) = occupied
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
    assert_empty_step(occupied.handle(WriterEvent::CollectFinished { cut: first_cut }));

    let (mut released, first_cut, ticket) = machine_with_collector_and_publication()?;
    assert_empty_step(released.handle(WriterEvent::CollectFinished { cut: first_cut }));
    let cut = collection_cut(released.handle(WriterEvent::SealPublished {
        ticket,
        result: Ok(SealPublication::Authored),
    }))?;
    assert_eq!(ticket.cut(), cut);
    Ok(())
}

#[test]
fn stale_cancellation_identity_is_a_protocol_violation() -> TestResult {
    let (mut machine, ticket, _input) = machine_with_preparation()?;
    let (outputs, exit) = machine.handle(WriterEvent::IngressClosed).into_parts();
    assert_eq!(None, exit);
    assert!(matches!(
        outputs.as_slice(),
        [WriterOutput::Action(
            WriterAction::CancelCheckpointPreparation { ticket: observed }
        )] if *observed == ticket
    ));
    let wrong = CheckpointTicket::new(
        ticket.source(),
        ticket.cut(),
        AttemptId::new(ticket.attempt().owner_claim(), STALE_ATTEMPT_COUNTER),
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            machine.handle(WriterEvent::CheckpointPreparationCancelled { ticket: wrong });
        }))
        .is_err()
    );
    let (_, exit) = machine
        .handle(WriterEvent::CheckpointPreparationCancelled { ticket })
        .into_parts();
    assert_eq!(Some(WriterExit::Shutdown), exit);
    Ok(())
}

#[test]
fn checkpoint_attempts_wait_until_suffix_shedding_rearms_the_cut() -> TestResult {
    let (mut suppressed, first, _input) = machine_with_preparation()?;
    assert_empty_step(suppressed.handle(WriterEvent::CheckpointPrepared {
        ticket: first,
        outcome: PreparationOutcome::Abandoned,
    }));
    let (reply, _outcome) = oneshot::channel();
    let (outputs, exit) = suppressed
        .handle(WriterEvent::Command(CommandEnvelope::Delete {
            path: path("events/missing")?,
            reply,
        }))
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
    assert_empty_step(machine.handle(WriterEvent::Command(duplicate)));

    let (outputs, exit) = machine
        .handle(WriterEvent::WalEstablished {
            batch: first_batch,
            result: Ok(WalEstablishment::Durable),
        })
        .into_parts();
    assert_eq!(None, exit);
    assert!(
        execute_outputs(outputs).is_empty(),
        "the duplicate consumes no second WAL coordinate"
    );
    assert_eq!(Ok(CreateOutcome::Created), created.try_recv()?);
    assert_eq!(
        Ok(CreateOutcome::AlreadyExists),
        duplicate_outcome.try_recv()?
    );

    let (duplicate, mut immediate) = create("events/duplicate")?;
    execute_actions(machine.handle(WriterEvent::Command(duplicate)));
    assert_eq!(Ok(CreateOutcome::AlreadyExists), immediate.try_recv()?);
    Ok(())
}

#[test]
fn full_pending_barrier_refuses_before_admission() -> TestResult {
    let mut machine = machine_at(1)?;
    let (first, _created) = create("events/full")?;
    let _batch = establish_wal(machine.handle(WriterEvent::Command(first)))?;
    for _ordinal in 0..WAL_RUN_FACTS_MAX {
        let (duplicate, _outcome) = create("events/full")?;
        assert_empty_step(machine.handle(WriterEvent::Command(duplicate)));
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
    assert!(has_publish_then_replies(machine.handle(
        WriterEvent::WalEstablished {
            batch,
            result: Ok(WalEstablishment::Durable),
        }
    )));
    Ok(())
}

#[test]
fn startup_schedules_recovered_work_once_before_ingress() -> TestResult {
    let mut below = recovered_machine_at(WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER - 1)?;
    assert_empty_step(below.handle(WriterEvent::Started));

    let mut due = recovered_machine_at(WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER)?;
    let (outputs, exit) = due.handle(WriterEvent::Started).into_parts();
    assert_eq!(None, exit);
    let (ticket, _input) = take_preparation(outputs).ok_or("startup issues the due checkpoint")?;
    assert!(
        catch_unwind(AssertUnwindSafe(|| due.handle(WriterEvent::Started))).is_err(),
        "startup is a one-shot machine observation"
    );
    assert_empty_step(due.handle(WriterEvent::CheckpointPrepared {
        ticket,
        outcome: PreparationOutcome::Abandoned,
    }));
    Ok(())
}

#[test]
fn consecutive_wal_runs_publish_and_release_only_their_own_barriers() -> TestResult {
    let mut machine = machine_at(1)?;
    let (first, mut first_outcome) = create("events/first")?;
    let first_batch = establish_wal(machine.handle(WriterEvent::Command(first)))?;
    let (second, mut second_outcome) = create("events/second")?;
    assert_empty_step(machine.handle(WriterEvent::Command(second)));

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
    let first_view = published_view(&outputs).ok_or("the durable WAL publishes its view")?;
    assert!(first_view.resolve(&path("events/first")?).is_some());
    assert!(first_view.resolve(&path("events/second")?).is_none());
    let mut effects = execute_outputs(outputs);
    let WriterEffect::EstablishWal(second_candidate) =
        effects.pop().ok_or("the pending fact is promoted")?
    else {
        return Err("the promoted effect is a WAL".into());
    };
    assert!(effects.is_empty());
    assert_eq!(Ok(CreateOutcome::Created), first_outcome.try_recv()?);
    assert!(matches!(
        second_outcome.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let (outputs, exit) = machine
        .handle(WriterEvent::WalEstablished {
            batch: second_candidate.batch(),
            result: Ok(WalEstablishment::Durable),
        })
        .into_parts();
    assert_eq!(None, exit);
    let second_view =
        published_view(&outputs).ok_or("the second durable WAL publishes its view")?;
    assert!(second_view.resolve(&path("events/second")?).is_some());
    assert!(execute_outputs(outputs).is_empty());
    assert_eq!(Ok(CreateOutcome::Created), second_outcome.try_recv()?);
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
        execute_replies(
            machine.handle(WriterEvent::Command(CommandEnvelope::Create {
                command,
                reply,
            })),
        );
        assert_eq!(Err(AdmissionRefusal::PathOccupied), outcome.try_recv()?);
    }

    let (reply, mut missing_close) = oneshot::channel();
    execute_replies(machine.handle(WriterEvent::Command(CommandEnvelope::Close {
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
    execute_replies(machine.handle(WriterEvent::Command(CommandEnvelope::Close {
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
    execute_replies(
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
fn suffix_shedding_refuses_new_facts_without_changing_durable_state() -> TestResult {
    let existing = path("events/existing")?;
    let mut forest = Forest::empty();
    assert_eq!(
        Ok(Applied),
        forest.strict_fold(
            BatchId::try_from(1)?,
            &OperationFact::StreamCreated {
                path: existing,
                uid: StreamUid::try_from(1)?,
                content_type: StreamContentType::octet_stream(),
                expiry: ExpiryPolicy::None,
                lifecycle: StreamLifecycle::Open,
            },
        )
    );
    let mut machine = recovered_machine_at_with_forest(WAL_SUFFIX_COORDINATES_MAX_V2 - 1, &forest)?;
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
    assert!(
        published_view(&outputs).is_none(),
        "suffix shedding does not publish a new durable view"
    );
    let mut effects = execute_outputs(outputs);
    let WriterEffect::PrepareCheckpoint(retry_input) =
        effects.pop().ok_or("shed rearms checkpoint")?
    else {
        return Err("suffix shedding issues checkpoint preparation".into());
    };
    assert!(effects.is_empty());
    let retry = retry_input.ticket();
    assert_eq!(Err(AdmissionRefusal::Overloaded), refused.try_recv()?);
    assert_eq!(first.cut(), retry.cut());

    let (duplicate, mut duplicate_outcome) = create("events/existing")?;
    execute_replies(machine.handle(WriterEvent::Command(duplicate)));
    assert_eq!(
        Ok(CreateOutcome::AlreadyExists),
        duplicate_outcome.try_recv()?
    );
    Ok(())
}

#[test]
fn authored_checkpoint_publication_preserves_the_durable_view_and_starts_collection() -> TestResult
{
    let (mut machine, ticket, input) = machine_with_preparation()?;
    let durable = Forest::empty();
    let prepared = prepared(input)?;
    let successor = prepared.successor().clone();
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
    assert!(matches!(
        outputs.first(),
        Some(WriterOutput::Action(WriterAction::PublishView(view))) if view == &durable
    ));
    let mut effects = execute_outputs(outputs);
    let WriterEffect::Collect(input) = effects.pop().ok_or("collection starts")? else {
        return Err("the checkpoint starts collection".into());
    };
    assert!(effects.is_empty());
    assert_eq!(ticket.cut(), input.cut());
    assert_eq!(&successor, input.successor());
    Ok(())
}

#[test]
fn invalid_completions_panic_before_consuming_the_issued_effect() -> TestResult {
    let (mut preparing, ticket, input) = machine_with_preparation()?;
    let wrong = CheckpointTicket::new(ticket.source().successor()?, ticket.cut(), ticket.attempt());
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            preparing.handle(WriterEvent::CheckpointPrepared {
                ticket: wrong,
                outcome: PreparationOutcome::Abandoned,
            });
        }))
        .is_err()
    );
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
    assert_publication(
        preparing.handle(WriterEvent::CheckpointPrepared {
            ticket,
            outcome: PreparationOutcome::Prepared(Box::new(prepared(input)?)),
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
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            preparing.handle(WriterEvent::CheckpointPrepared {
                ticket,
                outcome: PreparationOutcome::Abandoned,
            });
        }))
        .is_err()
    );
    let cut = collection_cut(preparing.handle(WriterEvent::SealPublished {
        ticket,
        result: Ok(SealPublication::Authored),
    }))?;
    assert_eq!(ticket.cut(), cut);
    Ok(())
}

#[test]
fn malformed_prepared_checkpoints_fail_before_they_become_events() -> TestResult {
    let (_machine, ticket, input) = machine_with_preparation()?;
    let (_ticket, source, _base, snapshot) = input.into_parts();
    let valid_successor = Seal::new(
        source.partition(),
        source.generation().successor()?,
        WalReplayPoint::Through {
            batch: ticket.cut(),
            owner: strom_storage_domain::OwnerToken::from(ticket.attempt().owner_claim()),
        },
        TreeVersion::empty(),
        TreeVersion::empty(),
    )?;
    let foreign_partition = "11112222-3333-4444-8888-9999aaaabbbb".parse()?;
    let cross_partition = Seal::new(
        foreign_partition,
        valid_successor.generation(),
        valid_successor.replay(),
        TreeVersion::empty(),
        TreeVersion::empty(),
    )?;
    let skipped_generation = Seal::new(
        source.partition(),
        valid_successor.generation().successor()?,
        valid_successor.replay(),
        TreeVersion::empty(),
        TreeVersion::empty(),
    )?;
    let wrong_cut = Seal::new(
        source.partition(),
        valid_successor.generation(),
        WalReplayPoint::Through {
            batch: ticket.cut().successor()?,
            owner: strom_storage_domain::OwnerToken::from(ticket.attempt().owner_claim()),
        },
        TreeVersion::empty(),
        TreeVersion::empty(),
    )?;
    let genesis_successor = Seal::new(
        source.partition(),
        valid_successor.generation(),
        WalReplayPoint::Genesis,
        TreeVersion::empty(),
        TreeVersion::empty(),
    )?;
    let nonadvancing_source = Seal::new(
        source.partition(),
        source.generation(),
        WalReplayPoint::Through {
            batch: ticket.cut(),
            owner: strom_storage_domain::OwnerToken::from(SealGeneration::genesis()),
        },
        source.directory().clone(),
        source.ledger().clone(),
    )?;
    for (source, successor) in [
        (source.clone(), cross_partition),
        (source.clone(), skipped_generation),
        (source.clone(), wrong_cut),
        (source.clone(), genesis_successor),
        (nonadvancing_source, valid_successor),
    ] {
        let candidate = strom_storage_domain::EncodedAuthoritySeal::try_from(&successor)?;
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _prepared =
                    PreparedCheckpoint::new(ticket, source, successor, snapshot.clone(), candidate);
            }))
            .is_err()
        );
    }
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
        assert_eq!(None, exit, "WAL durability keeps the writer live");
        assert!(matches!(
            outputs.as_slice(),
            [WriterOutput::Action(
                WriterAction::CancelCheckpointPreparation { ticket: observed }
            )] if *observed == ticket
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
            assert!(outputs.is_empty(), "fail-stop emits no new effect");
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
            SealFailure::NoAuthority => assert_eq!(
                Some(WriterExit::Fenced {
                    batch: ticket.cut()
                }),
                exit
            ),
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
    let WriterEffect::EstablishWal(second_candidate) = effects.pop().ok_or("second WAL")? else {
        return Err("drain promotion emits a WAL".into());
    };
    assert!(effects.is_empty());
    assert_eq!(Ok(CreateOutcome::Created), first_reply.try_recv()?);
    assert!(matches!(
        second_reply.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let (outputs, exit) = draining
        .handle(WriterEvent::WalEstablished {
            batch: second_candidate.batch(),
            result: Ok(WalEstablishment::Durable),
        })
        .into_parts();
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
    let _cut = collection_cut(collecting.handle(WriterEvent::SealPublished {
        ticket,
        result: Ok(SealPublication::Authored),
    }))?;
    let (outputs, exit) = collecting.handle(WriterEvent::IngressClosed).into_parts();
    assert!(outputs.is_empty());
    assert_eq!(Some(WriterExit::Shutdown), exit);
    Ok(())
}

fn machine_with_collector_and_publication() -> TestResult<(WriterMachine, BatchId, CheckpointTicket)>
{
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
        assert_eq!(
            None, exit,
            "WAL durability keeps the collector fixture live"
        );
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

fn machine_with_wal_and_checkpoint(stage: CheckpointStage) -> TestResult<(WriterMachine, BatchId)> {
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
            assert_eq!(None, exit, "checkpoint cancellation waits for completion");
            assert!(
                matches!(
                    outputs.as_slice(),
                    [WriterOutput::Action(
                        WriterAction::CancelCheckpointPreparation { ticket: observed }
                    )] if *observed == ticket
                ),
                "ingress closure cancels the exact preparation"
            );
        }
    }
    Ok((machine, batch))
}
