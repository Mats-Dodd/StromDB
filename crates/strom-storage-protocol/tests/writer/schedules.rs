use std::collections::BTreeSet;

use proptest::prelude::*;
use strom_storage_domain::WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER;
use strom_storage_protocol::{
    CheckpointInput, EffectKey, PreparationOutcome, SealPublication, TypedStoreError,
    WRITER_OUTPUTS_PER_STEP_MAX, WalEstablishment, WriterAction, WriterEffect, WriterEvent,
    WriterExit, WriterMachine, WriterOutput, WriterStep,
};

use super::fixtures::{
    CreateReply, TestResult, complete_success, create, prepared, recovered_machine_at,
    settle_replies,
};

const DRAIN_STEPS_MAX: usize = 64;

enum Outstanding {
    Effect(WriterEffect),
    Cancelling(CheckpointInput),
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ComparableKey {
    Wal(u64),
    Preparation([u64; 4]),
    Publication([u64; 4]),
    Collection(u64),
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn generated_legal_schedules_preserve_public_machine_bounds(
        choices in prop::collection::vec(any::<[u8; 2]>(), 0..=64),
    ) {
        run_schedule(1, "low", &choices)
            .expect("the generated low-head schedule is legal");
        run_schedule(WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER, "checkpoint", &choices)
            .expect("the generated checkpoint schedule is legal");
    }
}

fn run_schedule(durable: u64, label: &str, choices: &[[u8; 2]]) -> TestResult {
    let mut machine = recovered_machine_at(durable)?;
    let mut outstanding = Vec::new();
    let mut replies = Vec::new();
    let mut ingress_open = true;
    let mut ordinal = 0;

    let started = start_machine(&mut machine, &mut outstanding)?;
    assert_eq!(None, started, "writer startup remains live");
    issue_command(&mut machine, &mut outstanding, &mut replies, label, ordinal)?;
    ordinal = ordinal
        .checked_add(1)
        .ok_or("generated command ordinal is not exhausted")?;

    for choice in choices {
        let exit = match choice[0].rem_euclid(4) {
            0 if ingress_open => {
                let selected = if choice[1].is_multiple_of(3) && ordinal > 0 {
                    ordinal
                        .checked_sub(1)
                        .ok_or("a positive command ordinal has a predecessor")?
                } else {
                    let selected = ordinal;
                    ordinal = ordinal
                        .checked_add(1)
                        .ok_or("generated command ordinal is not exhausted")?;
                    selected
                };
                issue_command(
                    &mut machine,
                    &mut outstanding,
                    &mut replies,
                    label,
                    selected,
                )?
            }
            1 | 3 if !outstanding.is_empty() => {
                complete_generated(&mut machine, &mut outstanding, *choice)?
            }
            2 if ingress_open => {
                ingress_open = false;
                close_ingress(&mut machine, &mut outstanding)?
            }
            _ => None,
        };
        if let Some(exit) = exit {
            if exit == WriterExit::Shutdown {
                assert_all_replies_settled(replies);
            }
            return Ok(());
        }
    }

    if ingress_open {
        let exit = close_ingress(&mut machine, &mut outstanding)?;
        if let Some(exit) = exit {
            assert_eq!(
                WriterExit::Shutdown,
                exit,
                "a terminal graceful close is Shutdown"
            );
            assert_all_replies_settled(replies);
            return Ok(());
        }
    }

    for _step in 0..DRAIN_STEPS_MAX {
        let exit = complete_for_shutdown(&mut machine, &mut outstanding)?;
        if let Some(exit) = exit {
            assert_eq!(
                WriterExit::Shutdown,
                exit,
                "successful graceful drain ends in Shutdown"
            );
            assert_all_replies_settled(replies);
            return Ok(());
        }
    }
    Err("graceful drain exceeds DRAIN_STEPS_MAX".into())
}

fn start_machine(
    machine: &mut WriterMachine,
    outstanding: &mut Vec<Outstanding>,
) -> TestResult<Option<WriterExit>> {
    observe_step(machine.handle(WriterEvent::Started), outstanding)
}

fn issue_command(
    machine: &mut WriterMachine,
    outstanding: &mut Vec<Outstanding>,
    replies: &mut Vec<CreateReply>,
    label: &str,
    ordinal: usize,
) -> TestResult<Option<WriterExit>> {
    let (command, reply) = create(&format!("generated/{label}/{ordinal}"))?;
    replies.push(reply);
    observe_step(machine.handle(WriterEvent::Command(command)), outstanding)
}

fn complete_generated(
    machine: &mut WriterMachine,
    outstanding: &mut Vec<Outstanding>,
    choice: [u8; 2],
) -> TestResult<Option<WriterExit>> {
    let index = usize::from(choice[0])
        .checked_rem(outstanding.len())
        .ok_or("generated completion requires outstanding work")?;
    let work = outstanding.remove(index);
    let event = match work {
        Outstanding::Effect(WriterEffect::EstablishWal(candidate)) => WriterEvent::WalEstablished {
            batch: candidate.batch(),
            result: wal_result(choice[1]),
        },
        Outstanding::Effect(WriterEffect::PrepareCheckpoint(input)) => {
            preparation_event(input, choice[1])?
        }
        Outstanding::Effect(WriterEffect::PublishAuthority {
            ticket,
            candidate: _,
        }) => WriterEvent::SealPublished {
            ticket,
            result: seal_result(choice[1]),
        },
        Outstanding::Effect(WriterEffect::Collect(input)) => WriterEvent::CollectFinished {
            cut: input.into_parts().0,
        },
        Outstanding::Cancelling(input) => cancellation_event(input, choice[1])?,
    };
    observe_step(machine.handle(event), outstanding)
}

fn close_ingress(
    machine: &mut WriterMachine,
    outstanding: &mut Vec<Outstanding>,
) -> TestResult<Option<WriterExit>> {
    observe_step(machine.handle(WriterEvent::IngressClosed), outstanding)
}

fn assert_all_replies_settled(replies: Vec<CreateReply>) {
    for mut reply in replies {
        assert!(
            reply.try_recv().is_ok(),
            "Shutdown settles every accepted command reply"
        );
    }
}

fn complete_for_shutdown(
    machine: &mut WriterMachine,
    outstanding: &mut Vec<Outstanding>,
) -> TestResult<Option<WriterExit>> {
    let index = outstanding
        .iter()
        .position(|work| !matches!(work, Outstanding::Effect(WriterEffect::Collect(_))))
        .expect("leak-only collection never blocks graceful shutdown");
    let work = outstanding.remove(index);
    let event = match work {
        Outstanding::Effect(effect) => complete_success(effect)?,
        Outstanding::Cancelling(input) => WriterEvent::CheckpointPreparationCancelled {
            ticket: input.ticket(),
        },
    };
    observe_step(machine.handle(event), outstanding)
}

fn wal_result(choice: u8) -> Result<WalEstablishment, TypedStoreError> {
    match choice.rem_euclid(6) {
        0 => Ok(WalEstablishment::Durable),
        1 => Ok(WalEstablishment::Occupied),
        2 => Ok(WalEstablishment::UnresolvedAbsent),
        3 => Err(TypedStoreError::Retryable {
            detail: "generated retryable WAL failure".into(),
        }),
        4 => Err(TypedStoreError::Rejected {
            detail: "generated rejected WAL failure".into(),
        }),
        _ => Err(TypedStoreError::Contradiction {
            detail: "generated WAL contradiction".into(),
        }),
    }
}

fn preparation_event(input: CheckpointInput, choice: u8) -> TestResult<WriterEvent> {
    let ticket = input.ticket();
    let outcome = preparation_outcome(input, choice)?;
    Ok(WriterEvent::CheckpointPrepared { ticket, outcome })
}

fn seal_result(choice: u8) -> Result<SealPublication, TypedStoreError> {
    match choice.rem_euclid(6) {
        0 => Ok(SealPublication::Authored),
        1 => Ok(SealPublication::NoAuthority),
        2 => Ok(SealPublication::Unresolved),
        3 => Err(TypedStoreError::Retryable {
            detail: "generated retryable Seal failure".into(),
        }),
        4 => Err(TypedStoreError::Rejected {
            detail: "generated rejected Seal failure".into(),
        }),
        _ => Err(TypedStoreError::Contradiction {
            detail: "generated Seal contradiction".into(),
        }),
    }
}

fn cancellation_event(input: CheckpointInput, choice: u8) -> TestResult<WriterEvent> {
    let ticket = input.ticket();
    if choice.is_multiple_of(4) {
        Ok(WriterEvent::CheckpointPreparationCancelled { ticket })
    } else {
        let outcome = preparation_outcome(
            input,
            choice
                .checked_sub(1)
                .ok_or("non-cancellation choice has a predecessor")?,
        )?;
        Ok(WriterEvent::CheckpointPrepared { ticket, outcome })
    }
}

fn preparation_outcome(input: CheckpointInput, choice: u8) -> TestResult<PreparationOutcome> {
    Ok(match choice.rem_euclid(3) {
        0 => PreparationOutcome::Prepared(Box::new(prepared(input)?)),
        1 => PreparationOutcome::Abandoned,
        2.. => PreparationOutcome::Contradiction {
            detail: "generated preparation contradiction".into(),
        },
    })
}

fn observe_step(
    step: WriterStep,
    outstanding: &mut Vec<Outstanding>,
) -> TestResult<Option<WriterExit>> {
    let (outputs, exit) = step.into_parts();
    assert!(
        outputs.len() <= WRITER_OUTPUTS_PER_STEP_MAX,
        "one event stays inside the public output bound"
    );
    if exit.is_some() {
        assert!(
            outputs
                .iter()
                .all(|output| matches!(output, WriterOutput::Action(_))),
            "terminal steps contain actions only"
        );
    }
    let publication = outputs
        .iter()
        .position(|output| matches!(output, WriterOutput::Action(WriterAction::PublishView(_))));
    let replies = outputs
        .iter()
        .position(|output| matches!(output, WriterOutput::Action(WriterAction::SendReplies(_))));
    if let (Some(publication), Some(replies)) = (publication, replies) {
        assert!(
            publication < replies,
            "durable view publication precedes reply release"
        );
    }

    for output in outputs {
        match output {
            WriterOutput::Effect(effect) => outstanding.push(Outstanding::Effect(effect)),
            WriterOutput::Action(WriterAction::SendReplies(replies)) => settle_replies(replies),
            WriterOutput::Action(WriterAction::PublishView(_)) => {}
            WriterOutput::Action(WriterAction::CancelCheckpointPreparation { ticket }) => {
                let key = EffectKey::CheckpointPreparation { ticket };
                let index = outstanding
                    .iter()
                    .position(
                        |work| matches!(work, Outstanding::Effect(effect) if effect.key() == key),
                    )
                    .ok_or("cancellation names one outstanding preparation")?;
                let removed = outstanding.remove(index);
                let Outstanding::Effect(WriterEffect::PrepareCheckpoint(input)) = removed else {
                    return Err("the cancelled effect remains a preparation".into());
                };
                outstanding.push(Outstanding::Cancelling(input));
            }
        }
    }
    assert_outstanding_budgets(outstanding);
    Ok(exit)
}

fn assert_outstanding_budgets(outstanding: &[Outstanding]) {
    let mut keys = BTreeSet::new();
    let mut wal_occupied = false;
    let mut preparation_occupied = false;
    let mut publication_occupied = false;
    let mut collection_occupied = false;
    for work in outstanding {
        let key = match work {
            Outstanding::Effect(effect) => effect.key(),
            Outstanding::Cancelling(input) => EffectKey::CheckpointPreparation {
                ticket: input.ticket(),
            },
        };
        assert!(
            keys.insert(comparable_key(key)),
            "outstanding effect keys are unique"
        );
        let occupied = match key {
            EffectKey::Wal { .. } => &mut wal_occupied,
            EffectKey::CheckpointPreparation { .. } => &mut preparation_occupied,
            EffectKey::SealPublication { .. } => &mut publication_occupied,
            EffectKey::Collection { .. } => &mut collection_occupied,
        };
        assert!(!*occupied, "one effect per kind can be outstanding");
        *occupied = true;
    }
    assert!(
        !(preparation_occupied && publication_occupied),
        "preparation and publication share one checkpoint budget"
    );
}

const fn comparable_key(key: EffectKey) -> ComparableKey {
    match key {
        EffectKey::Wal { batch } => ComparableKey::Wal(batch.get()),
        EffectKey::CheckpointPreparation { ticket } => {
            ComparableKey::Preparation(ticket_parts(ticket))
        }
        EffectKey::SealPublication { ticket } => ComparableKey::Publication(ticket_parts(ticket)),
        EffectKey::Collection { cut } => ComparableKey::Collection(cut.get()),
    }
}

const fn ticket_parts(ticket: strom_storage_protocol::CheckpointTicket) -> [u64; 4] {
    [
        ticket.source().get(),
        ticket.cut().get(),
        ticket.attempt().owner_claim().get(),
        ticket.attempt().local_counter(),
    ]
}
