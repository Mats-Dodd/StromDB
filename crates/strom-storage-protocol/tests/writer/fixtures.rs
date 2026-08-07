use std::num::NonZeroU64;

use strom_domain::{CreateOutcome, ExpiryPolicy, StreamContentType, StreamLifecycle};
use strom_storage_domain::{
    AttemptId, BatchId, DecodedTable, DirectoryKey, EncodedAuthoritySeal, FreshIdentity,
    OwnerToken, Seal, SealGeneration, SortedRun, StoreKind, TableObjectId, TableRef, TreeVersion,
    WalBody, WalObject, WalReplayPoint,
};
use strom_storage_protocol::{
    AdmissionRefusal, BootstrapEffect, BootstrapEvent, BootstrapMachine, BootstrapStep,
    CheckpointInput, CheckpointTicket, CommandEnvelope, Completion, CreateStream, Forest,
    PreparationOutcome, PreparedCheckpoint, SealPublication, WalEstablishment, WriterAction,
    WriterEffect, WriterEvent, WriterMachine, WriterOutput, WriterStep,
};
use tokio::sync::oneshot;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
pub(super) type CreateReply = oneshot::Receiver<Result<CreateOutcome, AdmissionRefusal>>;

pub(super) fn create(raw: &str) -> TestResult<(CommandEnvelope, CreateReply)> {
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

pub(super) fn path(raw: &str) -> TestResult<DirectoryKey> {
    Ok(DirectoryKey::try_from(Box::<[u8]>::from(raw.as_bytes()))?)
}

pub(super) fn machine_at(durable: u64) -> TestResult<WriterMachine> {
    let mut machine = recovered_machine_at(durable)?;
    assert_empty_step(machine.handle(WriterEvent::Started));
    Ok(machine)
}

pub(super) fn machine_with_preparation()
-> TestResult<(WriterMachine, CheckpointTicket, CheckpointInput)> {
    machine_with_preparation_at(strom_storage_domain::WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER)
}

pub(super) fn machine_with_preparation_at(
    durable: u64,
) -> TestResult<(WriterMachine, CheckpointTicket, CheckpointInput)> {
    let mut machine = recovered_machine_at(durable)?;
    let (outputs, exit) = machine.handle(WriterEvent::Started).into_parts();
    assert_eq!(None, exit, "checkpoint fixture startup remains live");
    let (ticket, input) = take_preparation(outputs).ok_or("a due checkpoint is issued")?;
    Ok((machine, ticket, input))
}

pub(super) fn recovered_machine_at(durable: u64) -> TestResult<WriterMachine> {
    recovered_machine_at_with_forest(durable, &Forest::empty())
}

pub(super) fn recovered_machine_at_with_forest(
    durable: u64,
    forest: &Forest,
) -> TestResult<WriterMachine> {
    let partition = "00112233-4455-6677-8899-aabbccddeeff".parse()?;
    let generation = SealGeneration::try_from(durable)?;
    let claim_generation = generation.successor()?;
    let durable_batch = BatchId::try_from(durable)?;
    let cells = forest.checkpoint_cells();
    let directory = recovery_tree(
        generation,
        StoreKind::Directory,
        0,
        !cells.directory.is_empty(),
    )?;
    let ledger = recovery_tree(generation, StoreKind::Ledger, 1, !cells.ledger.is_empty())?;
    let seal = Seal::new(
        partition,
        generation,
        WalReplayPoint::Genesis,
        directory,
        ledger,
    )?;

    let mut machine = BootstrapMachine::new();
    drop(machine.handle(BootstrapEvent::Started {
        genesis_partition: partition,
    }));
    drop(machine.handle(BootstrapEvent::HeadObserved(Some(generation))));
    drop(machine.handle(BootstrapEvent::SealRead(Some(seal))));
    let mut step = machine.handle(BootstrapEvent::ClaimPublished(SealPublication::Authored));
    loop {
        step = match step {
            BootstrapStep::Effect(BootstrapEffect::ReadTable { table, .. }) => {
                let decoded = match table.object().store() {
                    StoreKind::Directory => DecodedTable::Directory(cells.directory.clone()),
                    StoreKind::Ledger => DecodedTable::Ledger(cells.ledger.clone()),
                    StoreKind::Tally | StoreKind::Annals => {
                        return Err("recovery fixture selected a nonresident table".into());
                    }
                };
                machine.handle(BootstrapEvent::TableRead { table, decoded })
            }
            BootstrapStep::Effect(BootstrapEffect::ObserveWalTail) => break,
            other @ (BootstrapStep::Effect(_)
            | BootstrapStep::Complete(_)
            | BootstrapStep::Exit(_)) => {
                return Err(
                    format!("expected a base read or WAL observation, got {other:?}").into(),
                );
            }
        };
    }
    let listed_tail = durable
        .checked_sub(1)
        .filter(|tail| *tail > 0)
        .map(BatchId::try_from)
        .transpose()?;
    let mut step = machine.handle(BootstrapEvent::WalTailObserved(listed_tail));
    if let Some(tail) = listed_tail {
        assert!(
            matches!(
                step,
                BootstrapStep::Effect(BootstrapEffect::ReadWal { batch, .. }) if batch == tail
            ),
            "the listed tail is read before FENCE placement"
        );
        step = machine.handle(BootstrapEvent::WalRead(Some(WalObject::new(
            partition,
            tail,
            OwnerToken::from(SealGeneration::try_from(tail.get())?),
            WalBody::Fence,
        ))));
    }
    assert!(
        matches!(
            step,
            BootstrapStep::Effect(BootstrapEffect::EstablishFence(_))
        ),
        "the bounded first hole requests the takeover FENCE"
    );
    let mut step = machine.handle(BootstrapEvent::FenceEstablished(WalEstablishment::Durable));
    assert!(
        matches!(
            step,
            BootstrapStep::Effect(BootstrapEffect::ReadWal { batch, .. }) if batch == BatchId::try_from(1)?
        ),
        "replay begins at batch one for a Genesis cut"
    );
    for raw_batch in 1..=durable {
        let batch = BatchId::try_from(raw_batch)?;
        let owner = if batch == durable_batch {
            OwnerToken::from(claim_generation)
        } else {
            OwnerToken::from(SealGeneration::try_from(raw_batch)?)
        };
        step = machine.handle(BootstrapEvent::WalRead(Some(WalObject::new(
            partition,
            batch,
            owner,
            WalBody::Fence,
        ))));
    }
    assert!(
        matches!(step, BootstrapStep::Effect(BootstrapEffect::ObserveHead)),
        "replay through the takeover FENCE requests the final head refresh"
    );
    let BootstrapStep::Complete(recovery) =
        machine.handle(BootstrapEvent::HeadObserved(Some(claim_generation)))
    else {
        return Err("recovery fixture reaches a complete bootstrap".into());
    };
    Ok(WriterMachine::from_recovery(recovery))
}

fn recovery_tree(
    generation: SealGeneration,
    store: StoreKind,
    ordinal: u32,
    populated: bool,
) -> TestResult<TreeVersion> {
    if !populated {
        return Ok(TreeVersion::empty());
    }
    let fresh = FreshIdentity::new(
        generation,
        AttemptId::new(SealGeneration::genesis(), 1),
        ordinal,
    )?;
    let table = TableRef::new(TableObjectId::new(fresh, store), NonZeroU64::MIN)?;
    Ok(TreeVersion::try_from(vec![SortedRun::try_from(vec![
        table,
    ])?])?)
}

pub(super) fn complete_success(effect: WriterEffect) -> TestResult<WriterEvent> {
    match effect {
        WriterEffect::EstablishWal(candidate) => Ok(WriterEvent::WalEstablished {
            batch: candidate.batch(),
            result: Ok(WalEstablishment::Durable),
        }),
        WriterEffect::PrepareCheckpoint(input) => {
            let ticket = input.ticket();
            Ok(WriterEvent::CheckpointPrepared {
                ticket,
                outcome: PreparationOutcome::Prepared(Box::new(prepared(input)?)),
            })
        }
        WriterEffect::PublishAuthority {
            ticket,
            candidate: _,
        } => Ok(WriterEvent::SealPublished {
            ticket,
            result: Ok(SealPublication::Authored),
        }),
        WriterEffect::Collect(input) => Ok(WriterEvent::CollectFinished {
            cut: input.into_parts().0,
        }),
    }
}

pub(super) fn prepared(input: CheckpointInput) -> TestResult<PreparedCheckpoint> {
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

pub(super) fn establish_wal(step: WriterStep) -> TestResult<BatchId> {
    let (outputs, exit) = step.into_parts();
    assert_eq!(None, exit, "WAL fixture step remains live");
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

pub(super) fn take_preparation(
    outputs: Vec<WriterOutput>,
) -> Option<(CheckpointTicket, CheckpointInput)> {
    outputs.into_iter().find_map(|output| match output {
        WriterOutput::Effect(WriterEffect::PrepareCheckpoint(input)) => {
            Some((input.ticket(), input))
        }
        WriterOutput::Effect(
            WriterEffect::EstablishWal(_)
            | WriterEffect::PublishAuthority { .. }
            | WriterEffect::Collect(_),
        )
        | WriterOutput::Action(_) => None,
    })
}

pub(super) fn assert_empty_step(step: WriterStep) {
    let (outputs, exit) = step.into_parts();
    assert!(outputs.is_empty(), "the scripted step emits no work");
    assert_eq!(None, exit, "the scripted empty step remains live");
}

pub(super) fn assert_publication(step: WriterStep, ticket: CheckpointTicket) {
    let (outputs, exit) = step.into_parts();
    assert_eq!(None, exit, "checkpoint publication request remains live");
    assert!(
        matches!(
            outputs.as_slice(),
            [WriterOutput::Effect(WriterEffect::PublishAuthority { ticket: observed, .. })]
                if *observed == ticket
        ),
        "checkpoint preparation emits its correlated publication"
    );
}

pub(super) fn has_publish_then_replies(step: WriterStep) -> bool {
    let (outputs, exit) = step.into_parts();
    exit.is_none()
        && matches!(
            outputs.as_slice(),
            [
                WriterOutput::Action(WriterAction::PublishView(_)),
                WriterOutput::Action(WriterAction::SendReplies(_))
            ]
        )
}

pub(super) fn published_view(outputs: &[WriterOutput]) -> Option<&Forest> {
    outputs.iter().find_map(|output| match output {
        WriterOutput::Action(WriterAction::PublishView(view)) => Some(view),
        WriterOutput::Effect(_)
        | WriterOutput::Action(
            WriterAction::SendReplies(_) | WriterAction::CancelCheckpointPreparation { .. },
        ) => None,
    })
}

pub(super) fn collection_cut(step: WriterStep) -> TestResult<BatchId> {
    let (outputs, exit) = step.into_parts();
    assert_eq!(None, exit, "collection request step remains live");
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

pub(super) fn execute_actions(step: WriterStep) {
    let (outputs, exit) = step.into_parts();
    assert_eq!(None, exit, "the immediate action step remains live");
    assert!(
        execute_outputs(outputs).is_empty(),
        "the scripted step contains immediate actions only"
    );
}

pub(super) fn execute_replies(step: WriterStep) {
    let (outputs, exit) = step.into_parts();
    assert_eq!(None, exit, "the immediate reply step remains live");
    assert!(
        !outputs.is_empty()
            && outputs
                .iter()
                .all(|output| matches!(output, WriterOutput::Action(WriterAction::SendReplies(_)))),
        "the scripted step emits replies without publishing or starting effects"
    );
    assert!(
        execute_outputs(outputs).is_empty(),
        "the scripted reply step contains no effects"
    );
}

pub(super) fn execute_outputs(outputs: Vec<WriterOutput>) -> Vec<WriterEffect> {
    let mut effects = Vec::new();
    for output in outputs {
        match output {
            WriterOutput::Action(WriterAction::SendReplies(replies)) => settle_replies(replies),
            WriterOutput::Action(
                WriterAction::PublishView(_) | WriterAction::CancelCheckpointPreparation { .. },
            ) => {}
            WriterOutput::Effect(effect) => effects.push(effect),
        }
    }
    effects
}

pub(super) fn settle_replies(replies: Vec<Completion>) {
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
