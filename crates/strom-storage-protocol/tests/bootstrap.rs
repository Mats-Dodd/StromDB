//! Synchronous scripts for the pure bootstrap protocol.

#![expect(
    clippy::panic,
    reason = "script extractors report the unexpected step that failed the test"
)]
use std::num::NonZeroU64;
use std::panic::{AssertUnwindSafe, catch_unwind};

use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle, StreamPath};
use strom_storage_domain::{
    AttemptId, BatchId, DecodedTable, DirectoryEntry, FreshIdentity, LedgerCell, OperationFact,
    OwnerToken, PARTITION_BOOTSTRAP_BYTES_MAX_V2, PARTITION_BOOTSTRAP_OBJECTS_MAX_V2, PartitionId,
    SST_OBJECT_BYTES_MAX, Seal, SealGeneration, SortedRun, StoreKind, StreamRecord, StreamUid,
    TableObjectId, TableRef, TreeVersion, WAL_SUFFIX_COORDINATES_MAX_V2, WalBody, WalFacts,
    WalObject, WalReplayPoint,
};
use strom_storage_protocol::{
    BootstrapEffect, BootstrapEvent, BootstrapExit, BootstrapMachine, BootstrapStep,
    GenesisEstablishment, SealPublication, TypedStoreError, WalEstablishment,
};

use support::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn empty_namespace_establishes_genesis_claims_fences_and_completes() -> TestResult {
    let mut machine = BootstrapMachine::new();
    assert_observe_head(machine.handle(BootstrapEvent::Started {
        genesis_partition: partition(),
    }));
    let candidate = expect_genesis(machine.handle(BootstrapEvent::HeadObserved(None)));
    assert_eq!(SealGeneration::genesis(), candidate.generation());
    assert_read_seal(
        machine.handle(BootstrapEvent::GenesisEstablished(
            GenesisEstablishment::Established,
        )),
        SealGeneration::genesis(),
    );

    let claim = drive_seal_and_claim(&mut machine, genesis(partition()));
    assert_observe_wal_tail(claim);
    let fence = expect_fence(machine.handle(BootstrapEvent::WalTailObserved(None)));
    let claim_generation = SealGeneration::genesis().successor()?;
    assert_eq!(BatchId::try_from(1)?, fence.batch());
    assert_read_wal(
        machine.handle(BootstrapEvent::FenceEstablished(WalEstablishment::Durable)),
        BatchId::try_from(1)?,
    );
    assert_observe_head(machine.handle(BootstrapEvent::WalRead(Some(WalObject::new(
        partition(),
        BatchId::try_from(1)?,
        OwnerToken::from(claim_generation),
        WalBody::Fence,
    )))));
    let recovery =
        expect_complete(machine.handle(BootstrapEvent::HeadObserved(Some(claim_generation))));
    assert_eq!(partition(), recovery.partition());
    assert_eq!(BatchId::try_from(1)?, recovery.durable_batch());
    assert_eq!(
        &strom_storage_protocol::Forest::empty(),
        recovery.durable_forest()
    );
    Ok(())
}

#[test]
fn genesis_loser_rediscovers_and_adopts_the_winner() -> TestResult {
    let mut machine = BootstrapMachine::new();
    let winner = "11112222-3333-4444-8888-9999aaaabbbb".parse()?;
    run_script(
        &mut machine,
        [
            Turn::new(
                BootstrapEvent::Started {
                    genesis_partition: partition(),
                },
                Expected::ObserveHead,
            ),
            Turn::new(
                BootstrapEvent::HeadObserved(None),
                Expected::EstablishGenesis,
            ),
            Turn::new(
                BootstrapEvent::GenesisEstablished(GenesisEstablishment::LostRace),
                Expected::ObserveHead,
            ),
            Turn::new(
                BootstrapEvent::HeadObserved(Some(SealGeneration::genesis())),
                Expected::ReadSeal(SealGeneration::genesis()),
            ),
            Turn::new(
                BootstrapEvent::SealRead(Some(genesis(winner))),
                Expected::PublishClaim,
            ),
            Turn::new(
                BootstrapEvent::ClaimPublished(SealPublication::Authored),
                Expected::ObserveWalTail,
            ),
        ],
    );
    Ok(())
}

#[test]
fn claim_outcomes_preserve_direct_authorship_rules() -> TestResult {
    let generation = SealGeneration::genesis().successor()?;

    let (mut fenced, step) = machine_at_claim();
    drop(step);
    assert!(matches!(
        fenced.handle(BootstrapEvent::ClaimPublished(
            SealPublication::NoAuthority
        )),
        BootstrapStep::Exit(BootstrapExit::Fenced { observed }) if observed == generation
    ));

    let (mut unresolved, step) = machine_at_claim();
    drop(step);
    assert!(matches!(
        unresolved.handle(BootstrapEvent::ClaimPublished(SealPublication::Unresolved)),
        BootstrapStep::Exit(BootstrapExit::Retryable { .. })
    ));

    let (mut rejected, step) = machine_at_claim();
    drop(step);
    assert!(matches!(
        rejected.handle(BootstrapEvent::StoreFailed(TypedStoreError::Rejected {
            detail: "denied".into()
        })),
        BootstrapStep::Exit(BootstrapExit::Contradiction { .. })
    ));
    Ok(())
}

#[test]
fn occupied_fence_relists_and_requires_strict_advancement() -> TestResult {
    let (mut machine, _claim_generation) = claimed_empty()?;
    let first = expect_fence(machine.handle(BootstrapEvent::WalTailObserved(None)));
    assert_eq!(BatchId::try_from(1)?, first.batch());
    assert_observe_wal_tail(
        machine.handle(BootstrapEvent::FenceEstablished(WalEstablishment::Occupied)),
    );

    let step = machine.handle(BootstrapEvent::WalTailObserved(Some(BatchId::try_from(1)?)));
    assert_read_wal(step, BatchId::try_from(1)?);
    let old_owner = OwnerToken::from(SealGeneration::genesis());
    let second = expect_fence(machine.handle(BootstrapEvent::WalRead(Some(WalObject::new(
        partition(),
        BatchId::try_from(1)?,
        old_owner,
        WalBody::Fence,
    )))));
    assert_eq!(BatchId::try_from(2)?, second.batch());
    Ok(())
}

#[test]
fn occupied_fence_rejects_a_nonadvancing_list() -> TestResult {
    let (mut machine, _claim_generation) = claimed_empty()?;
    drop(expect_fence(
        machine.handle(BootstrapEvent::WalTailObserved(None)),
    ));
    assert_observe_wal_tail(
        machine.handle(BootstrapEvent::FenceEstablished(WalEstablishment::Occupied)),
    );
    assert!(matches!(
        machine.handle(BootstrapEvent::WalTailObserved(None)),
        BootstrapStep::Exit(BootstrapExit::Contradiction { .. })
    ));
    Ok(())
}

#[test]
fn stale_tail_owner_refreshes_before_attempting_a_fence() -> TestResult {
    let (mut machine, claim_generation) = claimed_empty()?;
    assert_read_wal(
        machine.handle(BootstrapEvent::WalTailObserved(Some(BatchId::try_from(1)?))),
        BatchId::try_from(1)?,
    );
    assert_observe_head(machine.handle(BootstrapEvent::WalRead(Some(WalObject::new(
        partition(),
        BatchId::try_from(1)?,
        OwnerToken::from(claim_generation),
        WalBody::Fence,
    )))));
    assert!(matches!(
        machine.handle(BootstrapEvent::HeadObserved(Some(claim_generation))),
        BootstrapStep::Exit(BootstrapExit::Contradiction { .. })
    ));
    Ok(())
}

#[test]
fn replay_accepts_owner_succession_and_strictly_folds_runs() -> TestResult {
    let (mut machine, claim_generation) = claimed_empty()?;
    let old_generation = SealGeneration::genesis();
    let old_owner = OwnerToken::from(old_generation);
    assert_read_wal(
        machine.handle(BootstrapEvent::WalTailObserved(Some(BatchId::try_from(2)?))),
        BatchId::try_from(2)?,
    );
    let listed_tail = WalObject::new(
        partition(),
        BatchId::try_from(2)?,
        old_owner,
        WalBody::Run(WalFacts::try_from(vec![create_fact("events/a", 1)?])?),
    );
    drop(expect_fence(
        machine.handle(BootstrapEvent::WalRead(Some(listed_tail))),
    ));
    assert_read_wal(
        machine.handle(BootstrapEvent::FenceEstablished(WalEstablishment::Durable)),
        BatchId::try_from(1)?,
    );
    assert_read_wal(
        machine.handle(BootstrapEvent::WalRead(Some(WalObject::new(
            partition(),
            BatchId::try_from(1)?,
            old_owner,
            WalBody::Fence,
        )))),
        BatchId::try_from(2)?,
    );
    assert_read_wal(
        machine.handle(BootstrapEvent::WalRead(Some(WalObject::new(
            partition(),
            BatchId::try_from(2)?,
            old_owner,
            WalBody::Run(WalFacts::try_from(vec![create_fact("events/a", 1)?])?),
        )))),
        BatchId::try_from(3)?,
    );
    assert_observe_head(machine.handle(BootstrapEvent::WalRead(Some(WalObject::new(
        partition(),
        BatchId::try_from(3)?,
        OwnerToken::from(claim_generation),
        WalBody::Fence,
    )))));
    let recovery =
        expect_complete(machine.handle(BootstrapEvent::HeadObserved(Some(claim_generation))));
    assert_eq!(
        Some(DirectoryEntry::Live(StreamUid::try_from(1)?)),
        recovery
            .durable_forest()
            .resolve(&"events/a".parse::<StreamPath>()?)
    );
    Ok(())
}

#[test]
fn replay_owner_violation_is_classified_against_the_current_head() -> TestResult {
    let (mut machine, claim_generation) = claimed_empty()?;
    assert_read_wal(
        machine.handle(BootstrapEvent::WalTailObserved(Some(BatchId::try_from(1)?))),
        BatchId::try_from(1)?,
    );
    drop(expect_fence(machine.handle(BootstrapEvent::WalRead(Some(
        WalObject::new(
            partition(),
            BatchId::try_from(1)?,
            OwnerToken::from(SealGeneration::genesis()),
            WalBody::Fence,
        ),
    )))));
    assert_read_wal(
        machine.handle(BootstrapEvent::FenceEstablished(WalEstablishment::Durable)),
        BatchId::try_from(1)?,
    );
    assert_observe_head(machine.handle(BootstrapEvent::WalRead(Some(WalObject::new(
        partition(),
        BatchId::try_from(1)?,
        OwnerToken::from(SealGeneration::genesis()),
        WalBody::Run(WalFacts::try_from(vec![create_fact("events/a", 1)?])?),
    )))));
    let successor = claim_generation.successor()?;
    assert!(matches!(
        machine.handle(BootstrapEvent::HeadObserved(Some(successor))),
        BootstrapStep::Exit(BootstrapExit::Fenced { observed }) if observed == successor
    ));
    Ok(())
}

#[test]
fn replay_gap_with_a_current_claim_is_a_contradiction() -> TestResult {
    let (mut machine, claim_generation) = claimed_empty()?;
    drop(expect_fence(
        machine.handle(BootstrapEvent::WalTailObserved(None)),
    ));
    assert_read_wal(
        machine.handle(BootstrapEvent::FenceEstablished(WalEstablishment::Durable)),
        BatchId::try_from(1)?,
    );
    assert_observe_head(machine.handle(BootstrapEvent::WalRead(None)));
    assert!(matches!(
        machine.handle(BootstrapEvent::HeadObserved(Some(claim_generation))),
        BootstrapStep::Exit(BootstrapExit::Contradiction { .. })
    ));
    Ok(())
}

#[test]
fn table_reads_merge_oldest_to_newest_before_wal_replay() -> TestResult {
    let uid = StreamUid::try_from(1)?;
    let path = "events/base".parse::<StreamPath>()?;
    let head = seal_with_tables()?;
    let claim_generation = head.generation().successor()?;
    let mut machine = BootstrapMachine::new();
    assert_observe_head(machine.handle(BootstrapEvent::Started {
        genesis_partition: partition(),
    }));
    assert_read_seal(
        machine.handle(BootstrapEvent::HeadObserved(Some(head.generation()))),
        head.generation(),
    );
    drop(expect_claim(
        machine.handle(BootstrapEvent::SealRead(Some(head))),
    ));
    let directory_table = assert_read_table(
        machine.handle(BootstrapEvent::ClaimPublished(SealPublication::Authored)),
        StoreKind::Directory,
    );
    let older_ledger = assert_read_table(
        machine.handle(BootstrapEvent::TableRead {
            table: directory_table,
            decoded: DecodedTable::Directory(vec![(path.clone(), DirectoryEntry::Live(uid))]),
        }),
        StoreKind::Ledger,
    );
    let newer_ledger = assert_read_table(
        machine.handle(BootstrapEvent::TableRead {
            table: older_ledger,
            decoded: DecodedTable::Ledger(vec![(
                uid,
                LedgerCell::Value(record(StreamLifecycle::Open)?),
            )]),
        }),
        StoreKind::Ledger,
    );
    assert_observe_wal_tail(machine.handle(BootstrapEvent::TableRead {
        table: newer_ledger,
        decoded: DecodedTable::Ledger(vec![(
            uid,
            LedgerCell::Value(record(StreamLifecycle::Closed)?),
        )]),
    }));
    drop(expect_fence(
        machine.handle(BootstrapEvent::WalTailObserved(None)),
    ));
    assert_read_wal(
        machine.handle(BootstrapEvent::FenceEstablished(WalEstablishment::Durable)),
        BatchId::try_from(1)?,
    );
    assert_observe_head(machine.handle(BootstrapEvent::WalRead(Some(WalObject::new(
        partition(),
        BatchId::try_from(1)?,
        OwnerToken::from(claim_generation),
        WalBody::Fence,
    )))));
    let recovery =
        expect_complete(machine.handle(BootstrapEvent::HeadObserved(Some(claim_generation))));
    assert!(
        recovery
            .durable_forest()
            .record(uid)
            .is_some_and(|record| record.lifecycle().is_closed())
    );
    Ok(())
}

#[test]
fn source_bounds_accept_the_limit_and_reject_one_beyond_before_claim_publication() -> TestResult {
    let generation = SealGeneration::genesis().successor()?;
    let object_limit = seal_with_table_sources(
        generation,
        PARTITION_BOOTSTRAP_OBJECTS_MAX_V2,
        NonZeroU64::MIN,
    )?;
    drop(expect_claim(selected_seal_step(object_limit)));
    let object_over = seal_with_table_sources(
        generation,
        PARTITION_BOOTSTRAP_OBJECTS_MAX_V2
            .checked_add(1)
            .expect("the object-bound successor fits usize"),
        NonZeroU64::MIN,
    )?;
    assert!(matches!(
        selected_seal_step(object_over),
        BootstrapStep::Exit(BootstrapExit::Contradiction { .. })
    ));

    let table_bytes_max =
        NonZeroU64::new(SST_OBJECT_BYTES_MAX).expect("the maximum SST object length is nonzero");
    let byte_limit_count = usize::try_from(
        PARTITION_BOOTSTRAP_BYTES_MAX_V2
            .checked_div(SST_OBJECT_BYTES_MAX)
            .expect("the maximum SST object length is nonzero"),
    )?;
    assert_eq!(
        Some(PARTITION_BOOTSTRAP_BYTES_MAX_V2),
        u64::try_from(byte_limit_count)?.checked_mul(SST_OBJECT_BYTES_MAX),
        "the fixture lands exactly on the aggregate bootstrap byte bound"
    );
    let byte_limit = seal_with_table_sources(generation, byte_limit_count, table_bytes_max)?;
    drop(expect_claim(selected_seal_step(byte_limit)));
    let byte_over = seal_with_table_sources(
        generation,
        byte_limit_count
            .checked_add(1)
            .expect("the byte-bound successor count fits usize"),
        table_bytes_max,
    )?;
    assert!(matches!(
        selected_seal_step(byte_over),
        BootstrapStep::Exit(BootstrapExit::Contradiction { .. })
    ));
    Ok(())
}

#[test]
fn replay_span_accepts_the_limit_and_rejects_one_beyond() -> TestResult {
    let generation = SealGeneration::genesis().successor()?;
    let cut = BatchId::try_from(50)?;
    let head = Seal::new(
        partition(),
        generation,
        WalReplayPoint::Through {
            batch: cut,
            owner: OwnerToken::from(SealGeneration::genesis()),
        },
        TreeVersion::empty(),
        TreeVersion::empty(),
    )?;
    let fence_at_limit = BatchId::try_from(
        cut.get()
            .checked_add(WAL_SUFFIX_COORDINATES_MAX_V2)
            .expect("the bounded fixture coordinate fits"),
    )?;
    let tail_at_limit = BatchId::try_from(
        fence_at_limit
            .get()
            .checked_sub(1)
            .expect("the FENCE has a preceding WAL coordinate"),
    )?;

    let mut accepted = claimed_empty_head(head.clone());
    assert_read_wal(
        accepted.handle(BootstrapEvent::WalTailObserved(Some(tail_at_limit))),
        tail_at_limit,
    );
    let fence = expect_fence(accepted.handle(BootstrapEvent::WalRead(Some(WalObject::new(
        partition(),
        tail_at_limit,
        OwnerToken::from(SealGeneration::genesis()),
        WalBody::Fence,
    )))));
    assert_eq!(fence_at_limit, fence.batch());

    let mut rejected = claimed_empty_head(head);
    assert!(matches!(
        rejected.handle(BootstrapEvent::WalTailObserved(Some(fence_at_limit))),
        BootstrapStep::Exit(BootstrapExit::Retryable { .. })
    ));
    Ok(())
}

#[test]
fn same_run_overlap_is_a_contradiction() -> TestResult {
    let generation = SealGeneration::genesis().successor()?;
    let directory = TreeVersion::try_from(vec![SortedRun::try_from(vec![
        table_ref(generation, StoreKind::Directory, 0)?,
        table_ref(generation, StoreKind::Directory, 1)?,
    ])?])?;
    let head = Seal::new(
        partition(),
        generation,
        WalReplayPoint::Genesis,
        directory,
        TreeVersion::empty(),
    )?;

    let (mut overlap, first_table) = machine_reading_first_table(head);
    let later = "events/z".parse::<StreamPath>()?;
    let earlier = "events/a".parse::<StreamPath>()?;
    let second_table = assert_read_table(
        overlap.handle(BootstrapEvent::TableRead {
            table: first_table,
            decoded: DecodedTable::Directory(vec![(
                later,
                DirectoryEntry::Live(StreamUid::try_from(1)?),
            )]),
        }),
        StoreKind::Directory,
    );
    assert!(matches!(
        overlap.handle(BootstrapEvent::TableRead {
            table: second_table,
            decoded: DecodedTable::Directory(vec![(
                earlier,
                DirectoryEntry::Live(StreamUid::try_from(2)?),
            )]),
        }),
        BootstrapStep::Exit(BootstrapExit::Contradiction { .. })
    ));
    Ok(())
}

#[test]
fn final_refresh_rejects_fencing_regression_and_empty_namespace() -> TestResult {
    let (mut fenced, generation) = machine_at_final_refresh()?;
    let successor = generation.successor()?;
    assert!(matches!(
        fenced.handle(BootstrapEvent::HeadObserved(Some(successor))),
        BootstrapStep::Exit(BootstrapExit::Fenced { observed }) if observed == successor
    ));

    let (mut regressed, _generation) = machine_at_final_refresh()?;
    assert!(matches!(
        regressed.handle(BootstrapEvent::HeadObserved(
            Some(SealGeneration::genesis())
        )),
        BootstrapStep::Exit(BootstrapExit::Contradiction { .. })
    ));

    let (mut empty, _generation) = machine_at_final_refresh()?;
    assert!(matches!(
        empty.handle(BootstrapEvent::HeadObserved(None)),
        BootstrapStep::Exit(BootstrapExit::Contradiction { .. })
    ));
    Ok(())
}

#[test]
fn unresolved_fence_and_retryable_replay_read_stop_without_more_effects() -> TestResult {
    let (mut unresolved, _generation) = claimed_empty()?;
    drop(expect_fence(
        unresolved.handle(BootstrapEvent::WalTailObserved(None)),
    ));
    assert!(matches!(
        unresolved.handle(BootstrapEvent::FenceEstablished(
            WalEstablishment::UnresolvedAbsent
        )),
        BootstrapStep::Exit(BootstrapExit::Retryable { .. })
    ));

    let (mut retryable, _generation) = claimed_empty()?;
    drop(expect_fence(
        retryable.handle(BootstrapEvent::WalTailObserved(None)),
    ));
    assert_read_wal(
        retryable.handle(BootstrapEvent::FenceEstablished(WalEstablishment::Durable)),
        BatchId::try_from(1)?,
    );
    assert!(matches!(
        retryable.handle(BootstrapEvent::StoreFailed(TypedStoreError::Retryable {
            detail: "transport".into(),
        })),
        BootstrapStep::Exit(BootstrapExit::Retryable { .. })
    ));
    Ok(())
}

#[test]
fn duplicate_out_of_order_and_after_terminal_events_panic() {
    let mut duplicate = BootstrapMachine::new();
    drop(duplicate.handle(BootstrapEvent::Started {
        genesis_partition: partition(),
    }));
    assert_panics(|| {
        duplicate.handle(BootstrapEvent::Started {
            genesis_partition: partition(),
        })
    });

    let mut out_of_order = BootstrapMachine::new();
    drop(out_of_order.handle(BootstrapEvent::Started {
        genesis_partition: partition(),
    }));
    assert_panics(|| out_of_order.handle(BootstrapEvent::WalTailObserved(None)));

    let (mut terminal, step) = machine_at_claim();
    drop(step);
    assert!(matches!(
        terminal.handle(BootstrapEvent::ClaimPublished(SealPublication::NoAuthority)),
        BootstrapStep::Exit(BootstrapExit::Fenced { .. })
    ));
    assert_panics(|| terminal.handle(BootstrapEvent::HeadObserved(None)));
}

#[test]
fn terminal_run_and_miscorrelated_table_completion_panic() -> TestResult {
    let (mut terminal_run, claim_generation) = claimed_empty()?;
    drop(expect_fence(
        terminal_run.handle(BootstrapEvent::WalTailObserved(None)),
    ));
    drop(terminal_run.handle(BootstrapEvent::FenceEstablished(WalEstablishment::Durable)));
    assert_panics(|| {
        terminal_run.handle(BootstrapEvent::WalRead(Some(WalObject::new(
            partition(),
            BatchId::try_from(1).expect("batch one is canonical"),
            OwnerToken::from(claim_generation),
            WalBody::Run(
                WalFacts::try_from(vec![
                    create_fact("events/not-a-fence", 1).expect("fixture fact is canonical"),
                ])
                .expect("one fact is a canonical RUN"),
            ),
        ))))
    });

    let generation = SealGeneration::genesis().successor()?;
    let directory = TreeVersion::try_from(vec![SortedRun::try_from(vec![
        table_ref(generation, StoreKind::Directory, 0)?,
        table_ref(generation, StoreKind::Directory, 1)?,
    ])?])?;
    let head = Seal::new(
        partition(),
        generation,
        WalReplayPoint::Genesis,
        directory,
        TreeVersion::empty(),
    )?;
    let (mut table_machine, issued) = machine_reading_first_table(head);
    let other = table_ref(generation, StoreKind::Directory, 1)?;
    assert_ne!(issued, other);
    assert_panics(|| {
        table_machine.handle(BootstrapEvent::TableRead {
            table: other,
            decoded: DecodedTable::Directory(vec![(
                "events/a".parse().expect("fixture path is canonical"),
                DirectoryEntry::Live(StreamUid::try_from(1).expect("uid one is canonical")),
            )]),
        })
    });
    Ok(())
}

mod support {
    use super::*;

    #[derive(Debug, Clone, Copy)]
    pub(super) enum Expected {
        ObserveHead,
        EstablishGenesis,
        ReadSeal(SealGeneration),
        PublishClaim,
        ObserveWalTail,
    }

    #[derive(Debug)]
    pub(super) struct Turn {
        event: BootstrapEvent,
        expected: Expected,
    }

    impl Turn {
        pub(super) const fn new(event: BootstrapEvent, expected: Expected) -> Self {
            Self { event, expected }
        }
    }

    impl Expected {
        fn assert(self, step: BootstrapStep) {
            let matches = match self {
                Self::ObserveHead => {
                    matches!(step, BootstrapStep::Effect(BootstrapEffect::ObserveHead))
                }
                Self::EstablishGenesis => matches!(
                    step,
                    BootstrapStep::Effect(BootstrapEffect::EstablishGenesis(_))
                ),
                Self::ReadSeal(expected) => matches!(
                    step,
                    BootstrapStep::Effect(BootstrapEffect::ReadSeal { generation })
                        if generation == expected
                ),
                Self::PublishClaim => matches!(
                    step,
                    BootstrapStep::Effect(BootstrapEffect::PublishClaim(_))
                ),
                Self::ObserveWalTail => {
                    matches!(step, BootstrapStep::Effect(BootstrapEffect::ObserveWalTail))
                }
            };
            assert!(matches, "expected {self:?}, got {step:?}");
            drop(step);
        }
    }

    pub(super) fn run_script<const N: usize>(machine: &mut BootstrapMachine, turns: [Turn; N]) {
        for Turn { event, expected } in turns {
            expected.assert(machine.handle(event));
        }
    }

    pub(super) fn machine_at_final_refresh() -> TestResult<(BootstrapMachine, SealGeneration)> {
        let (mut machine, generation) = claimed_empty()?;
        drop(expect_fence(
            machine.handle(BootstrapEvent::WalTailObserved(None)),
        ));
        assert_read_wal(
            machine.handle(BootstrapEvent::FenceEstablished(WalEstablishment::Durable)),
            BatchId::try_from(1)?,
        );
        assert_observe_head(machine.handle(BootstrapEvent::WalRead(Some(WalObject::new(
            partition(),
            BatchId::try_from(1)?,
            OwnerToken::from(generation),
            WalBody::Fence,
        )))));
        Ok((machine, generation))
    }

    pub(super) fn claimed_empty() -> TestResult<(BootstrapMachine, SealGeneration)> {
        let (mut machine, step) = machine_at_claim();
        drop(step);
        assert_observe_wal_tail(
            machine.handle(BootstrapEvent::ClaimPublished(SealPublication::Authored)),
        );
        Ok((machine, SealGeneration::genesis().successor()?))
    }

    pub(super) fn claimed_empty_head(head: Seal) -> BootstrapMachine {
        let partition = head.partition();
        let generation = head.generation();
        let mut machine = BootstrapMachine::new();
        let step = machine.handle(BootstrapEvent::Started {
            genesis_partition: partition,
        });
        assert!(
            matches!(step, BootstrapStep::Effect(BootstrapEffect::ObserveHead)),
            "bootstrap begins by observing the Seal head, got {step:?}"
        );
        let step = machine.handle(BootstrapEvent::HeadObserved(Some(generation)));
        assert!(
            matches!(
                step,
                BootstrapStep::Effect(BootstrapEffect::ReadSeal {
                    generation: observed
                }) if observed == generation
            ),
            "the observed head selects its exact Seal, got {step:?}"
        );
        let step = machine.handle(BootstrapEvent::SealRead(Some(head)));
        assert!(
            matches!(
                step,
                BootstrapStep::Effect(BootstrapEffect::PublishClaim(_))
            ),
            "the selected Seal produces an exact claim candidate, got {step:?}"
        );
        let step = machine.handle(BootstrapEvent::ClaimPublished(SealPublication::Authored));
        assert!(
            matches!(step, BootstrapStep::Effect(BootstrapEffect::ObserveWalTail)),
            "an authored empty-base claim advances to WAL observation, got {step:?}"
        );
        machine
    }

    pub(super) fn assert_panics(mut action: impl FnMut() -> BootstrapStep) {
        assert!(
            catch_unwind(AssertUnwindSafe(&mut action)).is_err(),
            "invalid bootstrap sequencing panics"
        );
    }

    pub(super) fn machine_at_claim() -> (BootstrapMachine, BootstrapStep) {
        let mut machine = BootstrapMachine::new();
        assert_observe_head(machine.handle(BootstrapEvent::Started {
            genesis_partition: partition(),
        }));
        assert_read_seal(
            machine.handle(BootstrapEvent::HeadObserved(
                Some(SealGeneration::genesis()),
            )),
            SealGeneration::genesis(),
        );
        let step = machine.handle(BootstrapEvent::SealRead(Some(genesis(partition()))));
        assert!(
            matches!(
                step,
                BootstrapStep::Effect(BootstrapEffect::PublishClaim(_))
            ),
            "a decoded empty Seal requests its exact claim publication"
        );
        (machine, step)
    }

    pub(super) fn seal_with_tables() -> TestResult<Seal> {
        let generation = SealGeneration::genesis().successor()?;
        let directory = TreeVersion::try_from(vec![SortedRun::try_from(vec![table_ref(
            generation,
            StoreKind::Directory,
            0,
        )?])?])?;
        let older = SortedRun::try_from(vec![table_ref(generation, StoreKind::Ledger, 1)?])?;
        let newer = SortedRun::try_from(vec![table_ref(generation, StoreKind::Ledger, 2)?])?;
        Ok(Seal::new(
            partition(),
            generation,
            WalReplayPoint::Genesis,
            directory,
            TreeVersion::try_from(vec![newer, older])?,
        )?)
    }

    pub(super) fn table_ref(
        generation: SealGeneration,
        store: StoreKind,
        ordinal: u32,
    ) -> TestResult<TableRef> {
        table_ref_with_bytes(generation, store, ordinal, NonZeroU64::MIN)
    }

    pub(super) fn seal_with_table_sources(
        generation: SealGeneration,
        count: usize,
        object_bytes: NonZeroU64,
    ) -> TestResult<Seal> {
        let mut tables = Vec::with_capacity(count);
        for ordinal in 0..count {
            tables.push(table_ref_with_bytes(
                generation,
                StoreKind::Directory,
                u32::try_from(ordinal)?,
                object_bytes,
            )?);
        }
        Ok(Seal::new(
            partition(),
            generation,
            WalReplayPoint::Genesis,
            TreeVersion::try_from(vec![SortedRun::try_from(tables)?])?,
            TreeVersion::empty(),
        )?)
    }

    fn table_ref_with_bytes(
        generation: SealGeneration,
        store: StoreKind,
        ordinal: u32,
        object_bytes: NonZeroU64,
    ) -> TestResult<TableRef> {
        let fresh = FreshIdentity::new(
            generation,
            AttemptId::new(SealGeneration::genesis(), 1),
            ordinal,
        )?;
        Ok(TableRef::new(
            TableObjectId::new(fresh, store),
            object_bytes,
        )?)
    }

    pub(super) fn selected_seal_step(head: Seal) -> BootstrapStep {
        let mut machine = BootstrapMachine::new();
        assert_observe_head(machine.handle(BootstrapEvent::Started {
            genesis_partition: partition(),
        }));
        assert_read_seal(
            machine.handle(BootstrapEvent::HeadObserved(Some(head.generation()))),
            head.generation(),
        );
        machine.handle(BootstrapEvent::SealRead(Some(head)))
    }

    pub(super) fn assert_observe_wal_tail(step: BootstrapStep) {
        assert!(
            matches!(step, BootstrapStep::Effect(BootstrapEffect::ObserveWalTail)),
            "the transition requests a WAL-tail observation, got {step:?}"
        );
        drop(step);
    }

    pub(super) fn expect_fence(step: BootstrapStep) -> strom_storage_domain::EncodedWal {
        let BootstrapStep::Effect(BootstrapEffect::EstablishFence(candidate)) = step else {
            panic!("expected FENCE establishment, got {step:?}");
        };
        candidate
    }

    pub(super) fn assert_read_wal(step: BootstrapStep, expected: BatchId) {
        assert!(
            matches!(
                &step,
                BootstrapStep::Effect(BootstrapEffect::ReadWal { batch, .. }) if *batch == expected
            ),
            "the transition reads WAL batch {expected:?}, got {step:?}"
        );
        drop(step);
    }

    pub(super) fn machine_reading_first_table(head: Seal) -> (BootstrapMachine, TableRef) {
        let mut machine = BootstrapMachine::new();
        assert_observe_head(machine.handle(BootstrapEvent::Started {
            genesis_partition: partition(),
        }));
        assert_read_seal(
            machine.handle(BootstrapEvent::HeadObserved(Some(head.generation()))),
            head.generation(),
        );
        drop(expect_claim(
            machine.handle(BootstrapEvent::SealRead(Some(head))),
        ));
        let table = assert_read_table(
            machine.handle(BootstrapEvent::ClaimPublished(SealPublication::Authored)),
            StoreKind::Directory,
        );
        (machine, table)
    }

    pub(super) fn assert_observe_head(step: BootstrapStep) {
        assert!(
            matches!(step, BootstrapStep::Effect(BootstrapEffect::ObserveHead)),
            "the transition requests a Seal-head observation, got {step:?}"
        );
        drop(step);
    }

    pub(super) fn partition() -> PartitionId {
        "00112233-4455-6677-8899-aabbccddeeff"
            .parse()
            .expect("test partition is canonical")
    }

    pub(super) fn create_fact(path: &str, uid: u64) -> TestResult<OperationFact> {
        Ok(OperationFact::StreamCreated {
            path: path.parse()?,
            uid: StreamUid::try_from(uid)?,
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Open,
        })
    }

    pub(super) fn expect_genesis(step: BootstrapStep) -> strom_storage_domain::EncodedGenesisSeal {
        let BootstrapStep::Effect(BootstrapEffect::EstablishGenesis(candidate)) = step else {
            panic!("expected genesis establishment, got {step:?}");
        };
        candidate
    }

    pub(super) fn assert_read_seal(step: BootstrapStep, expected: SealGeneration) {
        assert!(
            matches!(
                &step,
                BootstrapStep::Effect(BootstrapEffect::ReadSeal { generation })
                    if *generation == expected
            ),
            "the transition reads Seal {expected:?}, got {step:?}"
        );
        drop(step);
    }

    pub(super) fn drive_seal_and_claim(
        machine: &mut BootstrapMachine,
        seal: Seal,
    ) -> BootstrapStep {
        drop(expect_claim(
            machine.handle(BootstrapEvent::SealRead(Some(seal))),
        ));
        machine.handle(BootstrapEvent::ClaimPublished(SealPublication::Authored))
    }

    pub(super) fn expect_claim(step: BootstrapStep) -> strom_storage_domain::EncodedAuthoritySeal {
        let BootstrapStep::Effect(BootstrapEffect::PublishClaim(candidate)) = step else {
            panic!("expected claim publication, got {step:?}");
        };
        candidate
    }

    pub(super) fn genesis(partition: PartitionId) -> Seal {
        Seal::new(
            partition,
            SealGeneration::genesis(),
            WalReplayPoint::Genesis,
            TreeVersion::empty(),
            TreeVersion::empty(),
        )
        .expect("empty genesis is canonical")
    }

    pub(super) fn assert_read_table(step: BootstrapStep, expected: StoreKind) -> TableRef {
        let BootstrapStep::Effect(BootstrapEffect::ReadTable { table, .. }) = &step else {
            panic!("expected a {expected:?} table read, got {step:?}");
        };
        assert_eq!(
            expected,
            table.object().store(),
            "the transition reads the expected resident store"
        );
        let table = *table;
        drop(step);
        table
    }

    pub(super) fn record(lifecycle: StreamLifecycle) -> TestResult<StreamRecord> {
        Ok(StreamRecord::new(
            StreamContentType::octet_stream(),
            ExpiryPolicy::None,
            lifecycle,
            BatchId::try_from(1)?,
        ))
    }

    pub(super) fn expect_complete(step: BootstrapStep) -> strom_storage_protocol::WriterRecovery {
        let BootstrapStep::Complete(recovery) = step else {
            panic!("expected completed recovery, got {step:?}");
        };
        recovery
    }
}
