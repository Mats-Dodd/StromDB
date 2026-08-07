//! Behavioral claims for the resident forest fold.

use proptest::prelude::*;
use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle, StreamPath};
use strom_storage_domain::{BatchId, DirectoryEntry, LedgerCell, OperationFact, StreamUid};
use strom_storage_protocol::{Applied, FoldContradiction, Forest};

type TestResult = Result<(), Box<dyn std::error::Error>>;

proptest! {
    #[test]
    fn dense_create_histories_install_one_live_row_per_fact(count in 0usize..=64) {
        let mut forest = Forest::empty();
        for index in 0..count {
            let ordinal = u64::try_from(index)
                .expect("test index fits in u64")
                .checked_add(1)
                .expect("test ordinal fits in u64");
            let path = stream_path(&format!("events/{index}"))
                .expect("generated path is valid");
            let uid = StreamUid::try_from(ordinal).expect("ordinal is nonzero");
            let batch = BatchId::try_from(ordinal).expect("ordinal is nonzero");
            prop_assert_eq!(
                forest.strict_fold(batch, &create(path.clone(), uid)),
                Ok(Applied)
            );
            prop_assert_eq!(forest.resolve(&path), Some(DirectoryEntry::Live(uid)));
            prop_assert!(forest.record(uid).is_some());
        }
    }
}

#[test]
fn create_close_delete_follows_the_fact_effects() -> TestResult {
    let mut forest = Forest::empty();
    let path = stream_path("events/a")?;
    let uid = StreamUid::try_from(1)?;
    let created_at = BatchId::try_from(1)?;

    assert_eq!(
        Applied,
        forest.strict_fold(created_at, &create(path.clone(), uid))?
    );
    assert_eq!(
        Some(DirectoryEntry::Live(uid)),
        forest.resolve(&path),
        "create installs a live directory entry"
    );
    assert_eq!(
        Applied,
        forest.strict_fold(
            BatchId::try_from(2)?,
            &OperationFact::StreamClosed {
                path: path.clone(),
                uid,
            },
        )?
    );
    assert!(
        forest
            .record(uid)
            .is_some_and(|record| record.lifecycle().is_closed()),
        "close changes only the ledger lifecycle"
    );
    assert_eq!(
        Applied,
        forest.strict_fold(
            BatchId::try_from(3)?,
            &OperationFact::StreamDeleted {
                path: path.clone(),
                uid,
            },
        )?
    );
    assert_eq!(
        Some(DirectoryEntry::Tombstone(uid)),
        forest.resolve(&path),
        "delete leaves permanent path occupancy"
    );
    assert_eq!(None, forest.record(uid));

    let path_b = stream_path("events/b")?;
    let uid_2 = StreamUid::try_from(2)?;
    assert_eq!(
        Applied,
        forest.strict_fold(BatchId::try_from(4)?, &create(path_b.clone(), uid_2))?
    );
    assert_eq!(
        Some(DirectoryEntry::Live(uid_2)),
        forest.resolve(&path_b),
        "the dense successor counts tombstoned paths"
    );
    Ok(())
}

#[test]
fn checkpoint_cells_emit_final_rows_without_ledger_deletes() -> TestResult {
    let path_a = stream_path("events/a")?;
    let path_b = stream_path("events/b")?;
    let uid_a = StreamUid::try_from(1)?;
    let uid_b = StreamUid::try_from(2)?;
    let mut forest = Forest::empty();
    forest.strict_fold(BatchId::try_from(1)?, &create(path_a.clone(), uid_a))?;
    forest.strict_fold(BatchId::try_from(2)?, &create(path_b.clone(), uid_b))?;
    forest.strict_fold(
        BatchId::try_from(3)?,
        &OperationFact::StreamDeleted {
            path: path_a.clone(),
            uid: uid_a,
        },
    )?;

    let cells = forest.checkpoint_cells();
    assert_eq!(
        vec![
            (path_a, DirectoryEntry::Tombstone(uid_a)),
            (path_b, DirectoryEntry::Live(uid_b)),
        ],
        cells.directory
    );
    assert!(
        matches!(
            cells.ledger.as_slice(),
            [(observed, LedgerCell::Value(_record))] if *observed == uid_b
        ),
        "a full checkpoint carries only the resident Ledger value"
    );
    Ok(())
}

#[test]
fn every_rejected_fact_leaves_the_forest_unchanged() -> TestResult {
    let path = stream_path("events/live")?;
    let closed_path = stream_path("events/closed")?;
    let absent = stream_path("events/absent")?;
    let uid = StreamUid::try_from(1)?;
    let closed_uid = StreamUid::try_from(2)?;
    let wrong_uid = StreamUid::try_from(4)?;
    let batch = BatchId::try_from(3)?;
    let mut base = Forest::empty();
    base.strict_fold(BatchId::try_from(1)?, &create(path.clone(), uid))?;
    base.strict_fold(
        BatchId::try_from(2)?,
        &OperationFact::StreamCreated {
            path: closed_path.clone(),
            uid: closed_uid,
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Closed,
        },
    )?;

    let rejected = [
        (
            FoldContradiction::PathOccupied,
            create(path.clone(), wrong_uid),
        ),
        (
            FoldContradiction::UidNotDenseSuccessor,
            create(stream_path("events/gap")?, wrong_uid),
        ),
        (
            FoldContradiction::PathNotLive,
            OperationFact::StreamClosed {
                path: absent.clone(),
                uid,
            },
        ),
        (
            FoldContradiction::PathUidMismatch,
            OperationFact::StreamDeleted {
                path: path.clone(),
                uid: closed_uid,
            },
        ),
        (
            FoldContradiction::StreamAlreadyClosed,
            OperationFact::StreamClosed {
                path: closed_path.clone(),
                uid: closed_uid,
            },
        ),
    ];

    for (expected, fact) in rejected {
        let mut forest = base.clone();
        let before = forest.clone();
        assert_eq!(Err(expected), forest.strict_fold(batch, &fact));
        assert_eq!(
            before, forest,
            "a rejected fold does not change observable state"
        );
    }
    Ok(())
}

fn stream_path(raw: &str) -> Result<StreamPath, Box<dyn std::error::Error>> {
    Ok(raw.parse()?)
}

const fn create(path: StreamPath, uid: StreamUid) -> OperationFact {
    OperationFact::StreamCreated {
        path,
        uid,
        content_type: StreamContentType::octet_stream(),
        expiry: ExpiryPolicy::None,
        lifecycle: StreamLifecycle::Open,
    }
}
