//! Behavioral claims for the resident forest fold.

use proptest::prelude::*;
use strom_domain::{ExpiryPolicy, StreamContentType, StreamId, StreamLifecycle};
use strom_storage_domain::{BatchId, DirectoryEntry, DirectoryKey, OperationFact, StreamUid};

use super::{Applied, FoldContradiction, Forest};

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
            let path = directory_key(&format!("events/{index}"))
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
        prop_assert_eq!(
            forest.path_count(),
            u64::try_from(count).expect("test count fits in u64")
        );
    }
}

#[test]
fn create_close_delete_follows_the_fact_effects() -> TestResult {
    let mut forest = Forest::empty();
    let path = directory_key("events/a")?;
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
    assert_eq!(1, forest.path_count());

    let path_b = directory_key("events/b")?;
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
fn every_rejected_fact_leaves_the_forest_unchanged() -> TestResult {
    let path = directory_key("events/live")?;
    let closed_path = directory_key("events/closed")?;
    let absent = directory_key("events/absent")?;
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
            create(directory_key("events/gap")?, wrong_uid),
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
        let before = observe(
            &forest,
            [&path, &closed_path, &absent],
            [uid, closed_uid, wrong_uid],
        );
        assert_eq!(Err(expected), forest.strict_fold(batch, &fact));
        assert_eq!(
            before,
            observe(
                &forest,
                [&path, &closed_path, &absent],
                [uid, closed_uid, wrong_uid]
            ),
            "a rejected fold does not change observable state"
        );
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct Observation {
    path_count: u64,
    paths: Vec<Option<DirectoryEntry>>,
    records: Vec<bool>,
}

fn directory_key(raw: &str) -> Result<DirectoryKey, Box<dyn std::error::Error>> {
    Ok(DirectoryKey::from(&raw.parse::<StreamId>()?))
}

fn create(path: DirectoryKey, uid: StreamUid) -> OperationFact {
    OperationFact::StreamCreated {
        path,
        uid,
        content_type: StreamContentType::octet_stream(),
        expiry: ExpiryPolicy::None,
        lifecycle: StreamLifecycle::Open,
    }
}

fn observe(forest: &Forest, paths: [&DirectoryKey; 3], uids: [StreamUid; 3]) -> Observation {
    Observation {
        path_count: forest.path_count(),
        paths: paths.into_iter().map(|path| forest.resolve(path)).collect(),
        records: uids
            .into_iter()
            .map(|uid| forest.record(uid).is_some())
            .collect(),
    }
}
