//! Behavioral claims for the pure forest fold: fact effects, contradictions,
//! and equivalence with a two-map reference model.

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;
use strom_domain::{ExpiryPolicy, StreamContentType, StreamId, StreamLifecycle};
use strom_storage_domain::{
    BatchId, DirectoryEntry, DirectoryKey, OperationFact, PARTITION_PATH_OCCUPANCIES_MAX_V2,
    StreamRecord, StreamUid,
};
use stromdb::{Applied, FoldContradiction, Forest};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReferenceForest {
    directory: BTreeMap<DirectoryKey, DirectoryEntry>,
    ledger: BTreeMap<StreamUid, StreamRecord>,
}

impl ReferenceForest {
    const fn empty() -> Self {
        Self {
            directory: BTreeMap::new(),
            ledger: BTreeMap::new(),
        }
    }

    fn path_count(&self) -> u64 {
        u64::try_from(self.directory.len()).expect("directory occupancy fits in u64")
    }

    fn record(&self, uid: StreamUid) -> Option<&StreamRecord> {
        self.ledger.get(&uid)
    }

    fn fold(&mut self, batch: BatchId, fact: &OperationFact) -> Result<(), FoldContradiction> {
        match fact {
            OperationFact::StreamCreated {
                path,
                uid,
                content_type,
                expiry,
            } => {
                if self.directory.contains_key(path) {
                    return Err(FoldContradiction::PathOccupied);
                }
                if self.path_count() >= PARTITION_PATH_OCCUPANCIES_MAX_V2 {
                    return Err(FoldContradiction::PathCapacityExhausted);
                }
                let next = self
                    .path_count()
                    .checked_add(1)
                    .expect("path occupancy stays below u64::MAX at test scale");
                if StreamUid::try_from(next) != Ok(*uid) {
                    return Err(FoldContradiction::UidNotDenseSuccessor);
                }
                self.directory
                    .insert(path.clone(), DirectoryEntry::Live(*uid));
                self.ledger.insert(
                    *uid,
                    StreamRecord::new(content_type.clone(), *expiry, StreamLifecycle::Open, batch),
                );
                Ok(())
            }
            OperationFact::StreamClosed { path, uid } => {
                match self.directory.get(path) {
                    Some(DirectoryEntry::Live(live_uid)) if live_uid == uid => {}
                    Some(DirectoryEntry::Live(_)) => {
                        return Err(FoldContradiction::PathUidMismatch);
                    }
                    Some(DirectoryEntry::Tombstone(_)) | None => {
                        return Err(FoldContradiction::PathNotLive);
                    }
                }
                let record = self
                    .ledger
                    .get(uid)
                    .expect("a Live directory row has exactly one Ledger record");
                if record.lifecycle().is_closed() {
                    return Err(FoldContradiction::StreamAlreadyClosed);
                }
                let closed = StreamRecord::new(
                    record.content_type().clone(),
                    record.expiry(),
                    StreamLifecycle::Closed,
                    record.created_at(),
                );
                self.ledger.insert(*uid, closed);
                Ok(())
            }
            OperationFact::StreamDeleted { path, uid } => {
                match self.directory.get(path) {
                    Some(DirectoryEntry::Live(live_uid)) if live_uid == uid => {}
                    Some(DirectoryEntry::Live(_)) => {
                        return Err(FoldContradiction::PathUidMismatch);
                    }
                    Some(DirectoryEntry::Tombstone(_)) | None => {
                        return Err(FoldContradiction::PathNotLive);
                    }
                }
                assert!(
                    self.ledger.contains_key(uid),
                    "a Live directory row has exactly one Ledger record"
                );
                self.directory
                    .insert(path.clone(), DirectoryEntry::Tombstone(*uid));
                self.ledger.remove(uid);
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Observation {
    path_count: u64,
    resolves: BTreeMap<DirectoryKey, Option<DirectoryEntry>>,
    records: BTreeMap<StreamUid, Option<StreamRecord>>,
}

#[derive(Debug, Clone)]
enum PlannedStep {
    CreateFresh {
        path: DirectoryKey,
        content_type: StreamContentType,
        expiry: ExpiryPolicy,
    },
    CloseLive {
        index: u32,
    },
    DeleteLive {
        index: u32,
    },
}

proptest! {
    #[test]
    fn dense_histories_match_the_pure_two_map_reference_model(
        history in valid_dense_history(),
    ) {
        let mut forest = Forest::empty();
        let mut model = ReferenceForest::empty();
        for (batch, fact) in &history {
            prop_assert_eq!(
                model.fold(*batch, fact),
                Ok(()),
                "the constructive history must stay inside the fact-effects table"
            );
            prop_assert_eq!(
                forest.strict_fold(*batch, fact),
                Ok(Applied),
                "Forest must accept every fact the reference model accepts"
            );
            assert_matches_model(&forest, &model, *batch);
        }
    }

    #[test]
    fn invalid_tail_fact_is_rejected_and_leaves_forest_unchanged(
        (history, bad) in history_and_invalid_tail(),
    ) {
        let mut forest = Forest::empty();
        let mut model = ReferenceForest::empty();
        for (batch, fact) in &history {
            prop_assert_eq!(model.fold(*batch, fact), Ok(()));
            prop_assert_eq!(forest.strict_fold(*batch, fact), Ok(Applied));
        }

        let mut paths: Vec<DirectoryKey> = model.directory.keys().cloned().collect();
        if let Ok(extra) = "zz/invalid-tail".parse::<StreamId>() {
            let key = DirectoryKey::from(&extra);
            if !paths.iter().any(|path| path == &key) {
                paths.push(key);
            }
        }
        let mut uids = Vec::new();
        for raw in 1..=model.path_count() {
            uids.push(StreamUid::try_from(raw).expect("allocated uids are nonzero"));
        }
        let before = observe(&forest, &paths, &uids);
        let batch = next_batch_after(&history);

        let mut probe = model.clone();
        let model_verdict = probe.fold(batch, &bad);
        prop_assert!(
            model_verdict.is_err(),
            "the generated tail must contradict the reference model"
        );
        prop_assert_eq!(
            forest.strict_fold(batch, &bad),
            model_verdict.map(|()| Applied),
            "Forest must reject the invalid tail with the model's exact contradiction"
        );
        prop_assert_eq!(
            observe(&forest, &paths, &uids),
            before,
            "a rejected invalid tail must leave the forest observably unchanged"
        );
    }
}

#[test]
fn create_close_delete_scenario_matches_fact_effects_table() -> TestResult {
    let path_a = directory_key("events/a")?;
    let path_b = directory_key("events/b")?;
    let uid_a = stream_uid(1)?;
    let uid_b = stream_uid(2)?;
    let content_a: StreamContentType = "application/json".parse()?;
    let content_b: StreamContentType = "application/octet-stream".parse()?;
    let expiry_a = ExpiryPolicy::None;
    let expiry_b = ExpiryPolicy::None;
    let batch_1 = batch_id(1)?;
    let batch_2 = batch_id(2)?;
    let batch_3 = batch_id(3)?;
    let batch_4 = batch_id(4)?;

    let mut forest = Forest::empty();
    assert_eq!(0, forest.path_count(), "genesis has no path occupancies");
    assert_eq!(
        None,
        forest.resolve(&path_a),
        "an unseen path is absent from Directory"
    );

    assert_eq!(
        Applied,
        forest.strict_fold(
            batch_1,
            &create_fact(path_a.clone(), uid_a, content_a.clone(), expiry_a)
        )?,
        "the first dense create applies"
    );
    assert_eq!(
        forest.resolve(&path_a),
        Some(DirectoryEntry::Live(uid_a)),
        "create installs a Live Directory entry"
    );
    assert_eq!(
        forest.record(uid_a),
        Some(&StreamRecord::new(
            content_a.clone(),
            expiry_a,
            StreamLifecycle::Open,
            batch_1
        )),
        "create installs an open Ledger record whose created_at is the fold batch"
    );
    assert_eq!(1, forest.path_count(), "create consumes one path occupancy");

    assert_eq!(
        Applied,
        forest.strict_fold(
            batch_2,
            &create_fact(path_b.clone(), uid_b, content_b.clone(), expiry_b)
        )?,
        "the second dense create applies"
    );
    assert_eq!(
        forest.resolve(&path_b),
        Some(DirectoryEntry::Live(uid_b)),
        "the second create installs its Live Directory entry"
    );
    assert_eq!(
        forest.record(uid_b),
        Some(&StreamRecord::new(
            content_b.clone(),
            expiry_b,
            StreamLifecycle::Open,
            batch_2
        )),
        "the second create installs its open Ledger record"
    );
    assert_eq!(
        forest.resolve(&path_a),
        Some(DirectoryEntry::Live(uid_a)),
        "an earlier Live path is unchanged by a later create"
    );
    assert_eq!(2, forest.path_count(), "each create advances path_count");

    assert_eq!(
        Applied,
        forest.strict_fold(
            batch_3,
            &OperationFact::StreamClosed {
                path: path_a.clone(),
                uid: uid_a,
            }
        )?,
        "close of a live open stream applies"
    );
    assert_eq!(
        forest.resolve(&path_a),
        Some(DirectoryEntry::Live(uid_a)),
        "close does not change Directory"
    );
    assert_eq!(
        forest.record(uid_a),
        Some(&StreamRecord::new(
            content_a,
            expiry_a,
            StreamLifecycle::Closed,
            batch_1
        )),
        "close flips lifecycle to Closed and preserves content_type, expiry, and created_at"
    );
    assert_eq!(
        forest.resolve(&path_b),
        Some(DirectoryEntry::Live(uid_b)),
        "close leaves the other Live path untouched"
    );
    assert_eq!(
        forest.record(uid_b),
        Some(&StreamRecord::new(
            content_b,
            expiry_b,
            StreamLifecycle::Open,
            batch_2
        )),
        "close leaves the other Ledger record open"
    );
    assert_eq!(
        2,
        forest.path_count(),
        "close does not change path occupancy"
    );

    assert_eq!(
        Applied,
        forest.strict_fold(
            batch_4,
            &OperationFact::StreamDeleted {
                path: path_b.clone(),
                uid: uid_b,
            }
        )?,
        "delete of a live stream applies"
    );
    assert_eq!(
        forest.resolve(&path_b),
        Some(DirectoryEntry::Tombstone(uid_b)),
        "delete installs a permanent Directory tombstone"
    );
    assert_eq!(
        None,
        forest.record(uid_b),
        "delete removes the Ledger record"
    );
    assert_eq!(
        forest.resolve(&path_a),
        Some(DirectoryEntry::Live(uid_a)),
        "delete leaves the closed Live path untouched"
    );
    assert_eq!(
        2,
        forest.path_count(),
        "delete does not reclaim path occupancy"
    );

    assert_eq!(
        Applied,
        forest.strict_fold(
            batch_id(5)?,
            &OperationFact::StreamDeleted {
                path: path_a.clone(),
                uid: uid_a,
            }
        )?,
        "hard delete of a closed live stream applies"
    );
    assert_eq!(
        forest.resolve(&path_a),
        Some(DirectoryEntry::Tombstone(uid_a)),
        "delete after close tombstones the path"
    );
    assert_eq!(
        None,
        forest.record(uid_a),
        "delete after close removes the Ledger record"
    );
    assert_eq!(
        2,
        forest.path_count(),
        "delete after close retains the path occupancy"
    );
    Ok(())
}

#[test]
fn rejected_folds_enumerate_every_fact_effect_contradiction_and_leave_state_unchanged() -> TestResult
{
    let content: StreamContentType = "application/json".parse()?;
    let expiry = ExpiryPolicy::None;
    let path_a = directory_key("events/a")?;
    let path_b = directory_key("events/b")?;
    let path_c = directory_key("events/c")?;
    let path_absent = directory_key("events/absent")?;
    let uid_1 = stream_uid(1)?;
    let uid_2 = stream_uid(2)?;
    let uid_3 = stream_uid(3)?;
    let paths = [
        path_a.clone(),
        path_b.clone(),
        path_c.clone(),
        path_absent.clone(),
    ];
    let uids = [uid_1, uid_2, uid_3];

    let base = fixture_forest(&path_a, &path_b, uid_1, uid_2, &content, expiry)?;
    let before = observe(&base, &paths, &uids);

    let rejected: &[(FoldContradiction, OperationFact)] = &[
        (
            FoldContradiction::PathOccupied,
            create_fact(path_a.clone(), uid_3, content.clone(), expiry),
        ),
        (
            FoldContradiction::PathOccupied,
            create_fact(path_b.clone(), uid_3, content.clone(), expiry),
        ),
        (
            FoldContradiction::UidNotDenseSuccessor,
            create_fact(path_c.clone(), uid_1, content.clone(), expiry),
        ),
        (
            FoldContradiction::PathNotLive,
            OperationFact::StreamClosed {
                path: path_absent.clone(),
                uid: uid_1,
            },
        ),
        (
            FoldContradiction::PathNotLive,
            OperationFact::StreamDeleted {
                path: path_absent.clone(),
                uid: uid_1,
            },
        ),
        (
            FoldContradiction::PathNotLive,
            OperationFact::StreamClosed {
                path: path_b.clone(),
                uid: uid_2,
            },
        ),
        (
            FoldContradiction::PathNotLive,
            OperationFact::StreamDeleted {
                path: path_b.clone(),
                uid: uid_2,
            },
        ),
        (
            FoldContradiction::PathUidMismatch,
            OperationFact::StreamClosed {
                path: path_a.clone(),
                uid: uid_2,
            },
        ),
        (
            FoldContradiction::PathUidMismatch,
            OperationFact::StreamDeleted {
                path: path_a.clone(),
                uid: uid_2,
            },
        ),
        (
            FoldContradiction::StreamAlreadyClosed,
            OperationFact::StreamClosed {
                path: path_a.clone(),
                uid: uid_1,
            },
        ),
    ];

    for (expected, fact) in rejected {
        let mut forest = fixture_forest(&path_a, &path_b, uid_1, uid_2, &content, expiry)?;
        let before_case = observe(&forest, &paths, &uids);
        assert_eq!(
            before_case, before,
            "each contradiction case starts from the shared fixture observation"
        );
        assert_eq!(
            forest.strict_fold(batch_id(5)?, fact),
            Err(*expected),
            "rejected fact must name its FoldContradiction"
        );
        assert_eq!(
            observe(&forest, &paths, &uids),
            before_case,
            "a rejected fold must leave Directory, Ledger, and path_count unchanged"
        );
    }

    let mut empty = Forest::empty();
    let empty_before = observe(&empty, &paths, &uids);
    assert_eq!(
        Err(FoldContradiction::UidNotDenseSuccessor),
        empty.strict_fold(
            batch_id(1)?,
            &create_fact(path_a.clone(), uid_2, content.clone(), expiry)
        ),
        "create uid 2 into an empty forest is not the dense successor"
    );
    assert_eq!(
        observe(&empty, &paths, &uids),
        empty_before,
        "a rejected empty-forest create leaves genesis unchanged"
    );

    let mut reuse = Forest::empty();
    assert_eq!(
        Applied,
        reuse.strict_fold(
            batch_id(1)?,
            &create_fact(path_a.clone(), uid_1, content.clone(), expiry)
        )?,
        "first create with uid 1 applies"
    );
    let reuse_before = observe(&reuse, &paths, &uids);
    assert_eq!(
        Err(FoldContradiction::UidNotDenseSuccessor),
        reuse.strict_fold(
            batch_id(2)?,
            &create_fact(path_c.clone(), uid_1, content.clone(), expiry)
        ),
        "a second create that reuses uid 1 is not the dense successor"
    );
    assert_eq!(
        observe(&reuse, &paths, &uids),
        reuse_before,
        "rejected uid reuse leaves the forest unchanged"
    );

    Ok(())
}

fn directory_key(raw: &str) -> Result<DirectoryKey, Box<dyn std::error::Error>> {
    Ok(DirectoryKey::from(&raw.parse::<StreamId>()?))
}

fn stream_uid(raw: u64) -> Result<StreamUid, Box<dyn std::error::Error>> {
    Ok(StreamUid::try_from(raw)?)
}

fn fixture_forest(
    path_a: &DirectoryKey,
    path_b: &DirectoryKey,
    uid_1: StreamUid,
    uid_2: StreamUid,
    content: &StreamContentType,
    expiry: ExpiryPolicy,
) -> Result<Forest, Box<dyn std::error::Error>> {
    let mut forest = Forest::empty();
    assert_eq!(
        Applied,
        forest.strict_fold(
            batch_id(1)?,
            &create_fact(path_a.clone(), uid_1, content.clone(), expiry)
        )?,
        "fixture create for path a applies"
    );
    assert_eq!(
        Applied,
        forest.strict_fold(
            batch_id(2)?,
            &create_fact(path_b.clone(), uid_2, content.clone(), expiry)
        )?,
        "fixture create for path b applies"
    );
    assert_eq!(
        Applied,
        forest.strict_fold(
            batch_id(3)?,
            &OperationFact::StreamDeleted {
                path: path_b.clone(),
                uid: uid_2,
            }
        )?,
        "fixture delete for path b applies and leaves a tombstone"
    );
    assert_eq!(
        Applied,
        forest.strict_fold(
            batch_id(4)?,
            &OperationFact::StreamClosed {
                path: path_a.clone(),
                uid: uid_1,
            }
        )?,
        "fixture close for path a applies"
    );
    Ok(forest)
}

fn observe(forest: &Forest, paths: &[DirectoryKey], uids: &[StreamUid]) -> Observation {
    Observation {
        path_count: forest.path_count(),
        resolves: paths
            .iter()
            .map(|path| (path.clone(), forest.resolve(path)))
            .collect(),
        records: uids
            .iter()
            .map(|uid| (*uid, forest.record(*uid).cloned()))
            .collect(),
    }
}

fn history_and_invalid_tail()
-> impl Strategy<Value = (Vec<(BatchId, OperationFact)>, OperationFact)> {
    valid_dense_history().prop_flat_map(|history| {
        let model = fold_history(&history);
        invalid_fact_for(model).prop_map(move |bad| (history.clone(), bad))
    })
}

fn valid_dense_history() -> impl Strategy<Value = Vec<(BatchId, OperationFact)>> {
    proptest::collection::vec(planned_step(), 0..=24).prop_map(materialize_valid_history)
}

fn invalid_fact_for(model: ReferenceForest) -> BoxedStrategy<OperationFact> {
    let occupied: BTreeSet<DirectoryKey> = model.directory.keys().cloned().collect();
    let gap_uid = gap_uid_for(model.path_count());
    let successor_uid = StreamUid::try_from(
        model
            .path_count()
            .checked_add(1)
            .expect("successor uid stays in u64")
            .max(1),
    )
    .expect("successor uid is nonzero");

    let mut arms: Vec<BoxedStrategy<OperationFact>> = Vec::new();

    arms.push(
        (
            strom_domain::strategy::stream_id().prop_map(|id| DirectoryKey::from(&id)),
            strom_domain::strategy::stream_content_type(),
            strom_domain::strategy::expiry_policy(),
            Just(gap_uid),
            Just(occupied.clone()),
        )
            .prop_filter_map(
                "create with a uid gap on an unoccupied path",
                |(path, content_type, expiry, uid, occupied)| {
                    if occupied.contains(&path) {
                        return None;
                    }
                    Some(OperationFact::StreamCreated {
                        path,
                        uid,
                        content_type,
                        expiry,
                    })
                },
            )
            .boxed(),
    );

    arms.push(
        (
            strom_domain::strategy::stream_id().prop_map(|id| DirectoryKey::from(&id)),
            Just(StreamUid::try_from(1).expect("uid one is nonzero")),
            Just(occupied.clone()),
        )
            .prop_filter_map("close an absent path", |(path, uid, occupied)| {
                if occupied.contains(&path) {
                    return None;
                }
                Some(OperationFact::StreamClosed { path, uid })
            })
            .boxed(),
    );

    arms.push(
        (
            strom_domain::strategy::stream_id().prop_map(|id| DirectoryKey::from(&id)),
            Just(StreamUid::try_from(1).expect("uid one is nonzero")),
            Just(occupied),
        )
            .prop_filter_map("delete an absent path", |(path, uid, occupied)| {
                if occupied.contains(&path) {
                    return None;
                }
                Some(OperationFact::StreamDeleted { path, uid })
            })
            .boxed(),
    );

    for (path, entry) in model.directory {
        match entry {
            DirectoryEntry::Live(uid) => {
                arms.push(
                    (
                        Just(path.clone()),
                        Just(successor_uid),
                        strom_domain::strategy::stream_content_type(),
                        strom_domain::strategy::expiry_policy(),
                    )
                        .prop_map(|(path, uid, content_type, expiry)| {
                            OperationFact::StreamCreated {
                                path,
                                uid,
                                content_type,
                                expiry,
                            }
                        })
                        .boxed(),
                );
                let wrong = wrong_uid_for(uid);
                arms.push(
                    Just(OperationFact::StreamClosed {
                        path: path.clone(),
                        uid: wrong,
                    })
                    .boxed(),
                );
                arms.push(
                    Just(OperationFact::StreamDeleted {
                        path: path.clone(),
                        uid: wrong,
                    })
                    .boxed(),
                );
                if model
                    .ledger
                    .get(&uid)
                    .is_some_and(|record| record.lifecycle().is_closed())
                {
                    arms.push(
                        Just(OperationFact::StreamClosed {
                            path: path.clone(),
                            uid,
                        })
                        .boxed(),
                    );
                }
            }
            DirectoryEntry::Tombstone(uid) => {
                arms.push(
                    (
                        Just(path.clone()),
                        Just(successor_uid),
                        strom_domain::strategy::stream_content_type(),
                        strom_domain::strategy::expiry_policy(),
                    )
                        .prop_map(|(path, uid, content_type, expiry)| {
                            OperationFact::StreamCreated {
                                path,
                                uid,
                                content_type,
                                expiry,
                            }
                        })
                        .boxed(),
                );
                arms.push(
                    Just(OperationFact::StreamClosed {
                        path: path.clone(),
                        uid,
                    })
                    .boxed(),
                );
                arms.push(
                    Just(OperationFact::StreamDeleted {
                        path: path.clone(),
                        uid,
                    })
                    .boxed(),
                );
            }
        }
    }

    proptest::strategy::Union::new(arms).boxed()
}

fn assert_matches_model(forest: &Forest, model: &ReferenceForest, batch: BatchId) {
    assert_eq!(
        forest.path_count(),
        model.path_count(),
        "after batch {}: path_count equals Directory occupancy",
        batch.get()
    );
    for (path, entry) in &model.directory {
        assert_eq!(
            forest.resolve(path),
            Some(*entry),
            "after batch {}: Directory resolve must match the reference model",
            batch.get()
        );
        match entry {
            DirectoryEntry::Live(uid) => {
                assert_eq!(
                    forest.record(*uid),
                    model.record(*uid),
                    "after batch {}: every Live uid has exactly one Ledger record",
                    batch.get()
                );
            }
            DirectoryEntry::Tombstone(uid) => {
                assert_eq!(
                    None,
                    forest.record(*uid),
                    "after batch {}: every Tombstone uid has no Ledger record",
                    batch.get()
                );
            }
        }
    }
    for (uid, record) in &model.ledger {
        assert_eq!(
            forest.record(*uid),
            Some(record),
            "after batch {}: every Ledger record is reachable by its uid",
            batch.get()
        );
        assert!(
            model
                .directory
                .values()
                .any(|entry| matches!(entry, DirectoryEntry::Live(live) if *live == *uid)),
            "after batch {}: every Ledger value has exactly one Directory Live entry",
            batch.get()
        );
    }
    let mut seen = BTreeSet::new();
    for entry in model.directory.values() {
        let uid = match entry {
            DirectoryEntry::Live(uid) | DirectoryEntry::Tombstone(uid) => *uid,
        };
        assert!(
            seen.insert(uid),
            "after batch {}: every Directory uid is unique",
            batch.get()
        );
    }
    for raw in 1..=model.path_count() {
        let uid = StreamUid::try_from(raw).expect("uids from 1 through path_count are nonzero");
        assert!(
            seen.contains(&uid),
            "after batch {}: every uid from 1 through maximum has exactly one Directory row",
            batch.get()
        );
    }
}

fn fold_history(history: &[(BatchId, OperationFact)]) -> ReferenceForest {
    let mut model = ReferenceForest::empty();
    for (batch, fact) in history {
        model
            .fold(*batch, fact)
            .expect("a constructively valid history applies in the reference model");
    }
    model
}

fn next_batch_after(history: &[(BatchId, OperationFact)]) -> BatchId {
    let raw = u64::try_from(history.len())
        .ok()
        .and_then(|len| len.checked_add(1))
        .expect("history length plus one fits in u64");
    BatchId::try_from(raw).expect("batch ids start at one")
}

/// Materialize a valid dense history. Duplicate-path creates and close/delete
/// steps with an empty candidate set are skipped so every emitted fact applies.
fn materialize_valid_history(steps: Vec<PlannedStep>) -> Vec<(BatchId, OperationFact)> {
    let mut occupied = BTreeSet::new();
    let mut live_open: Vec<(DirectoryKey, StreamUid)> = Vec::new();
    let mut live: Vec<(DirectoryKey, StreamUid)> = Vec::new();
    let mut next_uid = 1u64;
    let mut history = Vec::new();
    let mut step_index = 1u64;

    for step in steps {
        match step {
            PlannedStep::CreateFresh {
                path,
                content_type,
                expiry,
            } => {
                if !occupied.insert(path.clone()) {
                    continue;
                }
                let uid = StreamUid::try_from(next_uid)
                    .expect("dense create uids start at one and stay nonzero");
                next_uid = next_uid
                    .checked_add(1)
                    .expect("test-scale histories stay below u64::MAX uids");
                let batch =
                    BatchId::try_from(step_index).expect("monotonic batch ids start at one");
                step_index = step_index
                    .checked_add(1)
                    .expect("test-scale histories stay below u64::MAX batches");
                history.push((
                    batch,
                    OperationFact::StreamCreated {
                        path: path.clone(),
                        uid,
                        content_type,
                        expiry,
                    },
                ));
                live_open.push((path.clone(), uid));
                live.push((path, uid));
            }
            PlannedStep::CloseLive { index } => {
                if live_open.is_empty() {
                    continue;
                }
                let choice = usize::try_from(index)
                    .ok()
                    .and_then(|raw| raw.checked_rem(live_open.len()))
                    .expect("a nonempty live_open set has a remainder index");
                let (path, uid) = live_open.remove(choice);
                let batch =
                    BatchId::try_from(step_index).expect("monotonic batch ids start at one");
                step_index = step_index
                    .checked_add(1)
                    .expect("test-scale histories stay below u64::MAX batches");
                history.push((batch, OperationFact::StreamClosed { path, uid }));
            }
            PlannedStep::DeleteLive { index } => {
                if live.is_empty() {
                    continue;
                }
                let choice = usize::try_from(index)
                    .ok()
                    .and_then(|raw| raw.checked_rem(live.len()))
                    .expect("a nonempty live set has a remainder index");
                let (path, uid) = live.remove(choice);
                live_open.retain(|(open_path, _)| open_path != &path);
                let batch =
                    BatchId::try_from(step_index).expect("monotonic batch ids start at one");
                step_index = step_index
                    .checked_add(1)
                    .expect("test-scale histories stay below u64::MAX batches");
                history.push((batch, OperationFact::StreamDeleted { path, uid }));
            }
        }
    }
    history
}

fn planned_step() -> impl Strategy<Value = PlannedStep> {
    prop_oneof![
        (
            strom_domain::strategy::stream_id().prop_map(|id| DirectoryKey::from(&id)),
            strom_domain::strategy::stream_content_type(),
            strom_domain::strategy::expiry_policy(),
        )
            .prop_map(|(path, content_type, expiry)| PlannedStep::CreateFresh {
                path,
                content_type,
                expiry,
            }),
        any::<u32>().prop_map(|index| PlannedStep::CloseLive { index }),
        any::<u32>().prop_map(|index| PlannedStep::DeleteLive { index }),
    ]
}

fn gap_uid_for(path_count: u64) -> StreamUid {
    let raw = path_count
        .checked_add(2)
        .expect("gap uid stays in u64")
        .max(2);
    StreamUid::try_from(raw).expect("gap uid is nonzero")
}

fn wrong_uid_for(uid: StreamUid) -> StreamUid {
    let one = StreamUid::try_from(1).expect("uid one is nonzero");
    let two = StreamUid::try_from(2).expect("uid two is nonzero");
    if uid == one { two } else { one }
}

fn batch_id(raw: u64) -> Result<BatchId, Box<dyn std::error::Error>> {
    Ok(BatchId::try_from(raw)?)
}

const fn create_fact(
    path: DirectoryKey,
    uid: StreamUid,
    content_type: StreamContentType,
    expiry: ExpiryPolicy,
) -> OperationFact {
    OperationFact::StreamCreated {
        path,
        uid,
        content_type,
        expiry,
    }
}
