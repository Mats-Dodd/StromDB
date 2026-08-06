//! Pure stream-command admission and WAL suffix accounting.

use std::num::NonZeroU64;

use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};
use strom_storage_domain::{
    BatchId, DirectoryEntry, DirectoryKey, OperationFact, PARTITION_PATH_OCCUPANCIES_MAX_V2,
    StreamUid, WAL_SUFFIX_COORDINATES_MAX_V2,
};

use crate::{Applied, FoldContradiction, Forest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateStream {
    pub(crate) path: DirectoryKey,
    pub(crate) content_type: StreamContentType,
    pub(crate) expiry: ExpiryPolicy,
    pub(crate) lifecycle: StreamLifecycle,
}

/// A command that did not enter admitted state or consume a WAL coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum AdmissionRefusal {
    #[error("stream path is already occupied")]
    PathOccupied,
    #[error("partition path capacity is exhausted")]
    PathCapacityExhausted,
    #[error("stream path is not live")]
    PathNotLive,
    #[error("partition writer is at a bounded capacity limit")]
    Overloaded,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AdmittedCommand {
    pub(crate) forest: Forest,
    pub(crate) fact: OperationFact,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CreateAdmission {
    Fact(AdmittedCommand),
    AlreadyExists,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CloseAdmission {
    Fact(AdmittedCommand),
    AlreadyClosed,
}

pub(crate) fn admit_create(
    admitted: &Forest,
    command: &CreateStream,
    batch: BatchId,
) -> Result<CreateAdmission, AdmissionRefusal> {
    match admitted.resolve(&command.path) {
        Some(DirectoryEntry::Tombstone(_)) => Err(AdmissionRefusal::PathOccupied),
        Some(DirectoryEntry::Live(uid)) => {
            let record = admitted
                .record(uid)
                .expect("a Live directory row has exactly one Ledger record");
            if record.content_type() == &command.content_type
                && record.expiry() == command.expiry
                && record.lifecycle() == command.lifecycle
            {
                Ok(CreateAdmission::AlreadyExists)
            } else {
                Err(AdmissionRefusal::PathOccupied)
            }
        }
        None => {
            let uid = decide_successor_uid(admitted.path_count()).map_err(|contradiction| {
                contradiction
                    .admission_refusal()
                    .expect("successor allocation only returns a caller-facing capacity refusal")
            })?;
            let fact = OperationFact::StreamCreated {
                path: command.path.clone(),
                uid,
                content_type: command.content_type.clone(),
                expiry: command.expiry,
                lifecycle: command.lifecycle,
            };
            apply_fact(admitted, batch, fact).map(CreateAdmission::Fact)
        }
    }
}

pub(crate) fn admit_close(
    admitted: &Forest,
    path: &DirectoryKey,
    batch: BatchId,
) -> Result<CloseAdmission, AdmissionRefusal> {
    let uid = resolve_live_uid(admitted, path)?;
    let record = admitted
        .record(uid)
        .expect("a Live directory row has exactly one Ledger record");
    if record.lifecycle().is_closed() {
        return Ok(CloseAdmission::AlreadyClosed);
    }
    apply_fact(
        admitted,
        batch,
        OperationFact::StreamClosed {
            path: path.clone(),
            uid,
        },
    )
    .map(CloseAdmission::Fact)
}

pub(crate) fn admit_delete(
    admitted: &Forest,
    path: &DirectoryKey,
    batch: BatchId,
) -> Result<AdmittedCommand, AdmissionRefusal> {
    apply_fact(
        admitted,
        batch,
        OperationFact::StreamDeleted {
            path: path.clone(),
            uid: resolve_live_uid(admitted, path)?,
        },
    )
}

/// One function owns both create-allocation gates, so capacity is proven
/// before the dense successor is constructed.
pub(crate) fn decide_successor_uid(path_count: u64) -> Result<StreamUid, FoldContradiction> {
    if path_count >= PARTITION_PATH_OCCUPANCIES_MAX_V2 {
        return Err(FoldContradiction::PathCapacityExhausted);
    }
    let successor = path_count
        .checked_add(1)
        .and_then(NonZeroU64::new)
        .expect("path_count below the V2 occupancy bound has a nonzero successor");
    Ok(StreamUid::from(successor))
}

/// Whether one proposed RUN and the reserved next claimant FENCE stay inside
/// the post-Seal suffix bound.
#[must_use]
pub(crate) fn decide_suffix_room(cut: Option<BatchId>, proposed: BatchId) -> bool {
    let cut = cut.map_or(0, BatchId::get);
    proposed
        .successor()
        .ok()
        .and_then(|reserved_fence| reserved_fence.get().checked_sub(cut))
        .is_some_and(|span| span > 1 && span <= WAL_SUFFIX_COORDINATES_MAX_V2)
}

impl FoldContradiction {
    const fn admission_refusal(self) -> Option<AdmissionRefusal> {
        match self {
            Self::PathOccupied => Some(AdmissionRefusal::PathOccupied),
            Self::PathCapacityExhausted => Some(AdmissionRefusal::PathCapacityExhausted),
            Self::PathNotLive => Some(AdmissionRefusal::PathNotLive),
            Self::StreamAlreadyClosed | Self::UidNotDenseSuccessor | Self::PathUidMismatch => None,
        }
    }
}

fn resolve_live_uid(forest: &Forest, path: &DirectoryKey) -> Result<StreamUid, AdmissionRefusal> {
    match forest.resolve(path) {
        Some(DirectoryEntry::Live(uid)) => Ok(uid),
        Some(DirectoryEntry::Tombstone(_)) | None => Err(AdmissionRefusal::PathNotLive),
    }
}

fn apply_fact(
    admitted: &Forest,
    batch: BatchId,
    fact: OperationFact,
) -> Result<AdmittedCommand, AdmissionRefusal> {
    let mut candidate = admitted.clone();
    match candidate.strict_fold(batch, &fact) {
        Ok(Applied) => Ok(AdmittedCommand {
            forest: candidate,
            fact,
        }),
        Err(contradiction) => Err(contradiction
            .admission_refusal()
            .expect("admission constructs the dense uid and exact path uid carried by its fact")),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use strom_domain::{StreamContentType, StreamTtl};

    use super::*;

    #[test]
    fn successor_allocation_accepts_the_last_slot_and_refuses_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        let last_slot = PARTITION_PATH_OCCUPANCIES_MAX_V2
            .checked_sub(1)
            .expect("the V2 occupancy bound is nonzero");
        assert_eq!(
            Ok(StreamUid::try_from(PARTITION_PATH_OCCUPANCIES_MAX_V2)?),
            decide_successor_uid(last_slot),
            "the final lifetime occupancy has one dense successor"
        );
        assert_eq!(
            Err(FoldContradiction::PathCapacityExhausted),
            decide_successor_uid(PARTITION_PATH_OCCUPANCIES_MAX_V2),
            "capacity is refused before successor arithmetic"
        );
        Ok(())
    }

    #[test]
    fn suffix_gate_reserves_exactly_one_takeover_coordinate()
    -> Result<(), Box<dyn std::error::Error>> {
        let last_genesis_run = BatchId::try_from(
            WAL_SUFFIX_COORDINATES_MAX_V2
                .checked_sub(1)
                .expect("the suffix bound is nonzero"),
        )?;
        let first_over_genesis = last_genesis_run.successor()?;
        assert!(
            decide_suffix_room(None, last_genesis_run),
            "the final RUN plus its FENCE fits exactly at the bound"
        );
        assert!(
            !decide_suffix_room(None, first_over_genesis),
            "one additional RUN would consume the reserved FENCE"
        );

        let cut = BatchId::try_from(700)?;
        let last_after_cut = BatchId::try_from(
            cut.get()
                .checked_add(WAL_SUFFIX_COORDINATES_MAX_V2)
                .and_then(|fence| fence.checked_sub(1))
                .expect("the small test coordinate fits"),
        )?;
        assert!(decide_suffix_room(Some(cut), last_after_cut));
        assert!(!decide_suffix_room(Some(cut), last_after_cut.successor()?));
        assert!(
            !decide_suffix_room(Some(cut), cut),
            "a proposed RUN must be strictly after the Seal cut"
        );
        Ok(())
    }

    #[test]
    fn suffix_gate_rejects_a_run_without_a_real_successor_coordinate()
    -> Result<(), Box<dyn std::error::Error>> {
        let cut = BatchId::try_from(u64::MAX - 2)?;
        assert!(decide_suffix_room(
            cut.into(),
            BatchId::try_from(u64::MAX - 1)?
        ));
        assert!(!decide_suffix_room(
            cut.into(),
            BatchId::try_from(u64::MAX)?
        ));
        Ok(())
    }

    #[test]
    fn every_refusal_leaves_the_forest_unchanged() -> Result<(), Box<dyn std::error::Error>> {
        let path = DirectoryKey::try_from(Box::<[u8]>::from(b"events/a".as_slice()))?;
        let batch = BatchId::try_from(1)?;
        let create = CreateStream {
            path: path.clone(),
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Open,
        };
        let CreateAdmission::Fact(admitted) = admit_create(&Forest::empty(), &create, batch)?
        else {
            return Err("first create must produce a fact".into());
        };
        let mut forest = admitted.forest;
        let before = forest.clone();
        let mismatched = CreateStream {
            path: path.clone(),
            content_type: "text/plain".parse()?,
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Open,
        };
        assert_eq!(
            Err(AdmissionRefusal::PathOccupied),
            admit_create(&forest, &mismatched, batch)
        );
        assert_eq!(before.path_count(), forest.path_count());
        assert_eq!(before.resolve(&path), forest.resolve(&path));

        assert_eq!(
            CreateAdmission::AlreadyExists,
            admit_create(&forest, &create, batch)?
        );
        assert_eq!(before.path_count(), forest.path_count());

        let CloseAdmission::Fact(closed) = admit_close(&forest, &path, batch)? else {
            return Err("first close must produce a fact".into());
        };
        forest = closed.forest;
        let record_before = forest.record(StreamUid::try_from(1)?).cloned();
        assert_eq!(
            Ok(CloseAdmission::AlreadyClosed),
            admit_close(&forest, &path, batch)
        );
        assert_eq!(
            record_before.as_ref(),
            forest.record(StreamUid::try_from(1)?)
        );

        let missing = DirectoryKey::try_from(Box::<[u8]>::from(b"events/missing".as_slice()))?;
        let before_count = forest.path_count();
        assert_eq!(
            Err(AdmissionRefusal::PathNotLive),
            admit_delete(&forest, &missing, batch)
        );
        assert_eq!(before_count, forest.path_count());
        Ok(())
    }

    #[test]
    fn create_closed_installs_closed_lifecycle_and_is_idempotent()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = DirectoryKey::try_from(Box::<[u8]>::from(b"events/closed".as_slice()))?;
        let batch = BatchId::try_from(1)?;
        let create_closed = CreateStream {
            path: path.clone(),
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Closed,
        };
        let CreateAdmission::Fact(admitted) =
            admit_create(&Forest::empty(), &create_closed, batch)?
        else {
            return Err("create-closed must produce a fact".into());
        };
        let uid = StreamUid::try_from(1)?;
        assert_eq!(
            Some(StreamLifecycle::Closed),
            admitted
                .forest
                .record(uid)
                .map(strom_storage_domain::StreamRecord::lifecycle),
            "one StreamCreated fact carries Closed into the ledger"
        );
        assert_eq!(
            Ok(CreateAdmission::AlreadyExists),
            admit_create(&admitted.forest, &create_closed, batch)
        );
        assert_eq!(
            Err(AdmissionRefusal::PathOccupied),
            admit_create(
                &admitted.forest,
                &CreateStream {
                    path,
                    content_type: StreamContentType::octet_stream(),
                    expiry: ExpiryPolicy::None,
                    lifecycle: StreamLifecycle::Open,
                },
                batch
            ),
            "open-create on a create-closed stream is a config mismatch"
        );
        Ok(())
    }

    #[test]
    fn create_refuses_each_config_mismatch_axis() -> Result<(), Box<dyn std::error::Error>> {
        let path = DirectoryKey::try_from(Box::<[u8]>::from(b"events/match".as_slice()))?;
        let batch = BatchId::try_from(1)?;
        let create = CreateStream {
            path: path.clone(),
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Open,
        };
        let CreateAdmission::Fact(admitted) = admit_create(&Forest::empty(), &create, batch)?
        else {
            return Err("first create must produce a fact".into());
        };
        let forest = admitted.forest;

        assert_eq!(
            Err(AdmissionRefusal::PathOccupied),
            admit_create(
                &forest,
                &CreateStream {
                    path: path.clone(),
                    content_type: "text/plain".parse()?,
                    expiry: ExpiryPolicy::None,
                    lifecycle: StreamLifecycle::Open,
                },
                batch
            ),
            "content type mismatch refuses"
        );
        assert_eq!(
            Err(AdmissionRefusal::PathOccupied),
            admit_create(
                &forest,
                &CreateStream {
                    path: path.clone(),
                    content_type: StreamContentType::octet_stream(),
                    expiry: ExpiryPolicy::SlidingTtl(StreamTtl::from(
                        NonZeroU64::new(60).expect("sixty is nonzero")
                    )),
                    lifecycle: StreamLifecycle::Open,
                },
                batch
            ),
            "expiry mismatch refuses"
        );
        assert_eq!(
            Err(AdmissionRefusal::PathOccupied),
            admit_create(
                &forest,
                &CreateStream {
                    path,
                    content_type: StreamContentType::octet_stream(),
                    expiry: ExpiryPolicy::None,
                    lifecycle: StreamLifecycle::Closed,
                },
                batch
            ),
            "lifecycle mismatch refuses"
        );
        Ok(())
    }

    #[test]
    fn delete_preserves_path_occupancy_and_the_next_create_gets_the_dense_successor()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = DirectoryKey::try_from(Box::<[u8]>::from(b"events/a".as_slice()))?;
        let second = DirectoryKey::try_from(Box::<[u8]>::from(b"events/b".as_slice()))?;
        let batch = BatchId::try_from(1)?;
        let create = |path| CreateStream {
            path,
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Open,
        };
        let CreateAdmission::Fact(first_create) =
            admit_create(&Forest::empty(), &create(first.clone()), batch)?
        else {
            return Err("first create must produce a fact".into());
        };
        let deleted = admit_delete(&first_create.forest, &first, batch)?;
        let forest = deleted.forest;
        assert_eq!(
            Err(AdmissionRefusal::PathOccupied),
            admit_create(&forest, &create(first), batch)
        );
        let CreateAdmission::Fact(admitted) = admit_create(&forest, &create(second), batch)? else {
            return Err("create on a free path must produce a fact".into());
        };
        assert_eq!(
            Some(DirectoryEntry::Live(StreamUid::try_from(2)?)),
            admitted
                .forest
                .resolve(&DirectoryKey::try_from(Box::<[u8]>::from(
                    b"events/b".as_slice()
                ))?)
        );
        Ok(())
    }
}
