//! Pure stream-command admission and WAL suffix accounting.

use std::num::NonZeroU64;

use strom_domain::{ExpiryPolicy, StreamContentType};
use strom_storage_domain::{
    BatchId, DirectoryEntry, DirectoryKey, OperationFact, PARTITION_PATH_OCCUPANCIES_MAX_V2,
    StreamUid, WAL_SUFFIX_COORDINATES_MAX_V2,
};

use crate::{Applied, FoldContradiction, Forest};

/// A mutation request after protocol parsing and before durable admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamCommand {
    Create {
        path: DirectoryKey,
        content_type: StreamContentType,
        expiry: ExpiryPolicy,
    },
    Close {
        path: DirectoryKey,
    },
    Delete {
        path: DirectoryKey,
    },
}

/// The durable result corresponding to one admitted command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamReply {
    Created { uid: StreamUid },
    Closed,
    Deleted,
}

/// A command that did not enter admitted state or consume a WAL coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionRefusal {
    #[error("stream path is already occupied")]
    PathOccupied,
    #[error("partition path capacity is exhausted")]
    PathCapacityExhausted,
    #[error("stream path is not live")]
    PathNotLive,
    #[error("stream is already closed")]
    StreamAlreadyClosed,
    #[error("partition writer is at a bounded capacity limit")]
    Overloaded,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AdmittedCommand {
    pub(crate) forest: Forest,
    pub(crate) fact: OperationFact,
    pub(crate) reply: StreamReply,
}

/// Admit one command by constructing and applying its exact durable fact.
pub(crate) fn admit(
    admitted: &Forest,
    command: &StreamCommand,
    batch: BatchId,
) -> Result<AdmittedCommand, AdmissionRefusal> {
    let fact = match command {
        StreamCommand::Create {
            path,
            content_type,
            expiry,
        } => {
            if admitted.resolve(path).is_some() {
                return Err(AdmissionRefusal::PathOccupied);
            }
            let uid = decide_successor_uid(admitted.path_count()).map_err(|contradiction| {
                contradiction
                    .admission_refusal()
                    .expect("successor allocation only returns a caller-facing capacity refusal")
            })?;
            OperationFact::StreamCreated {
                path: path.clone(),
                uid,
                content_type: content_type.clone(),
                expiry: *expiry,
            }
        }
        StreamCommand::Close { path } => OperationFact::StreamClosed {
            path: path.clone(),
            uid: resolve_live_uid(admitted, path)?,
        },
        StreamCommand::Delete { path } => OperationFact::StreamDeleted {
            path: path.clone(),
            uid: resolve_live_uid(admitted, path)?,
        },
    };

    let mut candidate = admitted.clone();
    match candidate.strict_fold(batch, &fact) {
        Ok(Applied) => Ok(AdmittedCommand {
            forest: candidate,
            reply: reply_for_fact(&fact),
            fact,
        }),
        Err(contradiction) => Err(contradiction
            .admission_refusal()
            .expect("admission constructs the dense uid and exact path uid carried by its fact")),
    }
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
            Self::StreamAlreadyClosed => Some(AdmissionRefusal::StreamAlreadyClosed),
            Self::UidNotDenseSuccessor | Self::PathUidMismatch => None,
        }
    }
}

fn resolve_live_uid(forest: &Forest, path: &DirectoryKey) -> Result<StreamUid, AdmissionRefusal> {
    match forest.resolve(path) {
        Some(DirectoryEntry::Live(uid)) => Ok(uid),
        Some(DirectoryEntry::Tombstone(_)) | None => Err(AdmissionRefusal::PathNotLive),
    }
}

const fn reply_for_fact(fact: &OperationFact) -> StreamReply {
    match fact {
        OperationFact::StreamCreated { uid, .. } => StreamReply::Created { uid: *uid },
        OperationFact::StreamClosed { .. } => StreamReply::Closed,
        OperationFact::StreamDeleted { .. } => StreamReply::Deleted,
    }
}

#[cfg(test)]
mod tests {
    use strom_domain::StreamContentType;

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
        let create = StreamCommand::Create {
            path: path.clone(),
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
        };
        let admitted = admit(&Forest::empty(), &create, batch)?;
        let mut forest = admitted.forest;
        let before = forest.clone();
        assert_eq!(
            Err(AdmissionRefusal::PathOccupied),
            admit(&forest, &create, batch)
        );
        assert_eq!(before.path_count(), forest.path_count());
        assert_eq!(before.resolve(&path), forest.resolve(&path));

        let close = StreamCommand::Close { path: path.clone() };
        forest = admit(&forest, &close, batch)?.forest;
        let record_before = forest.record(StreamUid::try_from(1)?).cloned();
        assert_eq!(
            Err(AdmissionRefusal::StreamAlreadyClosed),
            admit(&forest, &close, batch)
        );
        assert_eq!(
            record_before.as_ref(),
            forest.record(StreamUid::try_from(1)?)
        );

        let missing = DirectoryKey::try_from(Box::<[u8]>::from(b"events/missing".as_slice()))?;
        let before_count = forest.path_count();
        assert_eq!(
            Err(AdmissionRefusal::PathNotLive),
            admit(&forest, &StreamCommand::Delete { path: missing }, batch)
        );
        assert_eq!(before_count, forest.path_count());
        Ok(())
    }

    #[test]
    fn delete_preserves_path_occupancy_and_the_next_create_gets_the_dense_successor()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = DirectoryKey::try_from(Box::<[u8]>::from(b"events/a".as_slice()))?;
        let second = DirectoryKey::try_from(Box::<[u8]>::from(b"events/b".as_slice()))?;
        let batch = BatchId::try_from(1)?;
        let create = |path| StreamCommand::Create {
            path,
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
        };
        let forest = admit(&Forest::empty(), &create(first.clone()), batch)?.forest;
        let forest = admit(
            &forest,
            &StreamCommand::Delete {
                path: first.clone(),
            },
            batch,
        )?
        .forest;
        assert_eq!(
            Err(AdmissionRefusal::PathOccupied),
            admit(&forest, &create(first), batch)
        );
        let admitted = admit(&forest, &create(second), batch)?;
        assert_eq!(
            StreamReply::Created {
                uid: StreamUid::try_from(2)?
            },
            admitted.reply
        );
        Ok(())
    }
}
