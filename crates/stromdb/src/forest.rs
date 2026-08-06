//! Pure two-map forest: Directory and Ledger under strict fold.

mod directory;
mod ledger;

use std::num::NonZeroU64;

use directory::ResidentDirectory;
use ledger::ResidentLedger;
use strom_domain::StreamLifecycle;
use strom_storage_domain::{
    BatchId, DirectoryEntry, DirectoryKey, OperationFact, PARTITION_PATH_OCCUPANCIES_MAX_V2,
    StreamRecord, StreamUid,
};

/// Resident Directory and Ledger under the no-forks cross-store invariants.
#[derive(Debug)]
pub struct Forest {
    directory: ResidentDirectory,
    ledger: ResidentLedger,
}

/// Zero-sized witness that one fact applied exactly once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Applied;

/// A fact that cannot join this forest under strict fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FoldContradiction {
    /// Create: the path already has a Live or Tombstone row.
    #[error("path is already occupied")]
    PathOccupied,
    /// Create: lifetime path occupancies are at the V2 bound.
    #[error("partition path capacity is exhausted")]
    PathCapacityExhausted,
    /// Create: the fact uid is not `path_count + 1`.
    #[error("stream uid is not the dense successor")]
    UidNotDenseSuccessor,
    /// Close or delete: the row is absent or a Tombstone.
    #[error("path is not live")]
    PathNotLive,
    /// Close or delete: the Live uid differs from the fact.
    #[error("path uid does not match")]
    PathUidMismatch,
    /// Close: the ledger record is already Closed.
    #[error("stream is already closed")]
    StreamAlreadyClosed,
}

impl Forest {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            directory: ResidentDirectory::empty(),
            ledger: ResidentLedger::empty(),
        }
    }

    /// Apply one fact at `batch` under the strict fold rules.
    ///
    /// # Errors
    ///
    /// Returns the first contradiction. A rejected fold leaves state unchanged.
    ///
    /// # Panics
    ///
    /// Panics when a Live directory row has no ledger record. That breaks the
    /// cross-store invariant established by [`Forest::empty`] and preserved by
    /// every successful fold.
    pub fn strict_fold(
        &mut self,
        batch: BatchId,
        fact: &OperationFact,
    ) -> Result<Applied, FoldContradiction> {
        match fact {
            OperationFact::StreamCreated {
                path,
                uid,
                content_type,
                expiry,
            } => {
                if self.directory.get(path).is_some() {
                    return Err(FoldContradiction::PathOccupied);
                }
                let expected_uid = decide_successor_uid(self.path_count())?;
                if *uid != expected_uid {
                    return Err(FoldContradiction::UidNotDenseSuccessor);
                }

                self.directory.insert_live(path.clone(), *uid);
                self.ledger.insert(
                    *uid,
                    StreamRecord::new(content_type.clone(), *expiry, StreamLifecycle::Open, batch),
                );
                Ok(Applied)
            }
            OperationFact::StreamClosed { path, uid } => {
                let live_uid = require_live_uid(self.directory.get(path), *uid)?;
                let record = self
                    .ledger
                    .get(live_uid)
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
                self.ledger.replace(live_uid, closed);
                Ok(Applied)
            }
            OperationFact::StreamDeleted { path, uid } => {
                let live_uid = require_live_uid(self.directory.get(path), *uid)?;
                assert!(
                    self.ledger.get(live_uid).is_some(),
                    "a Live directory row has exactly one Ledger record"
                );
                self.directory.tombstone_live(path, live_uid);
                self.ledger.remove(live_uid);
                Ok(Applied)
            }
        }
    }

    #[must_use]
    pub fn resolve(&self, path: &DirectoryKey) -> Option<DirectoryEntry> {
        self.directory.get(path).copied()
    }

    #[must_use]
    pub fn record(&self, uid: StreamUid) -> Option<&StreamRecord> {
        self.ledger.get(uid)
    }

    /// # Panics
    ///
    /// Panics when the directory row count does not fit in `u64`.
    #[must_use]
    pub fn path_count(&self) -> u64 {
        u64::try_from(self.directory.len()).expect("directory row count fits in u64")
    }
}

fn require_live_uid(
    entry: Option<&DirectoryEntry>,
    uid: StreamUid,
) -> Result<StreamUid, FoldContradiction> {
    match entry {
        Some(DirectoryEntry::Live(live_uid)) => {
            if *live_uid == uid {
                Ok(*live_uid)
            } else {
                Err(FoldContradiction::PathUidMismatch)
            }
        }
        Some(DirectoryEntry::Tombstone(_)) | None => Err(FoldContradiction::PathNotLive),
    }
}

// One function owns both create-allocation gates so a fold cannot check the
// dense successor without first proving lifetime capacity (RFC 0003 gate order).
fn decide_successor_uid(path_count: u64) -> Result<StreamUid, FoldContradiction> {
    if path_count >= PARTITION_PATH_OCCUPANCIES_MAX_V2 {
        return Err(FoldContradiction::PathCapacityExhausted);
    }
    let successor = path_count
        .checked_add(1)
        .and_then(NonZeroU64::new)
        .expect("path_count below PARTITION_PATH_OCCUPANCIES_MAX_V2 has a nonzero successor");
    Ok(StreamUid::from(successor))
}

#[cfg(test)]
mod tests {
    use super::{FoldContradiction, decide_successor_uid};
    use strom_storage_domain::{PARTITION_PATH_OCCUPANCIES_MAX_V2, StreamUid};

    // Materializing ten million rows at the public boundary is deferred to the
    // RFC 0003 measurement slice; this covers the exact capacity boundary as a
    // cost-bounded representative (stromstyle §7).
    #[test]
    fn decide_successor_uid_allocates_the_last_slot_and_rejects_at_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        let last_slot = PARTITION_PATH_OCCUPANCIES_MAX_V2
            .checked_sub(1)
            .expect("PARTITION_PATH_OCCUPANCIES_MAX_V2 is greater than zero");
        assert_eq!(
            decide_successor_uid(last_slot),
            Ok(StreamUid::try_from(PARTITION_PATH_OCCUPANCIES_MAX_V2)?),
            "the final lifetime occupancy still allocates its dense successor"
        );
        assert_eq!(
            Err(FoldContradiction::PathCapacityExhausted),
            decide_successor_uid(PARTITION_PATH_OCCUPANCIES_MAX_V2),
            "a full partition refuses a new path before allocating a uid"
        );
        Ok(())
    }
}
