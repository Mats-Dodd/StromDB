//! Pure two-map forest: Directory and Ledger under strict fold.

mod directory;
mod ledger;

use directory::ResidentDirectory;
use imbl::OrdMap;
use ledger::ResidentLedger;
use strom_domain::StreamLifecycle;
use strom_storage_domain::{
    BatchId, DirectoryEntry, DirectoryKey, OperationFact, PARTITION_PATH_OCCUPANCIES_MAX_V2,
    StreamRecord, StreamUid,
};

use crate::admission::decide_successor_uid;

/// Resident Directory and Ledger under the no-forks cross-store invariants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Forest {
    directory: ResidentDirectory,
    ledger: ResidentLedger,
}

/// Zero-sized witness that one fact applied exactly once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Applied;

/// A fact that cannot join this forest under strict fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum FoldContradiction {
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

/// Merged Directory and Ledger rows that cannot form a complete forest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ForestContradiction {
    #[error("Directory stream uids are not dense and unique")]
    UidGap,
    #[error("Directory row count differs from the maximum allocated stream uid")]
    CountMismatch,
    #[error("a Live Directory row has no Ledger record")]
    LiveWithoutRecord,
    #[error("a Ledger record has no Live Directory row")]
    RecordWithoutLive,
    #[error("a Tombstone Directory row still has a Ledger record")]
    TombstoneWithRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UidState {
    LiveMissingRecord,
    LiveWithRecord,
    Tombstone,
}

impl Forest {
    #[cfg(test)]
    #[must_use]
    pub(crate) fn empty() -> Self {
        Self {
            directory: ResidentDirectory::empty(),
            ledger: ResidentLedger::empty(),
        }
    }

    /// Construct one all-or-nothing resident forest from newest-wins rows.
    ///
    /// # Errors
    ///
    /// Returns the first cross-store contradiction without exposing a partial
    /// forest.
    pub(crate) fn try_from_merged(
        directory: OrdMap<DirectoryKey, DirectoryEntry>,
        ledger: OrdMap<StreamUid, StreamRecord>,
    ) -> Result<Self, ForestContradiction> {
        let path_count = u64::try_from(directory.len())
            .map_err(|_overflow| ForestContradiction::CountMismatch)?;
        if path_count > PARTITION_PATH_OCCUPANCIES_MAX_V2 {
            return Err(ForestContradiction::CountMismatch);
        }

        let path_count_usize =
            usize::try_from(path_count).map_err(|_overflow| ForestContradiction::CountMismatch)?;
        let mut uid_states = vec![None; path_count_usize];
        for entry in directory.values() {
            let index = stream_uid_value(directory_entry_uid(entry))
                .checked_sub(1)
                .and_then(|zero_based| usize::try_from(zero_based).ok())
                .ok_or(ForestContradiction::UidGap)?;
            let Some(slot) = uid_states.get_mut(index) else {
                return Err(ForestContradiction::CountMismatch);
            };
            if slot.is_some() {
                return Err(ForestContradiction::UidGap);
            }
            *slot = Some(match entry {
                DirectoryEntry::Live(_) => UidState::LiveMissingRecord,
                DirectoryEntry::Tombstone(_) => UidState::Tombstone,
            });
        }
        if uid_states.iter().any(Option::is_none) {
            return Err(ForestContradiction::UidGap);
        }

        for uid in ledger.keys() {
            let index = stream_uid_value(*uid)
                .checked_sub(1)
                .and_then(|zero_based| usize::try_from(zero_based).ok())
                .ok_or(ForestContradiction::RecordWithoutLive)?;
            let Some(state) = uid_states.get_mut(index).and_then(Option::as_mut) else {
                return Err(ForestContradiction::RecordWithoutLive);
            };
            match state {
                UidState::LiveMissingRecord => *state = UidState::LiveWithRecord,
                UidState::Tombstone => return Err(ForestContradiction::TombstoneWithRecord),
                UidState::LiveWithRecord => {
                    return Err(ForestContradiction::RecordWithoutLive);
                }
            }
        }
        if uid_states.contains(&Some(UidState::LiveMissingRecord)) {
            return Err(ForestContradiction::LiveWithoutRecord);
        }

        Ok(Self {
            directory: ResidentDirectory::from_rows(directory),
            ledger: ResidentLedger::from_records(ledger),
        })
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
    pub(crate) fn strict_fold(
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
                lifecycle,
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
                    StreamRecord::new(content_type.clone(), *expiry, *lifecycle, batch),
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
    pub(crate) fn resolve(&self, path: &DirectoryKey) -> Option<DirectoryEntry> {
        self.directory.get(path).copied()
    }

    #[must_use]
    pub(crate) fn record(&self, uid: StreamUid) -> Option<&StreamRecord> {
        self.ledger.get(uid)
    }

    /// # Panics
    ///
    /// Panics when the directory row count does not fit in `u64`.
    #[must_use]
    pub(crate) fn path_count(&self) -> u64 {
        u64::try_from(self.directory.len()).expect("directory row count fits in u64")
    }

    pub(crate) const fn directory_rows(&self) -> &OrdMap<DirectoryKey, DirectoryEntry> {
        self.directory.rows()
    }

    pub(crate) const fn ledger_rows(&self) -> &OrdMap<StreamUid, StreamRecord> {
        self.ledger.rows()
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

const fn directory_entry_uid(entry: &DirectoryEntry) -> StreamUid {
    match entry {
        DirectoryEntry::Live(uid) | DirectoryEntry::Tombstone(uid) => *uid,
    }
}

const fn stream_uid_value(uid: StreamUid) -> u64 {
    uid.get()
}

#[cfg(test)]
mod tests {
    use imbl::OrdMap;
    use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};

    use super::*;

    #[test]
    fn merged_constructor_enumerates_every_cross_store_contradiction()
    -> Result<(), Box<dyn std::error::Error>> {
        let path_a = directory_key("events/a")?;
        let path_b = directory_key("events/b")?;
        let path_c = directory_key("events/c")?;
        let uid_1 = StreamUid::try_from(1)?;
        let uid_2 = StreamUid::try_from(2)?;
        let uid_3 = StreamUid::try_from(3)?;

        let count_mismatch = directory([(path_a.clone(), DirectoryEntry::Live(uid_2))]);
        assert_eq!(
            Err(ForestContradiction::CountMismatch),
            Forest::try_from_merged(count_mismatch, OrdMap::new())
        );

        let uid_gap = directory([
            (path_a.clone(), DirectoryEntry::Live(uid_1)),
            (path_b.clone(), DirectoryEntry::Live(uid_1)),
            (path_c, DirectoryEntry::Live(uid_3)),
        ]);
        assert_eq!(
            Err(ForestContradiction::UidGap),
            Forest::try_from_merged(uid_gap, OrdMap::new())
        );

        let live_without_record = directory([(path_a.clone(), DirectoryEntry::Live(uid_1))]);
        assert_eq!(
            Err(ForestContradiction::LiveWithoutRecord),
            Forest::try_from_merged(live_without_record, OrdMap::new())
        );

        let tombstone_with_record = directory([(path_a.clone(), DirectoryEntry::Tombstone(uid_1))]);
        assert_eq!(
            Err(ForestContradiction::TombstoneWithRecord),
            Forest::try_from_merged(tombstone_with_record, ledger([(uid_1, record()?)]))
        );

        let record_without_live = directory([(path_a, DirectoryEntry::Live(uid_1))]);
        assert_eq!(
            Err(ForestContradiction::RecordWithoutLive),
            Forest::try_from_merged(
                record_without_live,
                ledger([(uid_1, record()?), (uid_2, record()?)]),
            )
        );
        Ok(())
    }

    #[test]
    fn merged_constructor_accepts_exact_live_and_tombstone_correspondence()
    -> Result<(), Box<dyn std::error::Error>> {
        let path_live = directory_key("events/live")?;
        let path_deleted = directory_key("events/deleted")?;
        let uid_live = StreamUid::try_from(1)?;
        let uid_deleted = StreamUid::try_from(2)?;
        let forest = Forest::try_from_merged(
            directory([
                (path_deleted.clone(), DirectoryEntry::Tombstone(uid_deleted)),
                (path_live.clone(), DirectoryEntry::Live(uid_live)),
            ]),
            ledger([(uid_live, record()?)]),
        )?;
        assert_eq!(2, forest.path_count());
        assert_eq!(
            Some(DirectoryEntry::Live(uid_live)),
            forest.resolve(&path_live)
        );
        assert_eq!(
            Some(DirectoryEntry::Tombstone(uid_deleted)),
            forest.resolve(&path_deleted)
        );
        assert!(forest.record(uid_live).is_some());
        assert!(forest.record(uid_deleted).is_none());
        Ok(())
    }

    fn directory_key(raw: &str) -> Result<DirectoryKey, Box<dyn std::error::Error>> {
        Ok(DirectoryKey::try_from(Box::<[u8]>::from(raw.as_bytes()))?)
    }

    fn directory<const ROWS: usize>(
        rows: [(DirectoryKey, DirectoryEntry); ROWS],
    ) -> OrdMap<DirectoryKey, DirectoryEntry> {
        rows.into_iter().collect()
    }

    fn ledger<const ROWS: usize>(
        rows: [(StreamUid, StreamRecord); ROWS],
    ) -> OrdMap<StreamUid, StreamRecord> {
        rows.into_iter().collect()
    }

    fn record() -> Result<StreamRecord, Box<dyn std::error::Error>> {
        Ok(StreamRecord::new(
            StreamContentType::octet_stream(),
            ExpiryPolicy::None,
            StreamLifecycle::Open,
            BatchId::try_from(1)?,
        ))
    }
}

#[cfg(test)]
mod behavior_tests;
