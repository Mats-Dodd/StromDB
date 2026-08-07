//! Pure two-map forest: Directory and Ledger under strict fold.

use std::num::NonZeroU64;

use imbl::OrdMap;
use imbl::ordmap::DiffItem;
use strom_domain::{StreamLifecycle, StreamPath};
use strom_storage_domain::{
    BatchId, DirectoryEntry, LedgerCell, OperationFact, PARTITION_PATH_OCCUPANCIES_MAX_V2,
    StreamRecord, StreamUid,
};

/// Resident Directory and Ledger under the no-forks cross-store invariants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Forest {
    directory: OrdMap<StreamPath, DirectoryEntry>,
    ledger: OrdMap<StreamUid, StreamRecord>,
}

/// Directory and Ledger rows that transform `base` into `self`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForestDelta {
    pub directory: Vec<(StreamPath, DirectoryEntry)>,
    pub ledger: Vec<(StreamUid, LedgerCell)>,
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

/// Merged Directory and Ledger rows that cannot form a complete forest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ForestContradiction {
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
    #[must_use]
    pub fn empty() -> Self {
        Self {
            directory: OrdMap::new(),
            ledger: OrdMap::new(),
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
                lifecycle,
            } => {
                if self.directory.get(path).is_some() {
                    return Err(FoldContradiction::PathOccupied);
                }
                let expected_uid = self.successor_uid()?;
                if *uid != expected_uid {
                    return Err(FoldContradiction::UidNotDenseSuccessor);
                }

                let previous = self
                    .directory
                    .insert(path.clone(), DirectoryEntry::Live(*uid));
                assert!(
                    previous.is_none(),
                    "create folds only into an absent directory path"
                );
                let previous = self.ledger.insert(
                    *uid,
                    StreamRecord::new(content_type.clone(), *expiry, *lifecycle, batch),
                );
                assert!(previous.is_none(), "create folds a fresh ledger uid");
                Ok(Applied)
            }
            OperationFact::StreamClosed { path, uid } => {
                let live_uid = require_live_uid(self.directory.get(path), *uid)?;
                let record = self
                    .ledger
                    .get(&live_uid)
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
                let previous = self.ledger.insert(live_uid, closed);
                assert!(previous.is_some(), "close folds an existing ledger record");
                Ok(Applied)
            }
            OperationFact::StreamDeleted { path, uid } => {
                let live_uid = require_live_uid(self.directory.get(path), *uid)?;
                assert!(
                    self.ledger.get(&live_uid).is_some(),
                    "a Live directory row has exactly one Ledger record"
                );
                let entry = self
                    .directory
                    .get_mut(path)
                    .expect("delete folds a Live directory row");
                assert!(
                    matches!(entry, DirectoryEntry::Live(present) if *present == live_uid),
                    "delete folds the Live uid named by the fact"
                );
                *entry = DirectoryEntry::Tombstone(live_uid);
                let previous = self.ledger.remove(&live_uid);
                assert!(
                    previous.is_some(),
                    "delete removes an existing ledger record"
                );
                Ok(Applied)
            }
        }
    }

    /// Rows that transform `base` into this forest.
    ///
    /// Directory path occupancy is permanent: a remove in the `imbl` diff is an
    /// invariant failure, not a delete cell.
    ///
    /// # Panics
    ///
    /// Panics when the current forest removed a permanent Directory occupancy.
    #[must_use]
    pub fn delta_since(&self, base: &Self) -> ForestDelta {
        let directory = base
            .directory
            .diff(&self.directory)
            .filter_map(|difference| match difference {
                DiffItem::Add(key, entry)
                | DiffItem::Update {
                    old: _,
                    new: (key, entry),
                } => Some((key.clone(), *entry)),
                DiffItem::Remove(key, _entry) => {
                    assert!(
                        self.directory.contains_key(key),
                        "Directory path occupancy is permanent"
                    );
                    None
                }
            })
            .collect();
        let ledger = base
            .ledger
            .diff(&self.ledger)
            .map(|difference| match difference {
                DiffItem::Add(uid, record)
                | DiffItem::Update {
                    old: _,
                    new: (uid, record),
                } => (*uid, LedgerCell::Value(record.clone())),
                DiffItem::Remove(uid, _record) => (*uid, LedgerCell::Delete),
            })
            .collect();
        ForestDelta { directory, ledger }
    }

    /// All resident rows as cells for a full checkpoint base.
    ///
    /// A full base carries every final Directory entry and every resident
    /// Ledger value. It carries no Ledger delete cells.
    #[must_use]
    pub fn checkpoint_cells(&self) -> ForestDelta {
        let directory = self
            .directory
            .iter()
            .map(|(key, entry)| (key.clone(), *entry))
            .collect();
        let ledger = self
            .ledger
            .iter()
            .map(|(uid, record)| (*uid, LedgerCell::Value(record.clone())))
            .collect();
        ForestDelta { directory, ledger }
    }

    #[must_use]
    pub fn resolve(&self, path: &StreamPath) -> Option<DirectoryEntry> {
        self.directory.get(path).copied()
    }

    #[must_use]
    pub fn record(&self, uid: StreamUid) -> Option<&StreamRecord> {
        self.ledger.get(&uid)
    }

    /// # Panics
    ///
    /// Panics when the directory row count does not fit in `u64`.
    #[must_use]
    pub(crate) fn path_count(&self) -> u64 {
        u64::try_from(self.directory.len()).expect("directory row count fits in u64")
    }

    /// Allocate the next dense stream identity after proving path capacity.
    ///
    /// # Errors
    ///
    /// Returns [`FoldContradiction::PathCapacityExhausted`] at the path bound.
    pub(crate) fn successor_uid(&self) -> Result<StreamUid, FoldContradiction> {
        Self::successor_uid_after(self.path_count())
    }

    fn successor_uid_after(path_count: u64) -> Result<StreamUid, FoldContradiction> {
        if path_count >= PARTITION_PATH_OCCUPANCIES_MAX_V2 {
            return Err(FoldContradiction::PathCapacityExhausted);
        }
        let successor = path_count
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .expect("path_count below the V2 occupancy bound has a nonzero successor");
        Ok(StreamUid::from(successor))
    }

    #[must_use]
    pub(crate) fn shares_roots_with(&self, other: &Self) -> bool {
        self.directory.ptr_eq(&other.directory) && self.ledger.ptr_eq(&other.ledger)
    }
}

impl
    TryFrom<(
        OrdMap<StreamPath, DirectoryEntry>,
        OrdMap<StreamUid, StreamRecord>,
    )> for Forest
{
    type Error = ForestContradiction;

    fn try_from(
        (directory, ledger): (
            OrdMap<StreamPath, DirectoryEntry>,
            OrdMap<StreamUid, StreamRecord>,
        ),
    ) -> Result<Self, Self::Error> {
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

        Ok(Self { directory, ledger })
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
    fn successor_allocation_accepts_the_last_slot_and_refuses_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        let last_slot = PARTITION_PATH_OCCUPANCIES_MAX_V2
            .checked_sub(1)
            .expect("the V2 occupancy bound is nonzero");
        assert_eq!(
            Ok(StreamUid::try_from(PARTITION_PATH_OCCUPANCIES_MAX_V2)?),
            Forest::successor_uid_after(last_slot),
            "the final lifetime occupancy has one dense successor"
        );
        assert_eq!(
            Err(FoldContradiction::PathCapacityExhausted),
            Forest::successor_uid_after(PARTITION_PATH_OCCUPANCIES_MAX_V2),
            "capacity is refused before successor arithmetic"
        );
        Ok(())
    }

    #[test]
    fn merged_constructor_enumerates_every_cross_store_contradiction()
    -> Result<(), Box<dyn std::error::Error>> {
        let path_a = stream_path("events/a")?;
        let path_b = stream_path("events/b")?;
        let path_c = stream_path("events/c")?;
        let uid_1 = StreamUid::try_from(1)?;
        let uid_2 = StreamUid::try_from(2)?;
        let uid_3 = StreamUid::try_from(3)?;

        let count_mismatch = directory([(path_a.clone(), DirectoryEntry::Live(uid_2))]);
        assert_eq!(
            Err(ForestContradiction::CountMismatch),
            Forest::try_from((count_mismatch, OrdMap::new()))
        );

        let uid_gap = directory([
            (path_a.clone(), DirectoryEntry::Live(uid_1)),
            (path_b.clone(), DirectoryEntry::Live(uid_1)),
            (path_c, DirectoryEntry::Live(uid_3)),
        ]);
        assert_eq!(
            Err(ForestContradiction::UidGap),
            Forest::try_from((uid_gap, OrdMap::new()))
        );

        let live_without_record = directory([(path_a.clone(), DirectoryEntry::Live(uid_1))]);
        assert_eq!(
            Err(ForestContradiction::LiveWithoutRecord),
            Forest::try_from((live_without_record, OrdMap::new()))
        );

        let tombstone_with_record = directory([(path_a.clone(), DirectoryEntry::Tombstone(uid_1))]);
        assert_eq!(
            Err(ForestContradiction::TombstoneWithRecord),
            Forest::try_from((tombstone_with_record, ledger([(uid_1, record()?)]),))
        );

        let record_without_live = directory([(path_a, DirectoryEntry::Live(uid_1))]);
        assert_eq!(
            Err(ForestContradiction::RecordWithoutLive),
            Forest::try_from((
                record_without_live,
                ledger([(uid_1, record()?), (uid_2, record()?)]),
            ))
        );
        Ok(())
    }

    #[test]
    fn merged_constructor_accepts_exact_live_and_tombstone_correspondence()
    -> Result<(), Box<dyn std::error::Error>> {
        let path_live = stream_path("events/live")?;
        let path_deleted = stream_path("events/deleted")?;
        let uid_live = StreamUid::try_from(1)?;
        let uid_deleted = StreamUid::try_from(2)?;
        let forest = Forest::try_from((
            directory([
                (path_deleted.clone(), DirectoryEntry::Tombstone(uid_deleted)),
                (path_live.clone(), DirectoryEntry::Live(uid_live)),
            ]),
            ledger([(uid_live, record()?)]),
        ))?;
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

    fn stream_path(raw: &str) -> Result<StreamPath, Box<dyn std::error::Error>> {
        Ok(raw.parse()?)
    }

    fn directory<const ROWS: usize>(
        rows: [(StreamPath, DirectoryEntry); ROWS],
    ) -> OrdMap<StreamPath, DirectoryEntry> {
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
