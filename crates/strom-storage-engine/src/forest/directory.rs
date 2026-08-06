//! Resident Directory: path occupancy for the pure forest.

use imbl::OrdMap;
use strom_storage_domain::{DirectoryEntry, DirectoryKey, StreamUid};

/// In-memory Directory rows for strict fold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResidentDirectory {
    rows: OrdMap<DirectoryKey, DirectoryEntry>,
}

impl ResidentDirectory {
    #[cfg(test)]
    #[must_use]
    pub(super) fn empty() -> Self {
        Self {
            rows: OrdMap::new(),
        }
    }

    #[must_use]
    pub(super) const fn from_rows(rows: OrdMap<DirectoryKey, DirectoryEntry>) -> Self {
        Self { rows }
    }

    #[must_use]
    pub(super) fn get(&self, path: &DirectoryKey) -> Option<&DirectoryEntry> {
        self.rows.get(path)
    }

    #[must_use]
    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }

    #[must_use]
    pub(super) const fn rows(&self) -> &OrdMap<DirectoryKey, DirectoryEntry> {
        &self.rows
    }

    pub(super) fn insert_live(&mut self, path: DirectoryKey, uid: StreamUid) {
        let previous = self.rows.insert(path, DirectoryEntry::Live(uid));
        assert!(
            previous.is_none(),
            "create folds only into an absent directory path"
        );
    }

    pub(super) fn tombstone_live(&mut self, path: &DirectoryKey, uid: StreamUid) {
        let entry = self
            .rows
            .get_mut(path)
            .expect("delete folds a Live directory row");
        assert!(
            matches!(entry, DirectoryEntry::Live(live_uid) if *live_uid == uid),
            "delete folds the Live uid named by the fact"
        );
        *entry = DirectoryEntry::Tombstone(uid);
    }
}
