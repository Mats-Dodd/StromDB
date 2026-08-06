//! Resident Directory: path occupancy for the pure forest.

use std::collections::BTreeMap;

use strom_storage_domain::{DirectoryEntry, DirectoryKey, StreamUid};

/// In-memory Directory rows for strict fold.
#[derive(Debug)]
pub(super) struct ResidentDirectory {
    rows: BTreeMap<DirectoryKey, DirectoryEntry>,
}

impl ResidentDirectory {
    #[must_use]
    pub(super) const fn empty() -> Self {
        Self {
            rows: BTreeMap::new(),
        }
    }

    #[must_use]
    pub(super) fn get(&self, path: &DirectoryKey) -> Option<&DirectoryEntry> {
        self.rows.get(path)
    }

    #[must_use]
    pub(super) fn len(&self) -> usize {
        self.rows.len()
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
            matches!(*entry, DirectoryEntry::Live(live_uid) if live_uid == uid),
            "delete folds the Live uid named by the fact"
        );
        *entry = DirectoryEntry::Tombstone(uid);
    }
}
