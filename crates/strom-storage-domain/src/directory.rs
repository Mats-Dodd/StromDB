//! Directory-store entries.

use crate::StreamUid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
pub enum DirectoryEntry {
    Live(StreamUid),
    Tombstone(StreamUid),
}

impl From<&ArchivedDirectoryEntry> for DirectoryEntry {
    fn from(entry: &ArchivedDirectoryEntry) -> Self {
        match entry {
            ArchivedDirectoryEntry::Live(uid) => Self::Live(StreamUid::from(uid)),
            ArchivedDirectoryEntry::Tombstone(uid) => Self::Tombstone(StreamUid::from(uid)),
        }
    }
}
