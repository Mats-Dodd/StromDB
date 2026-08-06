//! Logical Ledger row values.

use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};

use crate::archive::{
    ContentTypeAsString, DecodeError, ExpiryAsArchive, LifecycleAsArchive, decode_content_type,
};
use crate::{BatchId, StreamUid};

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

#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
pub struct StreamRecord {
    #[rkyv(with = ContentTypeAsString)]
    content_type: StreamContentType,
    #[rkyv(with = ExpiryAsArchive)]
    expiry: ExpiryPolicy,
    #[rkyv(with = LifecycleAsArchive)]
    lifecycle: StreamLifecycle,
    created_at: BatchId,
}

const _: () = assert!(
    size_of::<StreamRecord>() == 56,
    "a resident stream record has no repeated StreamUid"
);

impl StreamRecord {
    #[must_use]
    pub const fn new(
        content_type: StreamContentType,
        expiry: ExpiryPolicy,
        lifecycle: StreamLifecycle,
        created_at: BatchId,
    ) -> Self {
        Self {
            content_type,
            expiry,
            lifecycle,
            created_at,
        }
    }

    #[must_use]
    pub const fn content_type(&self) -> &StreamContentType {
        &self.content_type
    }

    #[must_use]
    pub const fn expiry(&self) -> ExpiryPolicy {
        self.expiry
    }

    #[must_use]
    pub const fn lifecycle(&self) -> StreamLifecycle {
        self.lifecycle
    }

    #[must_use]
    pub const fn created_at(&self) -> BatchId {
        self.created_at
    }
}

impl TryFrom<&ArchivedStreamRecord> for StreamRecord {
    type Error = DecodeError;

    fn try_from(record: &ArchivedStreamRecord) -> Result<Self, Self::Error> {
        Ok(Self::new(
            decode_content_type(&record.content_type)?,
            ExpiryPolicy::try_from(&record.expiry)?,
            StreamLifecycle::from(&record.lifecycle),
            BatchId::from(&record.created_at),
        ))
    }
}

impl ArchivedStreamRecord {
    pub(crate) fn validated(&self) -> Result<&Self, DecodeError> {
        StreamContentType::validate_canonical(self.content_type.as_str())
            .map_err(|_domain_error| DecodeError::InvalidBody)?;
        ExpiryPolicy::try_from(&self.expiry)?;
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
#[rkyv(bytecheck(bounds(__C: rkyv::validation::ArchiveContext)))]
pub enum LedgerCell {
    Value(StreamRecord),
    Delete,
}

impl TryFrom<&ArchivedLedgerCell> for LedgerCell {
    type Error = DecodeError;

    fn try_from(cell: &ArchivedLedgerCell) -> Result<Self, Self::Error> {
        match cell {
            ArchivedLedgerCell::Value(record) => StreamRecord::try_from(record).map(Self::Value),
            ArchivedLedgerCell::Delete => Ok(Self::Delete),
        }
    }
}

impl ArchivedLedgerCell {
    pub(crate) const fn is_value(&self) -> bool {
        matches!(self, Self::Value(_))
    }

    pub(crate) fn validated(&self) -> Result<&Self, DecodeError> {
        if let Self::Value(record) = self {
            record.validated()?;
        }
        Ok(self)
    }
}
