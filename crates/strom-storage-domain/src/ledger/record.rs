//! Logical Ledger row values.

use serde::Serialize;
use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};

use crate::{BatchId, StreamUid};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectoryEntry {
    Live(StreamUid),
    Tombstone(StreamUid),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamRecord {
    content_type: StreamContentType,
    expiry: ExpiryPolicy,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerCell {
    Value(StreamRecord),
    Delete,
}
