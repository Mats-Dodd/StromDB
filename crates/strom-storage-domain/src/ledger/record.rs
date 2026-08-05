//! Logical Ledger row values.

use serde::Serialize;
use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};

use crate::{BatchId, StreamUid};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum LedgerRecord {
    Live(StreamRecord),
    Tombstone(PathTombstone),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamRecord {
    uid: StreamUid,
    content_type: StreamContentType,
    expiry: ExpiryPolicy,
    lifecycle: StreamLifecycle,
    created_at: BatchId,
}

impl StreamRecord {
    #[must_use]
    pub const fn new(
        uid: StreamUid,
        content_type: StreamContentType,
        expiry: ExpiryPolicy,
        lifecycle: StreamLifecycle,
        created_at: BatchId,
    ) -> Self {
        Self {
            uid,
            content_type,
            expiry,
            lifecycle,
            created_at,
        }
    }

    #[must_use]
    pub const fn uid(&self) -> StreamUid {
        self.uid
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PathTombstone {
    uid: StreamUid,
}

impl PathTombstone {
    #[must_use]
    pub const fn new(uid: StreamUid) -> Self {
        Self { uid }
    }

    #[must_use]
    pub const fn uid(self) -> StreamUid {
        self.uid
    }
}
