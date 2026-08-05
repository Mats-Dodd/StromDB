//! Already-decided stream mutation facts.

use serde::Serialize;
use strom_domain::{ExpiryPolicy, StreamContentType};

use crate::{DirectoryKey, StreamUid};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum OperationFact {
    StreamCreated {
        path: DirectoryKey,
        uid: StreamUid,
        content_type: StreamContentType,
        expiry: ExpiryPolicy,
    },
    StreamClosed {
        path: DirectoryKey,
        uid: StreamUid,
    },
    StreamDeleted {
        path: DirectoryKey,
        uid: StreamUid,
    },
}
