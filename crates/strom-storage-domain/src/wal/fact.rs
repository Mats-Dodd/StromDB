//! Already-decided stream mutation facts.

use serde::Serialize;
use strom_domain::{ExpiryPolicy, StreamContentType};

use crate::{LedgerKey, StreamUid};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum OperationFact {
    StreamCreated {
        path: LedgerKey,
        uid: StreamUid,
        content_type: StreamContentType,
        expiry: ExpiryPolicy,
    },
    StreamClosed {
        path: LedgerKey,
        uid: StreamUid,
    },
    StreamDeleted {
        path: LedgerKey,
        uid: StreamUid,
    },
}
