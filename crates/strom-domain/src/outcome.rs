//! Protocol-visible stream operation outcomes.

use crate::{ExpiryPolicy, StreamContentType, StreamLifecycle};

/// Whether create found the stream already durable at the same configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateOutcome {
    Created,
    AlreadyExists,
}

/// Whether close found the stream already closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseStreamOutcome {
    Closed,
    AlreadyClosed,
}

/// The current protocol-visible state of one stream path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamStatus {
    /// No stream has ever occupied this path (protocol `404`).
    Missing,
    /// The stream was deleted; the path stays occupied (protocol `410`).
    Deleted,
    /// The stream exists and is directly readable.
    Live {
        content_type: StreamContentType,
        expiry: ExpiryPolicy,
        lifecycle: StreamLifecycle,
    },
}
