//! Pure domain types for the Durable Streams protocol's cold stream metadata.
//!
//! This crate models the low-churn facts the Ledger stores per stream: its
//! identity, content type, expiry configuration, and lifecycle. None of these
//! change on a read or an append. Every type parses at the boundary through
//! `FromStr` or `TryFrom` and is trusted inward. The crate is pure: no I/O,
//! no clock.
//!
//! Every type implements [`serde::Serialize`], and each impl documents its
//! durable spelling. `Deserialize` is deliberately absent: untrusted bytes
//! must re-enter through the canonical parsers, so no decoder can skip an
//! invariant. The `proptest` feature adds a `strategy` module that generates
//! valid values through those same parsers.
//!
//! Protocol section references (§) cite `docs/protocol.md`.
//!
//! ```
//! use strom_domain::{ExpiryPolicy, StreamId, StreamTtl};
//!
//! let stream_id: StreamId = "events/abc".parse()?;
//! let ttl: StreamTtl = "3600".parse()?;
//! let policy = ExpiryPolicy::try_from((Some(ttl), None))?;
//! assert!(matches!(policy, ExpiryPolicy::SlidingTtl(_)));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod content_type;
mod expiry;
mod lifecycle;
#[cfg(feature = "proptest")]
pub mod strategy;
mod stream_id;

pub use content_type::{CONTENT_TYPE_BYTES_MAX, ContentTypeError, StreamContentType};
pub use expiry::{
    ExpiresAt, ExpiresAtError, ExpiresAtRangeError, ExpiryPolicy, ExpiryPolicyConflict, StreamTtl,
    StreamTtlError,
};
pub use lifecycle::StreamLifecycle;
pub use stream_id::{STREAM_ID_BYTES_MAX, StreamId, StreamIdError};
