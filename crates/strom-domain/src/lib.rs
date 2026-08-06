//! Durable Streams protocol vocabulary and caller-visible outcomes.
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
mod outcome;
#[cfg(feature = "proptest")]
pub mod strategy;
mod stream_id;

pub use content_type::{CONTENT_TYPE_BYTES_MAX, ContentTypeError, StreamContentType};
pub use expiry::{
    ExpiresAt, ExpiresAtError, ExpiresAtRangeError, ExpiryPolicy, ExpiryPolicyConflict, StreamTtl,
    StreamTtlError,
};
pub use lifecycle::StreamLifecycle;
pub use outcome::{CloseStreamOutcome, CreateOutcome, StreamStatus};
pub use stream_id::{STREAM_ID_BYTES_MAX, StreamId, StreamIdError};
