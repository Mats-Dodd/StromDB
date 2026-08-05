//! Failure vocabulary, shaped by the caller's decisions (stromstyle §3).

use crate::key::ObjectKey;

/// A failed adapter operation.
///
/// `Unresolved` outcomes of conditional creates are evidence, not errors, and
/// live in [`crate::CreateEvidence`]. Absence is `Ok(None)` on reads.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    /// Transport trouble; a bounded retry of the same idempotent request is legal.
    #[error("retryable transport failure: {detail}")]
    Retryable { detail: String },
    /// The backend refused the request definitively; retrying cannot help.
    #[error("backend rejected the request: {detail}")]
    Rejected { detail: String },
    /// Durable bytes violate the storage model. The caller fails closed.
    #[error("durable contradiction: {0}")]
    Contradiction(#[source] StoreContradiction),
}

/// Durable state that contradicts the storage model.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreContradiction {
    /// The object at a valid key is larger than the caller's named bound.
    #[error("object {key} is {bytes_actual} bytes; the bound is {bytes_max}")]
    OversizedObject {
        key: ObjectKey,
        bytes_max: u64,
        bytes_actual: u64,
    },
    /// A listing surfaced a noncanonical key under an owned prefix.
    #[error("listed key {listed:?} is not canonical: {detail}")]
    ForeignKey { listed: String, detail: String },
    /// The backend returned keys out of ascending lexicographic order.
    #[error("listing returned {listed} after {previous}")]
    UnorderedList {
        previous: ObjectKey,
        listed: ObjectKey,
    },
}

/// The S3 client could not be constructed from the given configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid S3 configuration: {detail}")]
pub struct S3ConfigError {
    pub detail: String,
}
