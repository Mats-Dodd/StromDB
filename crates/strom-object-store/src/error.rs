//! Failure vocabulary, shaped by the caller's decisions (stromstyle §3).

use std::fmt;

use crate::bytes::Checksum;
use crate::key::ObjectKey;

/// A failed adapter operation.
///
/// `Unresolved` outcomes of conditional creates are evidence, not errors, and
/// live in [`crate::CreateEvidence`]. Absence is `Ok(None)` on reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// Transport trouble; a bounded retry of the same idempotent request is legal.
    Retryable { detail: String },
    /// The backend refused the request definitively; retrying cannot help.
    Rejected { detail: String },
    /// Durable bytes violate the storage model. The caller fails closed.
    Contradiction(StoreContradiction),
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retryable { detail } => {
                write!(formatter, "retryable transport failure: {detail}")
            }
            Self::Rejected { detail } => {
                write!(formatter, "backend rejected the request: {detail}")
            }
            Self::Contradiction(contradiction) => {
                write!(formatter, "durable contradiction: {contradiction}")
            }
        }
    }
}

impl std::error::Error for StoreError {}

/// Durable state that contradicts the storage model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreContradiction {
    /// The object at a valid key is larger than the caller's named bound.
    OversizedObject {
        key: ObjectKey,
        bytes_max: u64,
        bytes_actual: u64,
    },
    /// Authenticated range bytes did not match their expected checksum.
    ChecksumMismatch {
        key: ObjectKey,
        expected: Checksum,
        actual: Checksum,
    },
    /// A listing surfaced a noncanonical key under an owned prefix.
    ForeignKey { listed: String, detail: String },
    /// The backend returned keys out of ascending lexicographic order.
    UnorderedList {
        previous: ObjectKey,
        listed: ObjectKey,
    },
    /// The range read returned a different byte count than requested.
    ShortRange {
        key: ObjectKey,
        bytes_expected: u64,
        bytes_actual: u64,
    },
}

impl fmt::Display for StoreContradiction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OversizedObject {
                key,
                bytes_max,
                bytes_actual,
            } => {
                write!(
                    formatter,
                    "object {key} is {bytes_actual} bytes; the bound is {bytes_max}"
                )
            }
            Self::ChecksumMismatch {
                key,
                expected,
                actual,
            } => {
                write!(
                    formatter,
                    "object {key} range checksum is {actual}; expected {expected}"
                )
            }
            Self::ForeignKey { listed, detail } => {
                write!(
                    formatter,
                    "listed key {listed:?} is not canonical: {detail}"
                )
            }
            Self::UnorderedList { previous, listed } => {
                write!(formatter, "listing returned {listed} after {previous}")
            }
            Self::ShortRange {
                key,
                bytes_expected,
                bytes_actual,
            } => {
                write!(
                    formatter,
                    "object {key} range returned {bytes_actual} bytes; expected {bytes_expected}"
                )
            }
        }
    }
}

impl std::error::Error for StoreContradiction {}

/// The S3 client could not be constructed from the given configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ConfigError {
    pub detail: String,
}

impl fmt::Display for S3ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid S3 configuration: {}", self.detail)
    }
}

impl std::error::Error for S3ConfigError {}
