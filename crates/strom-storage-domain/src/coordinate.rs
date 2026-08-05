//! Shared nonzero-coordinate failures.

/// A nonzero storage coordinate cannot be incremented beyond `u64::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("storage coordinate is exhausted")]
pub struct CoordinateExhausted;

/// Zero is reserved and is not a durable coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("storage coordinate is zero")]
pub struct ZeroCoordinate;
