//! Shared nonzero-coordinate failures.

use std::fmt;

/// A nonzero storage coordinate cannot be incremented beyond `u64::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinateExhausted;

impl fmt::Display for CoordinateExhausted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("storage coordinate is exhausted")
    }
}

impl std::error::Error for CoordinateExhausted {}

/// Zero is reserved and is not a durable coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZeroCoordinate;

impl fmt::Display for ZeroCoordinate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("storage coordinate is zero")
    }
}

impl std::error::Error for ZeroCoordinate {}
