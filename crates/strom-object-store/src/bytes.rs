//! Byte-plane vocabulary: frozen candidate bodies, read bounds, and validators.

use std::fmt;
use std::num::NonZeroU64;

use crate::bounds::PUT_BYTES_MAX;

/// An immutable, bounded object body, encoded exactly once.
///
/// Authority-bearing candidates are sent once and never re-encoded after an
/// ambiguous response; freezing the bytes at construction is what makes the
/// later byte-compare reconciliation sound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenBytes(bytes::Bytes);

impl FrozenBytes {
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn clone_body(&self) -> bytes::Bytes {
        self.0.clone()
    }
}

impl TryFrom<Vec<u8>> for FrozenBytes {
    type Error = FrozenBytesError;

    fn try_from(raw: Vec<u8>) -> Result<Self, Self::Error> {
        if raw.is_empty() {
            return Err(FrozenBytesError::Empty);
        }
        // A usize wider than u64 cannot occur on supported targets; saturating
        // keeps the impossible branch on the rejecting side.
        let bytes_actual = u64::try_from(raw.len()).unwrap_or(u64::MAX);
        if bytes_actual > PUT_BYTES_MAX {
            return Err(FrozenBytesError::OverPutBound { bytes_actual });
        }
        Ok(Self(bytes::Bytes::from(raw)))
    }
}

/// Why a raw body cannot become a durable object candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FrozenBytesError {
    /// Every typed durable object has a non-empty body.
    #[error("object body is empty")]
    Empty,
    #[error("object body is {bytes_actual} bytes; the bound is {PUT_BYTES_MAX}")]
    OverPutBound { bytes_actual: u64 },
}

/// A caller-imposed ceiling on how many bytes one read may materialize.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteBound(NonZeroU64);

impl ByteBound {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for ByteBound {
    type Error = ZeroByteBound;

    fn try_from(raw: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(raw).map(Self).ok_or(ZeroByteBound)
    }
}

/// A byte bound of zero can never admit an object body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("byte bound is zero")]
pub struct ZeroByteBound;

/// The backend's opaque validator for one observed object version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Etag(String);

impl Etag {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Etag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl TryFrom<String> for Etag {
    type Error = EmptyEtag;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        if raw.is_empty() {
            return Err(EmptyEtag);
        }
        Ok(Self(raw))
    }
}

/// An empty validator can never prove which object version was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("etag is empty")]
pub struct EmptyEtag;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_bodies_and_zero_bounds_are_rejected() {
        assert_eq!(
            Err(FrozenBytesError::Empty),
            FrozenBytes::try_from(Vec::new()),
            "empty body is illegal"
        );
        assert_eq!(
            Err(ZeroByteBound),
            ByteBound::try_from(0),
            "zero bound is illegal"
        );
        assert_eq!(
            Err(EmptyEtag),
            Etag::try_from(String::new()),
            "empty etag is illegal"
        );
    }
}
