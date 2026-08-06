//! Nonzero storage coordinates and their shared failures.

use std::num::NonZeroU64;

/// A nonzero storage coordinate cannot be incremented beyond `u64::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("storage coordinate is exhausted")]
pub struct CoordinateExhausted;

/// Zero is reserved and is not a durable coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("storage coordinate is zero")]
pub struct ZeroCoordinate;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, rkyv::Archive, rkyv::Serialize,
)]
pub struct SealGeneration(NonZeroU64);

impl SealGeneration {
    #[must_use]
    pub const fn genesis() -> Self {
        Self(NonZeroU64::MIN)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// # Errors
    ///
    /// Returns [`CoordinateExhausted`] when this generation is `u64::MAX`.
    pub fn successor(self) -> Result<Self, CoordinateExhausted> {
        self.get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .map(Self)
            .ok_or(CoordinateExhausted)
    }
}

impl From<NonZeroU64> for SealGeneration {
    fn from(generation: NonZeroU64) -> Self {
        Self(generation)
    }
}

impl TryFrom<u64> for SealGeneration {
    type Error = ZeroCoordinate;

    fn try_from(generation: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(generation).map(Self).ok_or(ZeroCoordinate)
    }
}

impl From<&ArchivedSealGeneration> for SealGeneration {
    fn from(generation: &ArchivedSealGeneration) -> Self {
        Self(generation.0.to_native())
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, rkyv::Archive, rkyv::Serialize,
)]
pub struct BatchId(NonZeroU64);

impl BatchId {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// # Errors
    ///
    /// Returns [`CoordinateExhausted`] when this batch is `u64::MAX`.
    pub fn successor(self) -> Result<Self, CoordinateExhausted> {
        self.get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .map(Self)
            .ok_or(CoordinateExhausted)
    }
}

impl From<&ArchivedBatchId> for BatchId {
    fn from(batch: &ArchivedBatchId) -> Self {
        Self(batch.0.to_native())
    }
}

impl From<NonZeroU64> for BatchId {
    fn from(batch: NonZeroU64) -> Self {
        Self(batch)
    }
}

impl TryFrom<u64> for BatchId {
    type Error = ZeroCoordinate;

    fn try_from(batch: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(batch).map(Self).ok_or(ZeroCoordinate)
    }
}

/// Partition-local stream identity.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, rkyv::Archive, rkyv::Serialize,
)]
pub struct StreamUid(NonZeroU64);

impl From<&ArchivedStreamUid> for StreamUid {
    fn from(uid: &ArchivedStreamUid) -> Self {
        Self(uid.0.to_native())
    }
}

impl From<NonZeroU64> for StreamUid {
    fn from(uid: NonZeroU64) -> Self {
        Self(uid)
    }
}

impl TryFrom<u64> for StreamUid {
    type Error = ZeroCoordinate;

    fn try_from(uid: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(uid).map(Self).ok_or(ZeroCoordinate)
    }
}
