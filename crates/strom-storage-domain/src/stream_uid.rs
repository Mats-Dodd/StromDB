//! Partition-local stream identity.

use std::num::NonZeroU64;

use crate::ZeroCoordinate;

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
