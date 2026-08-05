//! Partition-local stream identity.

use std::num::NonZeroU64;

use serde::Serialize;

use crate::ZeroCoordinate;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamUid(NonZeroU64);

impl StreamUid {
    pub(crate) const fn encoded(self) -> u64 {
        self.0.get()
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

impl Serialize for StreamUid {
    fn serialize<Serializer: serde::Serializer>(
        &self,
        serializer: Serializer,
    ) -> Result<Serializer::Ok, Serializer::Error> {
        serializer.serialize_u64(self.encoded())
    }
}
