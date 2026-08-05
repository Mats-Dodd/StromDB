//! WAL replay owner token.

use serde::Serialize;

use crate::seal::SealGeneration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnerToken(SealGeneration);

impl OwnerToken {
    pub(crate) const fn encoded(self) -> u64 {
        self.0.get()
    }
}

impl From<SealGeneration> for OwnerToken {
    fn from(generation: SealGeneration) -> Self {
        Self(generation)
    }
}

impl Serialize for OwnerToken {
    fn serialize<Serializer: serde::Serializer>(
        &self,
        serializer: Serializer,
    ) -> Result<Serializer::Ok, Serializer::Error> {
        serializer.serialize_u64(self.encoded())
    }
}
