//! WAL replay owner token.

use crate::coordinate::SealGeneration;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, rkyv::Archive, rkyv::Serialize,
)]
pub struct OwnerToken(SealGeneration);

impl From<&ArchivedOwnerToken> for OwnerToken {
    fn from(token: &ArchivedOwnerToken) -> Self {
        Self(SealGeneration::from(&token.0))
    }
}

impl From<SealGeneration> for OwnerToken {
    fn from(generation: SealGeneration) -> Self {
        Self(generation)
    }
}
