//! Permanent Seal vocabulary.

mod codec;

use std::num::NonZeroU64;

use serde::Serialize;

use crate::{CoordinateExhausted, OwnerToken, PartitionId, ZeroCoordinate};

pub use codec::{decode_seal, encode_seal};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Seal {
    partition: PartitionId,
    generation: SealGeneration,
    replay: WalReplayPoint,
    format: SealFormat,
    ledger: TreeVersion,
    tally: TreeVersion,
    annals: TreeVersion,
}

impl Seal {
    #[must_use]
    pub const fn new(
        partition: PartitionId,
        generation: SealGeneration,
        replay: WalReplayPoint,
        format: SealFormat,
        ledger: TreeVersion,
        tally: TreeVersion,
        annals: TreeVersion,
    ) -> Self {
        Self {
            partition,
            generation,
            replay,
            format,
            ledger,
            tally,
            annals,
        }
    }

    #[must_use]
    pub const fn identity(&self) -> SealIdentity {
        SealIdentity::new(self.partition, self.generation)
    }

    #[must_use]
    pub const fn replay(&self) -> WalReplayPoint {
        self.replay
    }

    #[must_use]
    pub const fn format(&self) -> SealFormat {
        self.format
    }

    #[must_use]
    pub const fn ledger(&self) -> TreeVersion {
        self.ledger
    }

    #[must_use]
    pub const fn tally(&self) -> TreeVersion {
        self.tally
    }

    #[must_use]
    pub const fn annals(&self) -> TreeVersion {
        self.annals
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct SealIdentity {
    partition: PartitionId,
    generation: SealGeneration,
}

impl SealIdentity {
    #[must_use]
    pub const fn new(partition: PartitionId, generation: SealGeneration) -> Self {
        Self {
            partition,
            generation,
        }
    }

    #[must_use]
    pub const fn partition(self) -> PartitionId {
        self.partition
    }

    #[must_use]
    pub const fn generation(self) -> SealGeneration {
        self.generation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

impl Serialize for SealGeneration {
    fn serialize<Serializer: serde::Serializer>(
        &self,
        serializer: Serializer,
    ) -> Result<Serializer::Ok, Serializer::Error> {
        serializer.serialize_u64(self.get())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum WalReplayPoint {
    Genesis,
    Through {
        batch: crate::BatchId,
        owner: OwnerToken,
    },
}

impl WalReplayPoint {
    #[must_use]
    pub const fn batch(self) -> Option<crate::BatchId> {
        match self {
            Self::Genesis => None,
            Self::Through { batch, owner: _ } => Some(batch),
        }
    }

    #[must_use]
    pub const fn owner(self) -> Option<OwnerToken> {
        match self {
            Self::Genesis => None,
            Self::Through { batch: _, owner } => Some(owner),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealFormat {
    V1,
}

impl Serialize for SealFormat {
    fn serialize<Serializer: serde::Serializer>(
        &self,
        serializer: Serializer,
    ) -> Result<Serializer::Ok, Serializer::Error> {
        match self {
            Self::V1 => serializer.serialize_u8(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct TreeVersion(());

impl TreeVersion {
    #[must_use]
    pub const fn empty() -> Self {
        Self(())
    }
}
