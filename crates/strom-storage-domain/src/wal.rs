//! Write-ahead log vocabulary.

mod codec;
mod fact;

use std::num::NonZeroU64;

use crate::bounds::WAL_RUN_FACTS_MAX;
use crate::{CoordinateExhausted, OwnerToken, PartitionId, ZeroCoordinate};

pub use codec::{decode_wal, encode_wal};
pub use fact::OperationFact;

#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
pub enum WalObject {
    Run(WalRun),
    Fence(WalFence),
}

impl WalObject {
    #[must_use]
    pub const fn identity(&self) -> WalIdentity {
        match self {
            Self::Run(run) => run.identity(),
            Self::Fence(fence) => fence.identity(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
pub struct WalRun {
    partition: PartitionId,
    batch: BatchId,
    owner: OwnerToken,
    facts: BoundedNonEmptyVec<OperationFact>,
}

impl WalRun {
    #[must_use]
    pub const fn new(
        partition: PartitionId,
        batch: BatchId,
        owner: OwnerToken,
        facts: BoundedNonEmptyVec<OperationFact>,
    ) -> Self {
        Self {
            partition,
            batch,
            owner,
            facts,
        }
    }

    #[must_use]
    pub const fn identity(&self) -> WalIdentity {
        WalIdentity::new(self.partition, self.batch)
    }

    #[must_use]
    pub const fn owner(&self) -> OwnerToken {
        self.owner
    }

    #[must_use]
    pub fn facts(&self) -> &[OperationFact] {
        self.facts.as_slice()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
pub struct WalFence {
    partition: PartitionId,
    batch: BatchId,
    owner: OwnerToken,
}

impl WalFence {
    #[must_use]
    pub const fn new(partition: PartitionId, batch: BatchId, owner: OwnerToken) -> Self {
        Self {
            partition,
            batch,
            owner,
        }
    }

    #[must_use]
    pub const fn identity(self) -> WalIdentity {
        WalIdentity::new(self.partition, self.batch)
    }

    #[must_use]
    pub const fn owner(self) -> OwnerToken {
        self.owner
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WalIdentity {
    partition: PartitionId,
    batch: BatchId,
}

impl WalIdentity {
    #[must_use]
    pub const fn new(partition: PartitionId, batch: BatchId) -> Self {
        Self { partition, batch }
    }

    #[must_use]
    pub const fn partition(self) -> PartitionId {
        self.partition
    }

    #[must_use]
    pub const fn batch(self) -> BatchId {
        self.batch
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

#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
pub struct BoundedNonEmptyVec<Value> {
    values: Vec<Value>,
}

impl<Value> BoundedNonEmptyVec<Value> {
    #[must_use]
    pub fn as_slice(&self) -> &[Value] {
        &self.values
    }

    #[must_use]
    pub fn into_vec(self) -> Vec<Value> {
        self.values
    }
}

impl<Value> TryFrom<Vec<Value>> for BoundedNonEmptyVec<Value> {
    type Error = BoundedNonEmptyVecError;

    fn try_from(values: Vec<Value>) -> Result<Self, Self::Error> {
        if values.is_empty() {
            return Err(BoundedNonEmptyVecError::Empty);
        }
        if values.len() > WAL_RUN_FACTS_MAX {
            return Err(BoundedNonEmptyVecError::OverMax {
                facts_actual: values.len(),
            });
        }
        Ok(Self { values })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BoundedNonEmptyVecError {
    #[error("WAL run has no operation facts")]
    Empty,
    #[error("WAL run has {facts_actual} facts; the bound is {WAL_RUN_FACTS_MAX}")]
    OverMax { facts_actual: usize },
}
