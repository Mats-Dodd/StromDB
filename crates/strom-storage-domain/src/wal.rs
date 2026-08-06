//! Write-ahead log vocabulary.

mod codec;
mod fact;

use crate::bounds::WAL_RUN_FACTS_MAX;
use crate::{BatchId, OwnerToken, PartitionId};

pub use codec::{decode_wal, encode_wal};
pub use fact::OperationFact;

#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
pub struct WalObject {
    partition: PartitionId,
    batch: BatchId,
    owner: OwnerToken,
    body: WalBody,
}

impl WalObject {
    #[must_use]
    pub const fn new(
        partition: PartitionId,
        batch: BatchId,
        owner: OwnerToken,
        body: WalBody,
    ) -> Self {
        Self {
            partition,
            batch,
            owner,
            body,
        }
    }

    #[must_use]
    pub const fn partition(&self) -> PartitionId {
        self.partition
    }

    #[must_use]
    pub const fn batch(&self) -> BatchId {
        self.batch
    }

    #[must_use]
    pub const fn owner(&self) -> OwnerToken {
        self.owner
    }

    #[must_use]
    pub const fn body(&self) -> &WalBody {
        &self.body
    }
}

#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
pub enum WalBody {
    Run(WalFacts),
    Fence,
}

#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
pub struct WalFacts {
    facts: Vec<OperationFact>,
}

impl WalFacts {
    #[must_use]
    pub fn as_slice(&self) -> &[OperationFact] {
        &self.facts
    }

    #[must_use]
    pub fn into_vec(self) -> Vec<OperationFact> {
        self.facts
    }
}

impl TryFrom<Vec<OperationFact>> for WalFacts {
    type Error = WalFactsError;

    fn try_from(facts: Vec<OperationFact>) -> Result<Self, Self::Error> {
        if facts.is_empty() {
            return Err(WalFactsError::Empty);
        }
        if facts.len() > WAL_RUN_FACTS_MAX {
            return Err(WalFactsError::OverMax {
                facts_actual: facts.len(),
            });
        }
        Ok(Self { facts })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WalFactsError {
    #[error("WAL run has no operation facts")]
    Empty,
    #[error("WAL run has {facts_actual} facts; the bound is {WAL_RUN_FACTS_MAX}")]
    OverMax { facts_actual: usize },
}
