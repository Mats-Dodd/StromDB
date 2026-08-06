//! Permanent Seal and manifest vocabulary.

mod codec;

use std::collections::BTreeSet;
use std::num::NonZeroU64;

use crate::bounds::{RUN_TABLES_MAX, SST_OBJECT_BYTES_MAX, TREE_RUNS_MAX};
use crate::{BatchId, OwnerToken, PartitionId, SealGeneration, StoreKind, TableObjectId};

pub use codec::{decode_seal, encode_seal};

#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
pub struct Seal {
    partition: PartitionId,
    generation: SealGeneration,
    replay: WalReplayPoint,
    directory: TreeVersion,
    ledger: TreeVersion,
}

impl Seal {
    /// # Errors
    ///
    /// Returns [`SealError`] when a selected table contradicts the Seal
    /// generation or owning store.
    pub fn new(
        partition: PartitionId,
        generation: SealGeneration,
        replay: WalReplayPoint,
        directory: TreeVersion,
        ledger: TreeVersion,
    ) -> Result<Self, SealError> {
        validate_tree(&directory, StoreKind::Directory, generation)?;
        validate_tree(&ledger, StoreKind::Ledger, generation)?;
        let mut identities = BTreeSet::new();
        for tree in [&directory, &ledger] {
            for table in tree.tables() {
                if !identities.insert(table.object()) {
                    return Err(SealError::DuplicateTableObject);
                }
            }
        }
        Ok(Self {
            partition,
            generation,
            replay,
            directory,
            ledger,
        })
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
    pub const fn directory(&self) -> &TreeVersion {
        &self.directory
    }

    #[must_use]
    pub const fn ledger(&self) -> &TreeVersion {
        &self.ledger
    }
}

fn validate_tree(
    tree: &TreeVersion,
    store: StoreKind,
    generation: SealGeneration,
) -> Result<(), SealError> {
    for table in tree.tables() {
        if table.object().store() != store {
            return Err(SealError::StoreMismatch);
        }
        if table.object().fresh().birth_generation() > generation {
            return Err(SealError::FutureTable);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
pub enum WalReplayPoint {
    Genesis,
    Through { batch: BatchId, owner: OwnerToken },
}

#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
pub struct TreeVersion {
    runs: Vec<SortedRun>,
}

impl TreeVersion {
    #[must_use]
    pub const fn empty() -> Self {
        Self { runs: Vec::new() }
    }

    #[must_use]
    pub fn runs(&self) -> &[SortedRun] {
        &self.runs
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    fn tables(&self) -> impl Iterator<Item = &TableRef> {
        self.runs.iter().flat_map(|run| run.tables.iter())
    }
}

impl TryFrom<Vec<SortedRun>> for TreeVersion {
    type Error = SealError;

    fn try_from(runs: Vec<SortedRun>) -> Result<Self, Self::Error> {
        if runs.len() > TREE_RUNS_MAX {
            return Err(SealError::RunsOverMax);
        }
        Ok(Self { runs })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
pub struct SortedRun {
    tables: Vec<TableRef>,
}

impl SortedRun {
    #[must_use]
    pub fn tables(&self) -> &[TableRef] {
        &self.tables
    }
}

impl TryFrom<Vec<TableRef>> for SortedRun {
    type Error = SealError;

    fn try_from(tables: Vec<TableRef>) -> Result<Self, Self::Error> {
        if tables.is_empty() {
            return Err(SealError::EmptyRun);
        }
        if tables.len() > RUN_TABLES_MAX {
            return Err(SealError::TablesOverMax);
        }
        Ok(Self { tables })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
pub struct TableRef {
    object: TableObjectId,
    object_bytes: NonZeroU64,
}

impl TableRef {
    /// # Errors
    ///
    /// Returns [`SealError`] when the complete object length exceeds the SST bound.
    pub const fn new(object: TableObjectId, object_bytes: NonZeroU64) -> Result<Self, SealError> {
        if object_bytes.get() > SST_OBJECT_BYTES_MAX {
            return Err(SealError::ObjectBytesOverMax);
        }
        Ok(Self {
            object,
            object_bytes,
        })
    }

    #[must_use]
    pub const fn object(self) -> TableObjectId {
        self.object
    }

    #[must_use]
    pub const fn object_bytes(self) -> NonZeroU64 {
        self.object_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SealError {
    #[error("tree has more than {TREE_RUNS_MAX} runs")]
    RunsOverMax,
    #[error("sorted run is empty")]
    EmptyRun,
    #[error("sorted run has more than {RUN_TABLES_MAX} tables")]
    TablesOverMax,
    #[error("table object exceeds {SST_OBJECT_BYTES_MAX} bytes")]
    ObjectBytesOverMax,
    #[error("table store differs from its owning tree")]
    StoreMismatch,
    #[error("table birth generation is newer than its enclosing Seal")]
    FutureTable,
    #[error("table object identity occurs more than once in the Seal")]
    DuplicateTableObject,
}
