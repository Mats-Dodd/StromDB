//! Permanent Seal and manifest vocabulary.

mod codec;

use std::collections::BTreeSet;
use std::num::NonZeroU64;

use serde::Serialize;

use crate::bounds::{RUN_TABLES_MAX, SST_OBJECT_BYTES_MAX, TREE_RANGES_MAX_V2, TREE_RUNS_MAX};
use crate::{CoordinateExhausted, OwnerToken, PartitionId, ZeroCoordinate};

pub use codec::{decode_seal, encode_seal};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seal {
    partition: PartitionId,
    generation: SealGeneration,
    replay: WalReplayPoint,
    format: SealFormat,
    directory: TreeVersion,
    ledger: TreeVersion,
    tally: TreeVersion,
    annals: TreeVersion,
}

impl Seal {
    /// # Errors
    ///
    /// Returns [`SealError`] when a tree is not a canonical V2 manifest or a
    /// selected table contradicts the Seal generation or owning store.
    #[expect(
        clippy::too_many_arguments,
        reason = "the four peer trees are explicit fields in one atomic Seal"
    )]
    pub fn new(
        partition: PartitionId,
        generation: SealGeneration,
        replay: WalReplayPoint,
        format: SealFormat,
        directory: TreeVersion,
        ledger: TreeVersion,
        tally: TreeVersion,
        annals: TreeVersion,
    ) -> Result<Self, SealError> {
        validate_tree(&directory, StoreKind::Directory, generation)?;
        validate_tree(&ledger, StoreKind::Ledger, generation)?;
        validate_tree(&tally, StoreKind::Tally, generation)?;
        validate_tree(&annals, StoreKind::Annals, generation)?;
        if !tally.is_empty() || !annals.is_empty() {
            return Err(SealError::DeferredStoreNonEmpty);
        }
        let mut identities = BTreeSet::new();
        for tree in [&directory, &ledger, &tally, &annals] {
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
            format,
            directory,
            ledger,
            tally,
            annals,
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
    pub const fn format(&self) -> SealFormat {
        self.format
    }

    #[must_use]
    pub const fn directory(&self) -> &TreeVersion {
        &self.directory
    }

    #[must_use]
    pub const fn ledger(&self) -> &TreeVersion {
        &self.ledger
    }

    #[must_use]
    pub const fn tally(&self) -> &TreeVersion {
        &self.tally
    }

    #[must_use]
    pub const fn annals(&self) -> &TreeVersion {
        &self.annals
    }
}

fn validate_tree(
    tree: &TreeVersion,
    store: StoreKind,
    generation: SealGeneration,
) -> Result<(), SealError> {
    let [range] = tree.ranges() else {
        return Err(SealError::RangeCount);
    };
    if range.start() != &KeyBound::Minimum || range.end() != &KeyBound::Maximum {
        return Err(SealError::RangeBounds);
    }
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
    V2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeVersion {
    ranges: Vec<RangeVersion>,
}

impl TreeVersion {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            ranges: vec![RangeVersion {
                start: KeyBound::Minimum,
                end: KeyBound::Maximum,
                runs: Vec::new(),
            }],
        }
    }

    /// # Errors
    ///
    /// Returns [`SealError`] unless there is exactly one range.
    pub fn try_from_ranges(ranges: Vec<RangeVersion>) -> Result<Self, SealError> {
        if ranges.len() != TREE_RANGES_MAX_V2 {
            return Err(SealError::RangeCount);
        }
        Ok(Self { ranges })
    }

    #[must_use]
    pub fn ranges(&self) -> &[RangeVersion] {
        &self.ranges
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ranges.iter().all(|range| range.runs.is_empty())
    }

    fn tables(&self) -> impl Iterator<Item = &TableRef> {
        self.ranges
            .iter()
            .flat_map(|range| range.runs.iter())
            .flat_map(|run| run.tables.iter())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeVersion {
    start: KeyBound,
    end: KeyBound,
    runs: Vec<SortedRun>,
}

impl RangeVersion {
    /// # Errors
    ///
    /// Returns [`SealError`] when the range carries too many runs.
    pub fn new(start: KeyBound, end: KeyBound, runs: Vec<SortedRun>) -> Result<Self, SealError> {
        if runs.len() > TREE_RUNS_MAX {
            return Err(SealError::RunsOverMax);
        }
        Ok(Self { start, end, runs })
    }

    /// # Errors
    ///
    /// Returns [`SealError`] when the full range carries too many runs.
    pub fn full(runs: Vec<SortedRun>) -> Result<Self, SealError> {
        Self::new(KeyBound::Minimum, KeyBound::Maximum, runs)
    }

    #[must_use]
    pub const fn start(&self) -> &KeyBound {
        &self.start
    }

    #[must_use]
    pub const fn end(&self) -> &KeyBound {
        &self.end
    }

    #[must_use]
    pub fn runs(&self) -> &[SortedRun] {
        &self.runs
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyBound {
    Minimum,
    Key(Box<[u8]>),
    Maximum,
}

impl KeyBound {
    #[must_use]
    pub fn key<Bytes: Into<Box<[u8]>>>(bytes: Bytes) -> Self {
        Self::Key(bytes.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortedRun {
    tables: Vec<TableRef>,
}

impl SortedRun {
    /// # Errors
    ///
    /// Returns [`SealError`] when the run is empty or over its table bound.
    pub fn try_from_tables(tables: Vec<TableRef>) -> Result<Self, SealError> {
        if tables.is_empty() {
            return Err(SealError::EmptyRun);
        }
        if tables.len() > RUN_TABLES_MAX {
            return Err(SealError::TablesOverMax);
        }
        Ok(Self { tables })
    }

    #[must_use]
    pub fn tables(&self) -> &[TableRef] {
        &self.tables
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableObjectId {
    fresh: FreshIdentity,
    store: StoreKind,
}

impl TableObjectId {
    #[must_use]
    pub const fn new(fresh: FreshIdentity, store: StoreKind) -> Self {
        Self { fresh, store }
    }

    #[must_use]
    pub const fn fresh(self) -> FreshIdentity {
        self.fresh
    }

    #[must_use]
    pub const fn store(self) -> StoreKind {
        self.store
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FreshIdentity {
    birth_generation: SealGeneration,
    attempt: AttemptId,
    ordinal: u32,
}

impl FreshIdentity {
    /// # Errors
    ///
    /// Returns [`SealError`] unless the owner claim predates the birth generation.
    pub fn new(
        birth_generation: SealGeneration,
        attempt: AttemptId,
        ordinal: u32,
    ) -> Result<Self, SealError> {
        if attempt.owner_claim() >= birth_generation {
            return Err(SealError::OwnerNotBeforeBirth);
        }
        Ok(Self {
            birth_generation,
            attempt,
            ordinal,
        })
    }

    #[must_use]
    pub const fn birth_generation(self) -> SealGeneration {
        self.birth_generation
    }

    #[must_use]
    pub const fn attempt(self) -> AttemptId {
        self.attempt
    }

    #[must_use]
    pub const fn ordinal(self) -> u32 {
        self.ordinal
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AttemptId {
    owner_claim: SealGeneration,
    local_counter: u64,
}

impl AttemptId {
    #[must_use]
    pub const fn new(owner_claim: SealGeneration, local_counter: u64) -> Self {
        Self {
            owner_claim,
            local_counter,
        }
    }

    #[must_use]
    pub const fn owner_claim(self) -> SealGeneration {
        self.owner_claim
    }

    #[must_use]
    pub const fn local_counter(self) -> u64 {
        self.local_counter
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StoreKind {
    Directory,
    Ledger,
    Tally,
    Annals,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SealError {
    #[error("V2 tree must contain exactly one range")]
    RangeCount,
    #[error("V2 tree range must span Minimum through Maximum")]
    RangeBounds,
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
    #[error("table owner claim does not predate its birth generation")]
    OwnerNotBeforeBirth,
    #[error("table object identity occurs more than once in the Seal")]
    DuplicateTableObject,
    #[error("Tally and Annals must be canonically empty in V2")]
    DeferredStoreNonEmpty,
}
