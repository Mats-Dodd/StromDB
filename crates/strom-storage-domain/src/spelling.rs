//! Canonical durable key spellings.

use std::fmt;
use std::str::FromStr;

use crate::{
    AttemptId, BatchId, FreshIdentity, PartitionId, PartitionIdError, SealGeneration, SealIdentity,
    StoreKind, TableObjectId, WalIdentity,
};

const REVERSE_ORDINAL_DIGITS: usize = 20;
const GENERATION_DIGITS: usize = 20;
const TABLE_ORDINAL_DIGITS: usize = 10;
const PARTITION_SEGMENT: &str = "partition";
const SEAL_SEGMENT: &str = "seal";
const TABLE_SEGMENT: &str = "table";
const WAL_SEGMENT: &str = "wal";
const NAMESPACE_VERSION: &str = "v1";

/// Ascending LIST prefix for one partition's Seal namespace.
///
/// Keys embed reverse ordinals, so `MaxKeys=1` under this prefix surfaces the
/// newest generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SealNamespace(PartitionId);

impl SealNamespace {
    #[must_use]
    pub const fn new(partition: PartitionId) -> Self {
        Self(partition)
    }

    #[must_use]
    pub const fn partition(self) -> PartitionId {
        self.0
    }
}

impl From<PartitionId> for SealNamespace {
    fn from(partition: PartitionId) -> Self {
        Self(partition)
    }
}

impl FromStr for SealNamespace {
    type Err = KeySpellingError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Ok(Self(parse_namespace(input, SEAL_SEGMENT)?))
    }
}

impl fmt::Display for SealNamespace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{PARTITION_SEGMENT}/{}/{SEAL_SEGMENT}/{NAMESPACE_VERSION}",
            self.0
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SealKey(SealIdentity);

impl SealKey {
    #[must_use]
    pub const fn identity(self) -> SealIdentity {
        self.0
    }
}

impl From<SealIdentity> for SealKey {
    fn from(identity: SealIdentity) -> Self {
        Self(identity)
    }
}

impl FromStr for SealKey {
    type Err = KeySpellingError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (partition, ordinal) = parse_key(input, SEAL_SEGMENT)?;
        let generation = SealGeneration::try_from(ordinal)
            .map_err(|_detail| KeySpellingError::ZeroCoordinate)?;
        Ok(Self(SealIdentity::new(partition, generation)))
    }
}

impl fmt::Display for SealKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{PARTITION_SEGMENT}/{}/{SEAL_SEGMENT}/{NAMESPACE_VERSION}/{}",
            self.0.partition(),
            ReverseOrdinal(self.0.generation().get())
        )
    }
}

/// Ascending LIST prefix for one partition's WAL namespace.
///
/// Keys embed reverse ordinals, so `MaxKeys=1` under this prefix surfaces the
/// newest surviving batch coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WalNamespace(PartitionId);

impl WalNamespace {
    #[must_use]
    pub const fn new(partition: PartitionId) -> Self {
        Self(partition)
    }

    #[must_use]
    pub const fn partition(self) -> PartitionId {
        self.0
    }
}

impl From<PartitionId> for WalNamespace {
    fn from(partition: PartitionId) -> Self {
        Self(partition)
    }
}

impl FromStr for WalNamespace {
    type Err = KeySpellingError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Ok(Self(parse_namespace(input, WAL_SEGMENT)?))
    }
}

impl fmt::Display for WalNamespace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{PARTITION_SEGMENT}/{}/{WAL_SEGMENT}/{NAMESPACE_VERSION}",
            self.0
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WalKey(WalIdentity);

impl WalKey {
    #[must_use]
    pub const fn identity(self) -> WalIdentity {
        self.0
    }
}

impl From<WalIdentity> for WalKey {
    fn from(identity: WalIdentity) -> Self {
        Self(identity)
    }
}

impl FromStr for WalKey {
    type Err = KeySpellingError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (partition, ordinal) = parse_key(input, WAL_SEGMENT)?;
        let batch =
            BatchId::try_from(ordinal).map_err(|_detail| KeySpellingError::ZeroCoordinate)?;
        Ok(Self(WalIdentity::new(partition, batch)))
    }
}

impl fmt::Display for WalKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{PARTITION_SEGMENT}/{}/{WAL_SEGMENT}/{NAMESPACE_VERSION}/{}",
            self.0.partition(),
            ReverseOrdinal(self.0.batch().get())
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableKey {
    partition: PartitionId,
    object: TableObjectId,
}

impl TableKey {
    #[must_use]
    pub const fn new(partition: PartitionId, object: TableObjectId) -> Self {
        Self { partition, object }
    }

    #[must_use]
    pub const fn partition(self) -> PartitionId {
        self.partition
    }

    #[must_use]
    pub const fn object(self) -> TableObjectId {
        self.object
    }
}

impl FromStr for TableKey {
    type Err = KeySpellingError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut segments = input.split('/');
        if segments.next() != Some(PARTITION_SEGMENT) {
            return Err(KeySpellingError::Shape);
        }
        let partition = segments.next().ok_or(KeySpellingError::Shape)?;
        if segments.next() != Some(TABLE_SEGMENT) {
            return Err(KeySpellingError::Shape);
        }
        if segments.next() != Some(NAMESPACE_VERSION) {
            return Err(KeySpellingError::UnsupportedNamespace);
        }
        let store = parse_store(segments.next().ok_or(KeySpellingError::Shape)?)?;
        let birth = parse_fixed_u64(
            segments.next().ok_or(KeySpellingError::Shape)?,
            GENERATION_DIGITS,
        )?;
        let attempt = segments.next().ok_or(KeySpellingError::Shape)?;
        let (owner, counter) = attempt
            .split_once('-')
            .ok_or(KeySpellingError::TableCoordinate)?;
        if counter.contains('-') {
            return Err(KeySpellingError::TableCoordinate);
        }
        let owner = parse_fixed_u64(owner, GENERATION_DIGITS)?;
        let counter = parse_fixed_u64(counter, GENERATION_DIGITS)?;
        let ordinal = u32::try_from(parse_fixed_u64(
            segments.next().ok_or(KeySpellingError::Shape)?,
            TABLE_ORDINAL_DIGITS,
        )?)
        .map_err(|_detail| KeySpellingError::TableCoordinate)?;
        if segments.next().is_some() {
            return Err(KeySpellingError::Shape);
        }

        let partition = partition.parse().map_err(KeySpellingError::Partition)?;
        let birth =
            SealGeneration::try_from(birth).map_err(|_detail| KeySpellingError::ZeroCoordinate)?;
        let owner =
            SealGeneration::try_from(owner).map_err(|_detail| KeySpellingError::ZeroCoordinate)?;
        let fresh = FreshIdentity::new(birth, AttemptId::new(owner, counter), ordinal)
            .map_err(|_detail| KeySpellingError::InvalidTableIdentity)?;
        Ok(Self::new(partition, TableObjectId::new(fresh, store)))
    }
}

impl fmt::Display for TableKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fresh = self.object.fresh();
        write!(
            formatter,
            "{PARTITION_SEGMENT}/{}/{TABLE_SEGMENT}/{NAMESPACE_VERSION}/{}/{:020}/{:020}-{:020}/{:010}",
            self.partition,
            store_name(self.object.store()),
            fresh.birth_generation().get(),
            fresh.attempt().owner_claim().get(),
            fresh.attempt().local_counter(),
            fresh.ordinal(),
        )
    }
}

struct ReverseOrdinal(u64);

impl fmt::Display for ReverseOrdinal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reversed = u64::MAX
            .checked_sub(self.0)
            .expect("durable coordinates never exceed u64::MAX");
        write!(formatter, "{reversed:020}")
    }
}

fn parse_namespace(input: &str, expected_kind: &str) -> Result<PartitionId, KeySpellingError> {
    let mut segments = input.split('/');
    if segments.next() != Some(PARTITION_SEGMENT) {
        return Err(KeySpellingError::Shape);
    }
    let partition = segments.next().ok_or(KeySpellingError::Shape)?;
    if segments.next() != Some(expected_kind) {
        return Err(KeySpellingError::Shape);
    }
    if segments.next() != Some(NAMESPACE_VERSION) {
        return Err(KeySpellingError::UnsupportedNamespace);
    }
    if segments.next().is_some() {
        return Err(KeySpellingError::Shape);
    }
    partition.parse().map_err(KeySpellingError::Partition)
}

fn parse_key(input: &str, expected_kind: &str) -> Result<(PartitionId, u64), KeySpellingError> {
    let mut segments = input.split('/');
    if segments.next() != Some(PARTITION_SEGMENT) {
        return Err(KeySpellingError::Shape);
    }
    let partition = segments.next().ok_or(KeySpellingError::Shape)?;
    if segments.next() != Some(expected_kind) {
        return Err(KeySpellingError::Shape);
    }
    if segments.next() != Some(NAMESPACE_VERSION) {
        return Err(KeySpellingError::UnsupportedNamespace);
    }
    let reverse_ordinal = segments.next().ok_or(KeySpellingError::Shape)?;
    if segments.next().is_some() {
        return Err(KeySpellingError::Shape);
    }
    let partition = partition.parse().map_err(KeySpellingError::Partition)?;
    let reversed = parse_reverse_ordinal(reverse_ordinal)?;
    let ordinal = u64::MAX
        .checked_sub(reversed)
        .expect("a parsed u64 reverse ordinal cannot exceed u64::MAX");
    Ok((partition, ordinal))
}

fn parse_reverse_ordinal(input: &str) -> Result<u64, KeySpellingError> {
    if input.len() != REVERSE_ORDINAL_DIGITS || !input.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(KeySpellingError::ReverseOrdinal);
    }
    input
        .parse()
        .map_err(|_detail| KeySpellingError::ReverseOrdinal)
}

fn parse_fixed_u64(input: &str, digits: usize) -> Result<u64, KeySpellingError> {
    if input.len() != digits || !input.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(KeySpellingError::TableCoordinate);
    }
    input
        .parse()
        .map_err(|_detail| KeySpellingError::TableCoordinate)
}

const fn store_name(store: StoreKind) -> &'static str {
    match store {
        StoreKind::Directory => "directory",
        StoreKind::Ledger => "ledger",
        StoreKind::Tally => "tally",
        StoreKind::Annals => "annals",
    }
}

fn parse_store(input: &str) -> Result<StoreKind, KeySpellingError> {
    match input {
        "directory" => Ok(StoreKind::Directory),
        "ledger" => Ok(StoreKind::Ledger),
        "tally" => Ok(StoreKind::Tally),
        "annals" => Ok(StoreKind::Annals),
        _ => Err(KeySpellingError::TableStore),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum KeySpellingError {
    #[error("durable key has the wrong segment shape")]
    Shape,
    #[error("durable key namespace version is unsupported")]
    UnsupportedNamespace,
    #[error("durable key partition is invalid: {0}")]
    Partition(#[source] PartitionIdError),
    #[error("durable key reverse ordinal is not a fixed-width 20-digit u64")]
    ReverseOrdinal,
    #[error("durable key spells reserved coordinate zero")]
    ZeroCoordinate,
    #[error("table key store spelling is not canonical")]
    TableStore,
    #[error("table key coordinate does not have its canonical fixed-width decimal spelling")]
    TableCoordinate,
    #[error("table key fresh identity violates its generation ordering")]
    InvalidTableIdentity,
}
