//! Canonical durable key spellings.

use std::fmt;
use std::str::FromStr;

use crate::{BatchId, PartitionId, PartitionIdError, SealGeneration, SealIdentity, WalIdentity};

const REVERSE_ORDINAL_DIGITS: usize = 20;
const PARTITION_SEGMENT: &str = "partition";
const SEAL_SEGMENT: &str = "seal";
const WAL_SEGMENT: &str = "wal";
const NAMESPACE_VERSION: &str = "v1";

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

struct ReverseOrdinal(u64);

impl fmt::Display for ReverseOrdinal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reversed = u64::MAX
            .checked_sub(self.0)
            .expect("durable coordinates never exceed u64::MAX");
        write!(formatter, "{reversed:020}")
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySpellingError {
    Shape,
    UnsupportedNamespace,
    Partition(PartitionIdError),
    ReverseOrdinal,
    ZeroCoordinate,
}

impl fmt::Display for KeySpellingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shape => formatter.write_str("durable key has the wrong segment shape"),
            Self::UnsupportedNamespace => {
                formatter.write_str("durable key namespace version is unsupported")
            }
            Self::Partition(error) => {
                write!(formatter, "durable key partition is invalid: {error}")
            }
            Self::ReverseOrdinal => {
                formatter.write_str("durable key reverse ordinal is not a fixed-width 20-digit u64")
            }
            Self::ZeroCoordinate => {
                formatter.write_str("durable key spells reserved coordinate zero")
            }
        }
    }
}

impl std::error::Error for KeySpellingError {}
