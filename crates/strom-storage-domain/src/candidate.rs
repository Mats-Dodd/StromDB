//! Identity-bearing durable candidates encoded exactly once.

use std::num::NonZeroU64;

use bytes::Bytes;

use crate::{
    BatchId, DirectoryEntry, DirectoryKey, EncodeError, LedgerCell, PartitionId,
    SEAL_ENCODED_BYTES_MAX, SST_OBJECT_BYTES_MAX, Seal, SealGeneration, SstEncodeError, StreamUid,
    TableKey, TableRef, WAL_ENCODED_BYTES_MAX, WalObject, encode_directory_sst, encode_ledger_sst,
    encode_seal, encode_wal,
};

/// One WAL candidate whose identity and exact sent bytes agree by construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedWal {
    partition: PartitionId,
    batch: BatchId,
    bytes: Bytes,
}

impl EncodedWal {
    /// Encode `object` once and retain the exact bytes used for publication.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError`] when the WAL object cannot be encoded.
    pub fn new(object: &WalObject) -> Result<Self, EncodeError> {
        let bytes = Bytes::from(encode_wal(object)?);
        assert_candidate_bytes(&bytes, WAL_ENCODED_BYTES_MAX, "WAL");
        Ok(Self {
            partition: object.partition(),
            batch: object.batch(),
            bytes,
        })
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
    pub const fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EncodedSeal {
    seal: Seal,
    bytes: Bytes,
}

impl EncodedSeal {
    fn new(seal: &Seal) -> Result<Self, EncodeError> {
        let bytes = Bytes::from(encode_seal(seal)?);
        assert_candidate_bytes(&bytes, SEAL_ENCODED_BYTES_MAX, "Seal");
        Ok(Self {
            seal: seal.clone(),
            bytes,
        })
    }
}

/// A canonical generation-zero Seal candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedGenesisSeal(EncodedSeal);

impl EncodedGenesisSeal {
    #[must_use]
    pub const fn generation(&self) -> SealGeneration {
        self.0.seal.generation()
    }

    #[must_use]
    pub const fn bytes(&self) -> &Bytes {
        &self.0.bytes
    }
}

/// A non-genesis Seal candidate whose direct creation grants writer authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedAuthoritySeal(EncodedSeal);

impl EncodedAuthoritySeal {
    #[must_use]
    pub const fn generation(&self) -> SealGeneration {
        self.0.seal.generation()
    }

    #[must_use]
    pub const fn seal(&self) -> &Seal {
        &self.0.seal
    }

    #[must_use]
    pub const fn bytes(&self) -> &Bytes {
        &self.0.bytes
    }
}

/// Why a Seal cannot be refined into one durable publication role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SealCandidateError {
    #[error("a genesis candidate must name the genesis generation")]
    NotGenesis,
    #[error("an authority candidate must advance beyond the genesis generation")]
    NotAuthority,
    #[error(transparent)]
    Encode(#[from] EncodeError),
}

impl TryFrom<&Seal> for EncodedGenesisSeal {
    type Error = SealCandidateError;

    fn try_from(seal: &Seal) -> Result<Self, Self::Error> {
        if seal.generation() != SealGeneration::genesis() {
            return Err(SealCandidateError::NotGenesis);
        }
        Ok(Self(EncodedSeal::new(seal)?))
    }
}

impl TryFrom<&Seal> for EncodedAuthoritySeal {
    type Error = SealCandidateError;

    fn try_from(seal: &Seal) -> Result<Self, Self::Error> {
        if seal.generation() == SealGeneration::genesis() {
            return Err(SealCandidateError::NotAuthority);
        }
        Ok(Self(EncodedSeal::new(seal)?))
    }
}

/// One SST candidate whose key, reference, and exact encoded bytes agree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedTable {
    key: TableKey,
    table: TableRef,
    bytes: Bytes,
}

impl EncodedTable {
    /// Encode checked Directory rows for `key`.
    ///
    /// # Errors
    ///
    /// Returns [`SstEncodeError`] when the rows exceed the durable SST bounds.
    pub fn encode_directory(
        partition: PartitionId,
        key: TableKey,
        rows: &[(DirectoryKey, DirectoryEntry)],
    ) -> Result<Self, SstEncodeError> {
        Ok(Self::from_encoded(
            key,
            encode_directory_sst(partition, &key, rows)?,
        ))
    }

    /// Encode checked Ledger rows for `key`.
    ///
    /// # Errors
    ///
    /// Returns [`SstEncodeError`] when the rows exceed the durable SST bounds.
    pub fn encode_ledger(
        partition: PartitionId,
        key: TableKey,
        rows: &[(StreamUid, LedgerCell)],
    ) -> Result<Self, SstEncodeError> {
        Ok(Self::from_encoded(
            key,
            encode_ledger_sst(partition, &key, rows)?,
        ))
    }

    fn from_encoded(key: TableKey, encoded: Vec<u8>) -> Self {
        let bytes = Bytes::from(encoded);
        assert_candidate_bytes(
            &bytes,
            usize::try_from(SST_OBJECT_BYTES_MAX)
                .expect("the SST object bound fits in usize on supported targets"),
            "SST",
        );
        let object_bytes =
            NonZeroU64::new(u64::try_from(bytes.len()).expect("an encoded SST length fits in u64"))
                .expect("an encoded SST is nonempty");
        let table = TableRef::new(key.object(), object_bytes)
            .expect("the SST encoder enforces the hard object bound");
        Self { key, table, bytes }
    }

    #[must_use]
    pub const fn key(&self) -> TableKey {
        self.key
    }

    #[must_use]
    pub const fn table(&self) -> TableRef {
        self.table
    }

    #[must_use]
    pub const fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

/// Checked rows decoded from either supported SST kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedTable {
    Directory(Vec<(DirectoryKey, DirectoryEntry)>),
    Ledger(Vec<(StreamUid, LedgerCell)>),
}

fn assert_candidate_bytes(bytes: &Bytes, bytes_max: usize, kind: &str) {
    assert!(!bytes.is_empty(), "an encoded {kind} candidate is nonempty");
    assert!(
        bytes.len() <= bytes_max,
        "an encoded {kind} candidate stays within its durable byte bound"
    );
}
