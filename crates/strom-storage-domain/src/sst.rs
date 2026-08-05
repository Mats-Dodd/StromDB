//! Fixed-header sequential SST V1 codecs.

#![expect(
    clippy::big_endian_bytes,
    reason = "RFC 0003 fixes every SST numeric field as big-endian"
)]

mod directory;
mod ledger;

use crate::bounds::SST_OBJECT_BYTES_MAX;
use crate::{DecodeError, FreshIdentity, PartitionId, StoreKind, TableKey, TableObjectId};

pub use directory::{decode_directory_sst, encode_directory_sst};
pub use ledger::{decode_ledger_sst, encode_ledger_sst};

const MAGIC: &[u8; 4] = b"STRM";
const OBJECT_KIND_SST: u8 = 3;
const SST_VERSION: u8 = 1;
pub const SST_HEADER_BYTES: usize = 52;

const _: () = assert!(
    SST_HEADER_BYTES
        == MAGIC.len() + 2 + size_of::<[u8; 16]>() + (3 * size_of::<u64>()) + size_of::<u32>() + 2,
    "the SST V1 header layout is exactly 52 bytes"
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowCodec {
    DirectoryV1,
    LedgerV1,
}

impl RowCodec {
    const fn encoded(self) -> u8 {
        match self {
            Self::DirectoryV1 => 1,
            Self::LedgerV1 => 2,
        }
    }

    const fn store(self) -> StoreKind {
        match self {
            Self::DirectoryV1 => StoreKind::Directory,
            Self::LedgerV1 => StoreKind::Ledger,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SstHeader {
    partition: PartitionId,
    object: TableObjectId,
    row_codec: RowCodec,
}

impl SstHeader {
    /// # Errors
    ///
    /// Returns [`SstEncodeError::StoreCodecMismatch`] when the row codec is
    /// not the one defined for the object's store.
    pub fn new(
        partition: PartitionId,
        object: TableObjectId,
        row_codec: RowCodec,
    ) -> Result<Self, SstEncodeError> {
        if object.store() != row_codec.store() {
            return Err(SstEncodeError::StoreCodecMismatch);
        }
        Ok(Self {
            partition,
            object,
            row_codec,
        })
    }

    #[must_use]
    pub const fn partition(self) -> PartitionId {
        self.partition
    }

    #[must_use]
    pub const fn object(self) -> TableObjectId {
        self.object
    }

    #[must_use]
    pub const fn row_codec(self) -> RowCodec {
        self.row_codec
    }
}

/// Encodes the manual, checksum-free SST V1 header.
#[must_use]
pub fn encode_sst_header(header: SstHeader) -> [u8; SST_HEADER_BYTES] {
    let mut bytes = [0; SST_HEADER_BYTES];
    bytes[..4].copy_from_slice(MAGIC);
    bytes[4] = OBJECT_KIND_SST;
    bytes[5] = SST_VERSION;
    bytes[6..22].copy_from_slice(header.partition.as_bytes());
    let fresh = header.object.fresh();
    bytes[22..30].copy_from_slice(&fresh.birth_generation().get().to_be_bytes());
    bytes[30..38].copy_from_slice(&fresh.attempt().owner_claim().get().to_be_bytes());
    bytes[38..46].copy_from_slice(&fresh.attempt().local_counter().to_be_bytes());
    bytes[46..50].copy_from_slice(&fresh.ordinal().to_be_bytes());
    bytes[50] = encode_store(header.object.store());
    bytes[51] = header.row_codec.encoded();
    bytes
}

/// Decodes the fixed header and returns the untouched row suffix.
///
/// # Errors
///
/// Returns [`SstDecodeError`] when the complete object is over-bound, the
/// header is incomplete or malformed, its store and codec disagree, or its
/// object identity differs from `expected_object`.
pub fn decode_sst_header<'bytes>(
    expected_object: &TableObjectId,
    bytes: &'bytes [u8],
) -> Result<(SstHeader, &'bytes [u8]), SstDecodeError> {
    enforce_object_bound(bytes)?;
    let header_bytes = bytes
        .get(..SST_HEADER_BYTES)
        .ok_or(SstDecodeError::HeaderTooShort {
            bytes_actual: bytes.len(),
        })?;
    let magic: [u8; 4] = header_bytes
        .get(..4)
        .ok_or(SstDecodeError::HeaderTooShort {
            bytes_actual: bytes.len(),
        })?
        .try_into()
        .map_err(|_detail| SstDecodeError::HeaderTooShort {
            bytes_actual: bytes.len(),
        })?;
    if magic != *MAGIC {
        return Err(SstDecodeError::MagicMismatch { observed: magic });
    }
    let observed_kind = header_bytes
        .get(4)
        .copied()
        .ok_or(SstDecodeError::HeaderTooShort {
            bytes_actual: bytes.len(),
        })?;
    if observed_kind != OBJECT_KIND_SST {
        return Err(SstDecodeError::ObjectKindMismatch {
            observed: observed_kind,
        });
    }
    let observed_version = header_bytes
        .get(5)
        .copied()
        .ok_or(SstDecodeError::HeaderTooShort {
            bytes_actual: bytes.len(),
        })?;
    if observed_version != SST_VERSION {
        return Err(SstDecodeError::UnsupportedVersion {
            observed: observed_version,
        });
    }
    let partition_bytes: [u8; 16] = header_bytes
        .get(6..22)
        .ok_or(SstDecodeError::HeaderTooShort {
            bytes_actual: bytes.len(),
        })?
        .try_into()
        .map_err(|_detail| SstDecodeError::HeaderTooShort {
            bytes_actual: bytes.len(),
        })?;
    let partition = PartitionId::try_from(partition_bytes)
        .map_err(|_detail| SstDecodeError::InvalidPartition)?;
    let birth = read_u64(header_bytes, 22)?;
    let owner = read_u64(header_bytes, 30)?;
    let counter = read_u64(header_bytes, 38)?;
    let ordinal = read_u32(header_bytes, 46)?;
    let store = decode_store(header_bytes.get(50).copied().ok_or(
        SstDecodeError::HeaderTooShort {
            bytes_actual: bytes.len(),
        },
    )?)?;
    let row_codec = decode_row_codec(header_bytes.get(51).copied().ok_or(
        SstDecodeError::HeaderTooShort {
            bytes_actual: bytes.len(),
        },
    )?)?;
    if store != row_codec.store() {
        return Err(SstDecodeError::StoreCodecMismatch);
    }
    let birth = crate::SealGeneration::try_from(birth)
        .map_err(|_detail| SstDecodeError::InvalidIdentity)?;
    let owner = crate::SealGeneration::try_from(owner)
        .map_err(|_detail| SstDecodeError::InvalidIdentity)?;
    let fresh = FreshIdentity::new(birth, crate::AttemptId::new(owner, counter), ordinal)
        .map_err(|_detail| SstDecodeError::InvalidIdentity)?;
    let object = TableObjectId::new(fresh, store);
    if &object != expected_object {
        return Err(SstDecodeError::IdentityMismatch);
    }
    let rows = bytes
        .get(SST_HEADER_BYTES..)
        .ok_or(SstDecodeError::HeaderTooShort {
            bytes_actual: bytes.len(),
        })?;
    Ok((
        SstHeader {
            partition,
            object,
            row_codec,
        },
        rows,
    ))
}

fn decode_expected_header<'bytes>(
    expected: &TableKey,
    codec: RowCodec,
    bytes: &'bytes [u8],
) -> Result<&'bytes [u8], SstDecodeError> {
    let (header, rows) = decode_sst_header(&expected.object(), bytes)?;
    if header.partition != expected.partition() {
        return Err(SstDecodeError::IdentityMismatch);
    }
    if header.row_codec != codec {
        return Err(SstDecodeError::StoreCodecMismatch);
    }
    if rows.is_empty() {
        return Err(SstDecodeError::EmptyTable);
    }
    Ok(rows)
}

fn begin_table(expected: &TableKey, codec: RowCodec) -> Result<Vec<u8>, SstEncodeError> {
    let header = SstHeader::new(expected.partition(), expected.object(), codec)?;
    Ok(encode_sst_header(header).to_vec())
}

fn finish_table(bytes: Vec<u8>) -> Result<Vec<u8>, SstEncodeError> {
    let bytes_actual =
        u64::try_from(bytes.len()).map_err(|_detail| SstEncodeError::EncodedBytesOverMax {
            bytes_actual: u64::MAX,
        })?;
    if bytes_actual > SST_OBJECT_BYTES_MAX {
        return Err(SstEncodeError::EncodedBytesOverMax { bytes_actual });
    }
    Ok(bytes)
}

fn enforce_object_bound(bytes: &[u8]) -> Result<(), SstDecodeError> {
    let bytes_actual =
        u64::try_from(bytes.len()).map_err(|_detail| SstDecodeError::EncodedBytesOverMax {
            bytes_actual: u64::MAX,
        })?;
    if bytes_actual > SST_OBJECT_BYTES_MAX {
        return Err(SstDecodeError::EncodedBytesOverMax { bytes_actual });
    }
    Ok(())
}

const fn encode_store(store: StoreKind) -> u8 {
    match store {
        StoreKind::Directory => 1,
        StoreKind::Ledger => 2,
        StoreKind::Tally => 3,
        StoreKind::Annals => 4,
    }
}

const fn decode_store(tag: u8) -> Result<StoreKind, SstDecodeError> {
    match tag {
        1 => Ok(StoreKind::Directory),
        2 => Ok(StoreKind::Ledger),
        3 => Ok(StoreKind::Tally),
        4 => Ok(StoreKind::Annals),
        observed => Err(SstDecodeError::UnknownStore { observed }),
    }
}

const fn decode_row_codec(tag: u8) -> Result<RowCodec, SstDecodeError> {
    match tag {
        1 => Ok(RowCodec::DirectoryV1),
        2 => Ok(RowCodec::LedgerV1),
        observed => Err(SstDecodeError::UnknownRowCodec { observed }),
    }
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, SstDecodeError> {
    let end = offset
        .checked_add(size_of::<u64>())
        .ok_or(SstDecodeError::RowCountOverflow)?;
    let raw = bytes
        .get(offset..end)
        .ok_or(SstDecodeError::HeaderTooShort {
            bytes_actual: bytes.len(),
        })?
        .try_into()
        .map_err(|_detail| SstDecodeError::HeaderTooShort {
            bytes_actual: bytes.len(),
        })?;
    Ok(u64::from_be_bytes(raw))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, SstDecodeError> {
    let end = offset
        .checked_add(size_of::<u32>())
        .ok_or(SstDecodeError::RowCountOverflow)?;
    let raw = bytes
        .get(offset..end)
        .ok_or(SstDecodeError::HeaderTooShort {
            bytes_actual: bytes.len(),
        })?
        .try_into()
        .map_err(|_detail| SstDecodeError::HeaderTooShort {
            bytes_actual: bytes.len(),
        })?;
    Ok(u32::from_be_bytes(raw))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowField {
    DirectoryShared,
    DirectorySuffixLength,
    DirectoryEntryTag,
    DirectoryUid,
    DirectorySuffix,
    LedgerUid,
    LedgerCellTag,
    LedgerValueLength,
    LedgerValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SstEncodeError {
    #[error("SST store and row codec do not match")]
    StoreCodecMismatch,
    #[error("SST must contain at least one row")]
    EmptyTable,
    #[error("SST rows are not strictly ordered")]
    RowsNotStrictlyOrdered,
    #[error("SST row count overflowed")]
    RowCountOverflow,
    #[error("SST row field does not fit its fixed-width length")]
    RowLengthOverflow,
    #[error("stream record encoding failed: {0}")]
    StreamRecord(#[source] crate::EncodeError),
    #[error("SST is {bytes_actual} bytes; the bound is {SST_OBJECT_BYTES_MAX}")]
    EncodedBytesOverMax { bytes_actual: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SstDecodeError {
    #[error("SST is {bytes_actual} bytes; the bound is {SST_OBJECT_BYTES_MAX}")]
    EncodedBytesOverMax { bytes_actual: u64 },
    #[error("SST header is {bytes_actual} bytes; {SST_HEADER_BYTES} are required")]
    HeaderTooShort { bytes_actual: usize },
    #[error("SST magic is {observed:?}; expected STRM")]
    MagicMismatch { observed: [u8; 4] },
    #[error("object kind is {observed}; expected SST tag 3")]
    ObjectKindMismatch { observed: u8 },
    #[error("SST version {observed} is unsupported")]
    UnsupportedVersion { observed: u8 },
    #[error("SST partition is nil")]
    InvalidPartition,
    #[error("SST store tag {observed} is unknown")]
    UnknownStore { observed: u8 },
    #[error("SST row codec tag {observed} is unknown")]
    UnknownRowCodec { observed: u8 },
    #[error("SST store and row codec do not match")]
    StoreCodecMismatch,
    #[error("SST fresh identity violates its invariants")]
    InvalidIdentity,
    #[error("SST header identity differs from the durable object key")]
    IdentityMismatch,
    #[error("SST contains no rows")]
    EmptyTable,
    #[error("SST ended inside {field:?}")]
    UnexpectedEof { field: RowField },
    #[error("Directory shared prefix exceeds the previous key")]
    PrefixPastPreviousKey,
    #[error("Directory key is empty or over its byte bound")]
    DirectoryKeyLength,
    #[error("Directory key is not canonical")]
    InvalidDirectoryKey,
    #[error("Directory prefix is not the longest common prefix")]
    NonLongestPrefix,
    #[error("SST rows are not strictly ordered")]
    RowsNotStrictlyOrdered,
    #[error("Directory entry tag {observed} is unknown")]
    UnknownDirectoryTag { observed: u8 },
    #[error("Ledger cell tag {observed} is unknown")]
    UnknownLedgerTag { observed: u8 },
    #[error("stream UID is zero")]
    ZeroUid,
    #[error("Ledger value length is zero or over its byte bound")]
    LedgerValueLength,
    #[error("Ledger StreamRecord is invalid: {0}")]
    StreamRecord(#[source] DecodeError),
    #[error("Ledger StreamRecord does not have its canonical byte spelling")]
    NonCanonicalStreamRecord,
    #[error("SST row count overflowed")]
    RowCountOverflow,
}

struct Cursor {
    offset: usize,
}

impl Cursor {
    const fn new() -> Self {
        Self { offset: 0 }
    }

    const fn is_empty(&self, bytes: &[u8]) -> bool {
        self.offset == bytes.len()
    }

    fn take<'bytes>(
        &mut self,
        bytes: &'bytes [u8],
        length: usize,
        field: RowField,
    ) -> Result<&'bytes [u8], SstDecodeError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(SstDecodeError::UnexpectedEof { field })?;
        let field_bytes = bytes
            .get(self.offset..end)
            .ok_or(SstDecodeError::UnexpectedEof { field })?;
        self.offset = end;
        Ok(field_bytes)
    }

    fn u8(&mut self, bytes: &[u8], field: RowField) -> Result<u8, SstDecodeError> {
        self.take(bytes, 1, field)?
            .first()
            .copied()
            .ok_or(SstDecodeError::UnexpectedEof { field })
    }

    fn u16(&mut self, bytes: &[u8], field: RowField) -> Result<u16, SstDecodeError> {
        let raw = self
            .take(bytes, size_of::<u16>(), field)?
            .try_into()
            .map_err(|_detail| SstDecodeError::UnexpectedEof { field })?;
        Ok(u16::from_be_bytes(raw))
    }

    fn u64(&mut self, bytes: &[u8], field: RowField) -> Result<u64, SstDecodeError> {
        let raw = self
            .take(bytes, size_of::<u64>(), field)?
            .try_into()
            .map_err(|_detail| SstDecodeError::UnexpectedEof { field })?;
        Ok(u64::from_be_bytes(raw))
    }
}
