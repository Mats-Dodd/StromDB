//! Checked rkyv archives for sorted-string tables.

mod directory;
mod ledger;

use rkyv::{Archive, Archived};

use crate::archive::EncodeError;
use crate::bounds::{
    PARTITION_PATH_OCCUPANCIES_MAX_V2, SST_OBJECT_BYTES_MAX, SST_OBJECT_BYTES_MAX_USIZE,
};

pub use directory::{decode_directory_sst, encode_directory_sst};
pub use ledger::{decode_ledger_sst, encode_ledger_sst};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SstEncodeError {
    #[error("table key names the wrong store for this SST root")]
    StoreMismatch,
    #[error("SST must contain at least one row")]
    EmptyTable,
    #[error("SST rows are not strictly ordered")]
    RowsNotStrictlyOrdered,
    #[error("SST could not be archived")]
    Serialization,
    #[error("SST exceeds the {SST_OBJECT_BYTES_MAX}-byte bound")]
    EncodedBytesOverMax,
}

impl From<EncodeError> for SstEncodeError {
    fn from(error: EncodeError) -> Self {
        match error {
            EncodeError::Serialization => Self::Serialization,
            EncodeError::EncodedBytesOverMax { bytes_max: _ } => Self::EncodedBytesOverMax,
        }
    }
}

pub(super) fn check_encode_rows<Row: Archive>(rows_len: usize) -> Result<(), SstEncodeError> {
    let rows_len_u64 = u64::try_from(rows_len).unwrap_or(u64::MAX);
    let archived_rows_bytes = rows_len.saturating_mul(size_of::<Archived<Row>>());
    if rows_len_u64 > PARTITION_PATH_OCCUPANCIES_MAX_V2
        || archived_rows_bytes > SST_OBJECT_BYTES_MAX_USIZE
    {
        return Err(SstEncodeError::EncodedBytesOverMax);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SstDecodeError {
    #[error("SST is {bytes_actual} bytes; the bound is {SST_OBJECT_BYTES_MAX}")]
    EncodedBytesOverMax { bytes_actual: u64 },
    #[error("SST archive is structurally malformed")]
    MalformedArchive,
    #[error("SST body violates a domain invariant")]
    InvalidBody,
    #[error("SST body identity differs from the durable table key")]
    IdentityMismatch,
    #[error("table key names the wrong store for this SST root")]
    StoreMismatch,
    #[error("SST exceeds a decoded resource bound")]
    ResourceBound,
}

pub(super) fn check_decode_bound(bytes: &[u8]) -> Result<(), SstDecodeError> {
    crate::archive::decode_bound(bytes, SST_OBJECT_BYTES_MAX_USIZE).map_err(|error| {
        SstDecodeError::EncodedBytesOverMax {
            bytes_actual: u64::try_from(error.bytes_actual).unwrap_or(u64::MAX),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn impossible_row_counts_fail_before_input_iteration() {
        assert_eq!(
            check_encode_rows::<u64>(usize::MAX),
            Err(SstEncodeError::EncodedBytesOverMax)
        );
    }
}
