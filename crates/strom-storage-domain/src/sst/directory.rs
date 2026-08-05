//! Sequential Directory SST rows.

use super::{
    Cursor, RowCodec, RowField, SstDecodeError, SstEncodeError, begin_table,
    decode_expected_header, finish_table,
};
use crate::bounds::DIRECTORY_KEY_BYTES_MAX;
use crate::{DirectoryEntry, DirectoryKey, StreamUid, TableKey};

const ROW_FIXED_BYTES: usize = 2 + 2 + 1 + 8;

/// Encodes one non-empty, strictly ordered Directory table.
///
/// # Errors
///
/// Returns [`SstEncodeError`] when the object store is not Directory, rows are
/// empty or not strictly ordered, a count overflows, or the object is over-bound.
pub fn encode_directory_sst(
    expected: &TableKey,
    rows: &[(DirectoryKey, DirectoryEntry)],
) -> Result<Vec<u8>, SstEncodeError> {
    if rows.is_empty() {
        return Err(SstEncodeError::EmptyTable);
    }
    let mut bytes = begin_table(expected, RowCodec::DirectoryV1)?;
    let mut previous: &[u8] = &[];
    let mut row_count = 0usize;
    for (key, entry) in rows {
        if !previous.is_empty() && key.as_bytes() <= previous {
            return Err(SstEncodeError::RowsNotStrictlyOrdered);
        }
        let shared = longest_common_prefix(previous, key.as_bytes());
        let suffix = key
            .as_bytes()
            .get(shared..)
            .ok_or(SstEncodeError::RowLengthOverflow)?;
        let shared = u16::try_from(shared).map_err(|_detail| SstEncodeError::RowLengthOverflow)?;
        let suffix_length =
            u16::try_from(suffix.len()).map_err(|_detail| SstEncodeError::RowLengthOverflow)?;
        bytes.extend_from_slice(&shared.to_be_bytes());
        bytes.extend_from_slice(&suffix_length.to_be_bytes());
        let (tag, uid) = match entry {
            DirectoryEntry::Live(uid) => (1, *uid),
            DirectoryEntry::Tombstone(uid) => (2, *uid),
        };
        bytes.push(tag);
        bytes.extend_from_slice(&uid.encoded().to_be_bytes());
        bytes.extend_from_slice(suffix);
        previous = key.as_bytes();
        row_count = row_count
            .checked_add(1)
            .ok_or(SstEncodeError::RowCountOverflow)?;
    }
    finish_table(bytes)
}

/// Decodes a complete Directory SST into an all-or-nothing owned row set.
///
/// # Errors
///
/// Returns [`SstDecodeError`] when any header or row gate fails. No decoded
/// prefix is returned when a later row is malformed.
pub fn decode_directory_sst(
    expected: &TableKey,
    bytes: &[u8],
) -> Result<Vec<(DirectoryKey, DirectoryEntry)>, SstDecodeError> {
    let body = decode_expected_header(expected, RowCodec::DirectoryV1, bytes)?;
    let mut cursor = Cursor::new();
    let mut rows = Vec::new();
    let mut previous = Vec::new();
    let mut row_count = 0usize;
    while !cursor.is_empty(body) {
        let shared = usize::from(cursor.u16(body, RowField::DirectoryShared)?);
        let suffix_length = usize::from(cursor.u16(body, RowField::DirectorySuffixLength)?);
        let tag = cursor.u8(body, RowField::DirectoryEntryTag)?;
        let uid = cursor.u64(body, RowField::DirectoryUid)?;
        let suffix = cursor.take(body, suffix_length, RowField::DirectorySuffix)?;
        if shared > previous.len() {
            return Err(SstDecodeError::PrefixPastPreviousKey);
        }
        let key_length = shared
            .checked_add(suffix_length)
            .ok_or(SstDecodeError::DirectoryKeyLength)?;
        if key_length == 0 || key_length > DIRECTORY_KEY_BYTES_MAX {
            return Err(SstDecodeError::DirectoryKeyLength);
        }
        let mut materialized = Vec::with_capacity(key_length);
        materialized.extend_from_slice(
            previous
                .get(..shared)
                .ok_or(SstDecodeError::PrefixPastPreviousKey)?,
        );
        materialized.extend_from_slice(suffix);
        if longest_common_prefix(&previous, &materialized) != shared {
            return Err(SstDecodeError::NonLongestPrefix);
        }
        if !previous.is_empty() && materialized <= previous {
            return Err(SstDecodeError::RowsNotStrictlyOrdered);
        }
        let key = DirectoryKey::try_from(materialized.clone().into_boxed_slice())
            .map_err(|_detail| SstDecodeError::InvalidDirectoryKey)?;
        let uid = StreamUid::try_from(uid).map_err(|_detail| SstDecodeError::ZeroUid)?;
        let entry = match tag {
            1 => DirectoryEntry::Live(uid),
            2 => DirectoryEntry::Tombstone(uid),
            observed => return Err(SstDecodeError::UnknownDirectoryTag { observed }),
        };
        rows.push((key, entry));
        previous = materialized;
        row_count = row_count
            .checked_add(1)
            .ok_or(SstDecodeError::RowCountOverflow)?;
    }
    if rows.len() != row_count {
        return Err(SstDecodeError::RowCountOverflow);
    }
    Ok(rows)
}

pub(super) fn longest_common_prefix(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left_byte, right_byte)| left_byte == right_byte)
        .count()
}

const _: () = assert!(
    ROW_FIXED_BYTES == 13,
    "Directory V1 rows have thirteen fixed bytes before the path suffix"
);
