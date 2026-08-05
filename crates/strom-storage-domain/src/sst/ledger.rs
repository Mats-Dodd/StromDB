//! Sequential Ledger SST rows.

use super::{
    Cursor, RowCodec, RowField, SstDecodeError, SstEncodeError, begin_table,
    decode_expected_header, finish_table,
};
use crate::bounds::STREAM_RECORD_BYTES_MAX;
use crate::{LedgerCell, StreamUid, TableKey, decode_stream_record, encode_stream_record};

/// Encodes one non-empty, strictly UID-ordered Ledger table.
///
/// # Errors
///
/// Returns [`SstEncodeError`] when the object store is not Ledger, rows are
/// empty or not strictly ordered, a record cannot be encoded, a count
/// overflows, or the object is over-bound.
pub fn encode_ledger_sst(
    expected: &TableKey,
    rows: &[(StreamUid, LedgerCell)],
) -> Result<Vec<u8>, SstEncodeError> {
    if rows.is_empty() {
        return Err(SstEncodeError::EmptyTable);
    }
    let mut bytes = begin_table(expected, RowCodec::LedgerV1)?;
    let mut previous = None;
    let mut row_count = 0usize;
    for (uid, cell) in rows {
        if previous.is_some_and(|previous_uid| *uid <= previous_uid) {
            return Err(SstEncodeError::RowsNotStrictlyOrdered);
        }
        bytes.extend_from_slice(&uid.encoded().to_be_bytes());
        match cell {
            LedgerCell::Value(record) => {
                bytes.push(1);
                let value = encode_stream_record(record).map_err(SstEncodeError::StreamRecord)?;
                let value_length = u16::try_from(value.len())
                    .map_err(|_detail| SstEncodeError::RowLengthOverflow)?;
                bytes.extend_from_slice(&value_length.to_be_bytes());
                bytes.extend_from_slice(&value);
            }
            LedgerCell::Delete => bytes.push(2),
        }
        previous = Some(*uid);
        row_count = row_count
            .checked_add(1)
            .ok_or(SstEncodeError::RowCountOverflow)?;
    }
    finish_table(bytes)
}

/// Decodes a complete Ledger SST into an all-or-nothing owned row set.
///
/// # Errors
///
/// Returns [`SstDecodeError`] when any header or row gate fails. No decoded
/// prefix is returned when a later row is malformed.
pub fn decode_ledger_sst(
    expected: &TableKey,
    bytes: &[u8],
) -> Result<Vec<(StreamUid, LedgerCell)>, SstDecodeError> {
    let body = decode_expected_header(expected, RowCodec::LedgerV1, bytes)?;
    let mut cursor = Cursor::new();
    let mut rows = Vec::new();
    let mut previous = None;
    let mut row_count = 0usize;
    while !cursor.is_empty(body) {
        let uid = cursor.u64(body, RowField::LedgerUid)?;
        let tag = cursor.u8(body, RowField::LedgerCellTag)?;
        let uid = StreamUid::try_from(uid).map_err(|_detail| SstDecodeError::ZeroUid)?;
        if previous.is_some_and(|previous_uid| uid <= previous_uid) {
            return Err(SstDecodeError::RowsNotStrictlyOrdered);
        }
        let cell = match tag {
            1 => {
                let value_length = usize::from(cursor.u16(body, RowField::LedgerValueLength)?);
                if value_length == 0 || value_length > STREAM_RECORD_BYTES_MAX {
                    return Err(SstDecodeError::LedgerValueLength);
                }
                let value = cursor.take(body, value_length, RowField::LedgerValue)?;
                let record = decode_stream_record(value).map_err(SstDecodeError::StreamRecord)?;
                let canonical = encode_stream_record(&record)
                    .map_err(|_detail| SstDecodeError::NonCanonicalStreamRecord)?;
                if canonical != value {
                    return Err(SstDecodeError::NonCanonicalStreamRecord);
                }
                LedgerCell::Value(record)
            }
            2 => LedgerCell::Delete,
            observed => return Err(SstDecodeError::UnknownLedgerTag { observed }),
        };
        rows.push((uid, cell));
        previous = Some(uid);
        row_count = row_count
            .checked_add(1)
            .ok_or(SstDecodeError::RowCountOverflow)?;
    }
    if rows.len() != row_count {
        return Err(SstDecodeError::RowCountOverflow);
    }
    Ok(rows)
}
