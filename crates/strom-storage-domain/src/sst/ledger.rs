//! Ledger SST archive root.

use rkyv::rancor::Failure;

use super::{SstDecodeError, SstEncodeError, check_decode_bound, check_encode_rows};
use crate::archive;
use crate::bounds::{
    LEDGER_DELETE_ROW_LOGICAL_BYTES, LEDGER_VALUE_ROW_LOGICAL_BYTES_MAX,
    PARTITION_PATH_OCCUPANCIES_MAX_V2, PARTITION_RESIDENT_LOGICAL_BYTES_MAX_V2,
    SST_OBJECT_BYTES_MAX_USIZE,
};
use crate::{FreshIdentity, LedgerCell, PartitionId, StoreKind, StreamUid, TableKey};

#[derive(Debug, rkyv::Archive, rkyv::Serialize)]
#[rkyv(bytecheck(bounds(__C: rkyv::validation::ArchiveContext)))]
// This encoding-only root borrows rows only for one synchronous archive call.
// ast-grep-ignore: types-own-their-data
struct LedgerSstArchive<'rows> {
    partition: PartitionId,
    fresh: FreshIdentity,
    #[rkyv(with = rkyv::with::InlineAsBox)]
    rows: &'rows [(StreamUid, LedgerCell)],
}

/// Encodes one non-empty, strictly UID-ordered Ledger table.
///
/// # Errors
///
/// Returns [`SstEncodeError`] when the key names another store, the row set is
/// empty or unordered, serialization fails, or the complete object is over-bound.
pub fn encode_ledger_sst(
    expected: &TableKey,
    rows: &[(StreamUid, LedgerCell)],
) -> Result<Vec<u8>, SstEncodeError> {
    if expected.object().store() != StoreKind::Ledger {
        return Err(SstEncodeError::StoreMismatch);
    }
    if rows.is_empty() {
        return Err(SstEncodeError::EmptyTable);
    }
    check_encode_rows::<(StreamUid, LedgerCell)>(rows.len())?;
    let mut previous = None;
    for (uid, _cell) in rows {
        if previous.is_some_and(|previous_uid| *uid <= previous_uid) {
            return Err(SstEncodeError::RowsNotStrictlyOrdered);
        }
        previous = Some(*uid);
    }

    let root = LedgerSstArchive {
        partition: expected.partition(),
        fresh: expected.object().fresh(),
        rows,
    };
    archive::encode(&root, SST_OBJECT_BYTES_MAX_USIZE).map_err(SstEncodeError::from)
}

/// Decodes a complete Ledger SST into an all-or-nothing owned row set.
///
/// # Errors
///
/// Returns [`SstDecodeError`] when the byte, structure, identity, resource, or
/// row-domain gates fail.
pub fn decode_ledger_sst(
    expected: &TableKey,
    bytes: &[u8],
) -> Result<Vec<(StreamUid, LedgerCell)>, SstDecodeError> {
    check_decode_bound(bytes)?;
    if expected.object().store() != StoreKind::Ledger {
        return Err(SstDecodeError::StoreMismatch);
    }
    let root = rkyv::access::<ArchivedLedgerSstArchive<'_>, Failure>(bytes)
        .map_err(|_archive_error| SstDecodeError::MalformedArchive)?;
    let partition = PartitionId::try_from(&root.partition)
        .map_err(|_domain_error| SstDecodeError::InvalidBody)?;
    let fresh = FreshIdentity::try_from(&root.fresh)
        .map_err(|_domain_error| SstDecodeError::InvalidBody)?;
    if partition != expected.partition() || fresh != expected.object().fresh() {
        return Err(SstDecodeError::IdentityMismatch);
    }
    if root.rows.is_empty() {
        return Err(SstDecodeError::InvalidBody);
    }
    if u64::try_from(root.rows.len()).unwrap_or(u64::MAX) > PARTITION_PATH_OCCUPANCIES_MAX_V2 {
        return Err(SstDecodeError::ResourceBound);
    }

    let mut rows = Vec::new();
    rows.try_reserve_exact(root.rows.len())
        .map_err(|_allocation_error| SstDecodeError::ResourceBound)?;
    let mut previous = None;
    let mut resident_bytes = 0u64;
    for row in root.rows.iter() {
        let uid = StreamUid::from(&row.0);
        if previous.is_some_and(|previous_uid| uid <= previous_uid) {
            return Err(SstDecodeError::InvalidBody);
        }
        let row_bytes = if row.1.is_value() {
            LEDGER_VALUE_ROW_LOGICAL_BYTES_MAX
        } else {
            LEDGER_DELETE_ROW_LOGICAL_BYTES
        };
        resident_bytes = resident_bytes
            .checked_add(row_bytes)
            .ok_or(SstDecodeError::ResourceBound)?;
        if resident_bytes > PARTITION_RESIDENT_LOGICAL_BYTES_MAX_V2 {
            return Err(SstDecodeError::ResourceBound);
        }
        rows.push((
            uid,
            LedgerCell::try_from(&row.1).map_err(|_domain_error| SstDecodeError::InvalidBody)?,
        ));
        previous = Some(uid);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use strom_domain::{CONTENT_TYPE_BYTES_MAX, ExpiryPolicy, StreamContentType, StreamLifecycle};

    use super::*;
    use crate::{
        AttemptId, BatchId, LEDGER_DELETE_ROW_ENCODED_BYTES_MAX,
        LEDGER_VALUE_ROW_ENCODED_BYTES_MAX, SST_ARCHIVE_FIXED_BYTES_MAX, SealGeneration,
        StreamRecord, TableObjectId,
    };

    #[test]
    fn encoded_ledger_bounds_dominate_maximum_identity_and_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let expected = table_key()?;
        let empty = LedgerSstArchive {
            partition: expected.partition(),
            fresh: expected.object().fresh(),
            rows: &[],
        };
        let empty_bytes = archive::encode(&empty, SST_OBJECT_BYTES_MAX_USIZE)?;
        assert!(
            u64::try_from(empty_bytes.len())? <= SST_ARCHIVE_FIXED_BYTES_MAX,
            "fixed accounting dominates empty archive framing and identity"
        );

        let content_type: StreamContentType =
            format!("a/{}", "b".repeat(CONTENT_TYPE_BYTES_MAX - 2)).parse()?;
        let value = [(
            StreamUid::try_from(u64::MAX)?,
            LedgerCell::Value(StreamRecord::new(
                content_type,
                ExpiryPolicy::None,
                StreamLifecycle::Closed,
                BatchId::try_from(u64::MAX)?,
            )),
        )];
        let value_bytes = encode_ledger_sst(&expected, &value)?;
        assert!(
            u64::try_from(value_bytes.len())?
                <= SST_ARCHIVE_FIXED_BYTES_MAX + LEDGER_VALUE_ROW_ENCODED_BYTES_MAX,
            "Ledger value accounting dominates the maximum encoded fixture"
        );

        let delete = [(StreamUid::try_from(u64::MAX)?, LedgerCell::Delete)];
        let delete_bytes = encode_ledger_sst(&expected, &delete)?;
        assert!(
            u64::try_from(delete_bytes.len())?
                <= SST_ARCHIVE_FIXED_BYTES_MAX + LEDGER_DELETE_ROW_ENCODED_BYTES_MAX,
            "Ledger delete accounting dominates the maximum encoded fixture"
        );
        Ok(())
    }

    #[test]
    fn decoder_rejects_a_structurally_valid_descending_uid()
    -> Result<(), Box<dyn std::error::Error>> {
        let expected = table_key()?;
        let rows = [
            (StreamUid::try_from(2)?, LedgerCell::Delete),
            (StreamUid::try_from(1)?, LedgerCell::Delete),
        ];
        let root = LedgerSstArchive {
            partition: expected.partition(),
            fresh: expected.object().fresh(),
            rows: &rows,
        };
        let bytes = archive::encode(&root, SST_OBJECT_BYTES_MAX_USIZE)?;
        assert_eq!(
            Err(SstDecodeError::InvalidBody),
            decode_ledger_sst(&expected, &bytes)
        );
        Ok(())
    }

    fn table_key() -> Result<TableKey, Box<dyn std::error::Error>> {
        let fresh = FreshIdentity::new(
            SealGeneration::try_from(2)?,
            AttemptId::new(SealGeneration::genesis(), 4),
            0,
        )?;
        Ok(TableKey::new(
            "00112233-4455-6677-8899-aabbccddeeff".parse()?,
            TableObjectId::new(fresh, StoreKind::Ledger),
        ))
    }
}
