//! Ledger SST archive root.

use rkyv::rancor::Failure;

use super::{
    SST_OBJECT_BYTES_MAX_USIZE, SstDecodeError, SstEncodeError, check_decode_bound,
    check_encode_rows,
};
use crate::archive;
use crate::bounds::{
    LEDGER_DELETE_ROW_LOGICAL_BYTES, LEDGER_VALUE_ROW_LOGICAL_BYTES_MAX,
    PARTITION_PATH_OCCUPANCIES_MAX_V2, PARTITION_RESIDENT_LOGICAL_BYTES_MAX_V2,
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
    check_encode_rows::<(StreamUid, LedgerCell)>(rows.len())?;
    let mut previous = None;
    for (uid, _cell) in rows {
        if previous.is_some_and(|previous_uid| *uid <= previous_uid) {
            return Err(SstEncodeError::RowsNotStrictlyOrdered);
        }
        previous = Some(*uid);
    }
    if rows.is_empty() {
        return Err(SstEncodeError::EmptyTable);
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

    let mut previous = None;
    let mut resident_bytes = 0u64;
    for row in root.rows.iter() {
        let uid = StreamUid::from(&row.0);
        if previous.is_some_and(|previous_uid| uid <= previous_uid) {
            return Err(SstDecodeError::InvalidBody);
        }
        row.1
            .validated()
            .map_err(|_domain_error| SstDecodeError::InvalidBody)?;
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
        previous = Some(uid);
    }

    let mut rows = Vec::new();
    rows.try_reserve_exact(root.rows.len())
        .map_err(|_allocation_error| SstDecodeError::ResourceBound)?;
    for row in root.rows.iter() {
        rows.push((
            StreamUid::from(&row.0),
            LedgerCell::try_from(&row.1).map_err(|_domain_error| SstDecodeError::InvalidBody)?,
        ));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AttemptId, SealGeneration, TableObjectId};

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
            decode_ledger_sst(&expected, &bytes),
            Err(SstDecodeError::InvalidBody)
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
