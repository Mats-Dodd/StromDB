//! Directory SST archive root.

use rkyv::rancor::Failure;

use super::{SstDecodeError, SstEncodeError, check_decode_bound, check_encode_rows};
use crate::archive;
use crate::bounds::{
    DIRECTORY_KEY_BYTES_MAX, DIRECTORY_ROW_LOGICAL_BYTES_MAX, PARTITION_PATH_OCCUPANCIES_MAX_V2,
    PARTITION_RESIDENT_LOGICAL_BYTES_MAX_V2, SST_OBJECT_BYTES_MAX_USIZE,
};
use crate::{DirectoryEntry, DirectoryKey, FreshIdentity, PartitionId, StoreKind, TableKey};

#[derive(Debug, rkyv::Archive, rkyv::Serialize)]
#[rkyv(bytecheck(bounds(__C: rkyv::validation::ArchiveContext)))]
// This encoding-only root borrows rows only for one synchronous archive call.
// ast-grep-ignore: types-own-their-data
struct DirectorySstArchive<'rows> {
    partition: PartitionId,
    fresh: FreshIdentity,
    #[rkyv(with = rkyv::with::InlineAsBox)]
    rows: &'rows [(DirectoryKey, DirectoryEntry)],
}

/// Encodes one non-empty, strictly ordered Directory table.
///
/// # Errors
///
/// Returns [`SstEncodeError`] when the key names another store, the row set is
/// empty or unordered, serialization fails, or the complete object is over-bound.
pub fn encode_directory_sst(
    expected: &TableKey,
    rows: &[(DirectoryKey, DirectoryEntry)],
) -> Result<Vec<u8>, SstEncodeError> {
    if expected.object().store() != StoreKind::Directory {
        return Err(SstEncodeError::StoreMismatch);
    }
    if rows.is_empty() {
        return Err(SstEncodeError::EmptyTable);
    }
    check_encode_rows::<(DirectoryKey, DirectoryEntry)>(rows.len())?;
    let mut previous = None;
    for (key, _entry) in rows {
        if previous.is_some_and(|previous_key| key.as_bytes() <= previous_key) {
            return Err(SstEncodeError::RowsNotStrictlyOrdered);
        }
        previous = Some(key.as_bytes());
    }

    let root = DirectorySstArchive {
        partition: expected.partition(),
        fresh: expected.object().fresh(),
        rows,
    };
    archive::encode(&root, SST_OBJECT_BYTES_MAX_USIZE).map_err(SstEncodeError::from)
}

/// Decodes a complete Directory SST into an all-or-nothing owned row set.
///
/// # Errors
///
/// Returns [`SstDecodeError`] when the byte, structure, identity, resource, or
/// row-domain gates fail.
pub fn decode_directory_sst(
    expected: &TableKey,
    bytes: &[u8],
) -> Result<Vec<(DirectoryKey, DirectoryEntry)>, SstDecodeError> {
    check_decode_bound(bytes)?;
    if expected.object().store() != StoreKind::Directory {
        return Err(SstDecodeError::StoreMismatch);
    }
    let root = rkyv::access::<ArchivedDirectorySstArchive<'_>, Failure>(bytes)
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
        let key = row.0.as_bytes();
        if key.is_empty() || key.len() > DIRECTORY_KEY_BYTES_MAX {
            return Err(SstDecodeError::InvalidBody);
        }
        if previous.is_some_and(|previous_key| key <= previous_key) {
            return Err(SstDecodeError::InvalidBody);
        }
        row.0
            .validated_bytes()
            .map_err(|_domain_error| SstDecodeError::InvalidBody)?;
        resident_bytes = resident_bytes
            .checked_add(DIRECTORY_ROW_LOGICAL_BYTES_MAX)
            .ok_or(SstDecodeError::ResourceBound)?;
        if resident_bytes > PARTITION_RESIDENT_LOGICAL_BYTES_MAX_V2 {
            return Err(SstDecodeError::ResourceBound);
        }
        previous = Some(key);
    }

    let mut rows = Vec::new();
    rows.try_reserve_exact(root.rows.len())
        .map_err(|_allocation_error| SstDecodeError::ResourceBound)?;
    for row in root.rows.iter() {
        rows.push((
            DirectoryKey::try_from(&row.0).map_err(|_domain_error| SstDecodeError::InvalidBody)?,
            DirectoryEntry::from(&row.1),
        ));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AttemptId, SealGeneration, StreamUid, TableObjectId};

    #[test]
    fn decoder_rejects_a_structurally_valid_duplicate_key() -> Result<(), Box<dyn std::error::Error>>
    {
        let expected = table_key()?;
        let key = "events/a"
            .parse::<strom_domain::StreamId>()
            .map(|stream_id| DirectoryKey::from(&stream_id))?;
        let rows = [
            (key.clone(), DirectoryEntry::Live(StreamUid::try_from(1)?)),
            (key, DirectoryEntry::Tombstone(StreamUid::try_from(2)?)),
        ];
        let root = DirectorySstArchive {
            partition: expected.partition(),
            fresh: expected.object().fresh(),
            rows: &rows,
        };
        let bytes = archive::encode(&root, SST_OBJECT_BYTES_MAX_USIZE)?;
        assert_eq!(
            decode_directory_sst(&expected, &bytes),
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
            TableObjectId::new(fresh, StoreKind::Directory),
        ))
    }
}
