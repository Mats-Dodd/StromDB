//! Checked rkyv codec for permanent Seals.

use rkyv::rancor::Failure;

use super::{
    ArchivedKeyBound, ArchivedRangeVersion, ArchivedSeal, ArchivedSortedRun, ArchivedTableRef,
    ArchivedTreeVersion, ArchivedWalReplayPoint, KeyBound, RangeVersion, Seal, SealGeneration,
    SealIdentity, SortedRun, TableObjectId, TableRef, TreeVersion, WalReplayPoint,
};
use crate::archive::{DecodeError, EncodeError, decode_bound, encode};
use crate::bounds::{
    DIRECTORY_KEY_BYTES_MAX, RUN_TABLES_MAX, SEAL_ENCODED_BYTES_MAX, TREE_RANGES_MAX_V2,
    TREE_RUNS_MAX,
};
use crate::{BatchId, OwnerToken, PartitionId};

/// # Errors
///
/// Returns [`EncodeError`] when serialization fails or the complete archive
/// exceeds [`SEAL_ENCODED_BYTES_MAX`].
pub fn encode_seal(seal: &Seal) -> Result<Vec<u8>, EncodeError> {
    encode(seal, SEAL_ENCODED_BYTES_MAX)
}

/// # Errors
///
/// Returns [`DecodeError`] when the archive is malformed, violates a Seal
/// invariant, exceeds its resource bounds, or disagrees with its location.
pub fn decode_seal(identity: &SealIdentity, bytes: &[u8]) -> Result<Seal, DecodeError> {
    decode_bound(bytes, SEAL_ENCODED_BYTES_MAX)?;
    let archived = rkyv::access::<ArchivedSeal, Failure>(bytes)
        .map_err(|_archive_error| DecodeError::MalformedArchive)?;
    let partition = PartitionId::try_from(&archived.partition)?;
    let generation = SealGeneration::from(&archived.generation);
    if SealIdentity::new(partition, generation) != *identity {
        return Err(DecodeError::IdentityMismatch);
    }
    decode_archived_seal(archived, partition, generation)
}

fn decode_archived_seal(
    archived: &ArchivedSeal,
    partition: PartitionId,
    generation: SealGeneration,
) -> Result<Seal, DecodeError> {
    Seal::new(
        partition,
        generation,
        decode_replay(&archived.replay),
        decode_tree(&archived.directory)?,
        decode_tree(&archived.ledger)?,
        decode_tree(&archived.tally)?,
        decode_tree(&archived.annals)?,
    )
    .map_err(|_domain_error| DecodeError::InvalidBody)
}

fn decode_replay(archived: &ArchivedWalReplayPoint) -> WalReplayPoint {
    match archived {
        ArchivedWalReplayPoint::Genesis => WalReplayPoint::Genesis,
        ArchivedWalReplayPoint::Through { batch, owner } => WalReplayPoint::Through {
            batch: BatchId::from(batch),
            owner: OwnerToken::from(owner),
        },
    }
}

fn decode_tree(archived: &ArchivedTreeVersion) -> Result<TreeVersion, DecodeError> {
    if archived.ranges.len() != TREE_RANGES_MAX_V2 {
        return Err(DecodeError::InvalidBody);
    }
    let mut ranges = Vec::with_capacity(archived.ranges.len());
    for range in archived.ranges.iter() {
        ranges.push(decode_range(range)?);
    }
    TreeVersion::try_from_ranges(ranges).map_err(|_domain_error| DecodeError::InvalidBody)
}

fn decode_range(archived: &ArchivedRangeVersion) -> Result<RangeVersion, DecodeError> {
    if archived.runs.len() > TREE_RUNS_MAX {
        return Err(DecodeError::InvalidBody);
    }
    let mut runs = Vec::with_capacity(archived.runs.len());
    for run in archived.runs.iter() {
        runs.push(decode_run(run)?);
    }
    RangeVersion::new(
        decode_key_bound(&archived.start)?,
        decode_key_bound(&archived.end)?,
        runs,
    )
    .map_err(|_domain_error| DecodeError::InvalidBody)
}

fn decode_key_bound(archived: &ArchivedKeyBound) -> Result<KeyBound, DecodeError> {
    match archived {
        ArchivedKeyBound::Minimum => Ok(KeyBound::Minimum),
        ArchivedKeyBound::Key(key) => {
            if key.len() > DIRECTORY_KEY_BYTES_MAX {
                return Err(DecodeError::InvalidBody);
            }
            Ok(KeyBound::key(key.as_ref()))
        }
        ArchivedKeyBound::Maximum => Ok(KeyBound::Maximum),
    }
}

fn decode_run(archived: &ArchivedSortedRun) -> Result<SortedRun, DecodeError> {
    if archived.tables.is_empty() || archived.tables.len() > RUN_TABLES_MAX {
        return Err(DecodeError::InvalidBody);
    }
    let mut tables = Vec::with_capacity(archived.tables.len());
    for table in archived.tables.iter() {
        tables.push(decode_table_ref(table)?);
    }
    SortedRun::try_from_tables(tables).map_err(|_domain_error| DecodeError::InvalidBody)
}

fn decode_table_ref(archived: &ArchivedTableRef) -> Result<TableRef, DecodeError> {
    TableRef::new(
        TableObjectId::try_from(&archived.object)?,
        archived.object_bytes.to_native(),
    )
    .map_err(|_domain_error| DecodeError::InvalidBody)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_access_accepts_a_misaligned_byte_slice() -> Result<(), Box<dyn std::error::Error>> {
        let seal = valid_seal()?;
        let encoded = encode_seal(&seal)?;
        let mut misaligned = Vec::with_capacity(encoded.len().saturating_add(1));
        misaligned.push(0);
        misaligned.extend_from_slice(&encoded);

        assert_eq!(
            decode_seal(
                &seal.identity(),
                misaligned.get(1..).ok_or("misaligned archive exists")?,
            ),
            Ok(seal)
        );
        Ok(())
    }

    #[test]
    fn every_truncation_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let seal = valid_seal()?;
        let encoded = encode_seal(&seal)?;
        for length in 0..encoded.len() {
            assert_eq!(
                decode_seal(
                    &seal.identity(),
                    encoded.get(..length).ok_or("truncated archive exists")?,
                ),
                Err(DecodeError::MalformedArchive),
                "truncation at byte {length} must fail closed"
            );
        }
        Ok(())
    }

    #[test]
    fn byte_bound_precedes_archive_access() -> Result<(), Box<dyn std::error::Error>> {
        let seal = valid_seal()?;
        let bytes = vec![0; SEAL_ENCODED_BYTES_MAX.saturating_add(1)];
        assert_eq!(
            decode_seal(&seal.identity(), &bytes),
            Err(DecodeError::EncodedBytesOverMax {
                bytes_max: SEAL_ENCODED_BYTES_MAX,
                bytes_actual: bytes.len(),
            })
        );
        Ok(())
    }

    #[test]
    fn durable_location_is_checked_before_manifest_reconstruction()
    -> Result<(), Box<dyn std::error::Error>> {
        let seal = valid_seal()?;
        let wrong_identity =
            SealIdentity::new(seal.identity().partition(), SealGeneration::try_from(2)?);
        assert_eq!(
            decode_seal(&wrong_identity, &encode_seal(&seal)?),
            Err(DecodeError::IdentityMismatch)
        );
        Ok(())
    }

    #[test]
    fn decode_rejects_an_invalid_tree_shape() -> Result<(), Box<dyn std::error::Error>> {
        let partition: PartitionId = "00112233-4455-6677-8899-aabbccddeeff".parse()?;
        let seal = Seal {
            partition,
            generation: SealGeneration::genesis(),
            replay: WalReplayPoint::Genesis,
            directory: TreeVersion { ranges: Vec::new() },
            ledger: TreeVersion::empty(),
            tally: TreeVersion::empty(),
            annals: TreeVersion::empty(),
        };
        let bytes = encode_seal(&seal)?;

        assert_eq!(
            decode_seal(&seal.identity(), &bytes),
            Err(DecodeError::InvalidBody)
        );
        Ok(())
    }

    fn valid_seal() -> Result<Seal, Box<dyn std::error::Error>> {
        Ok(Seal::new(
            "00112233-4455-6677-8899-aabbccddeeff".parse()?,
            SealGeneration::genesis(),
            WalReplayPoint::Genesis,
            TreeVersion::empty(),
            TreeVersion::empty(),
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?)
    }
}
