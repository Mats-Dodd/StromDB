//! Checked rkyv codec for WAL objects.

use rkyv::rancor::Failure;

use super::fact::ArchivedOperationFact;
use super::{
    ArchivedWalFence, ArchivedWalObject, ArchivedWalRun, BatchId, BoundedNonEmptyVec,
    OperationFact, WalFence, WalIdentity, WalObject, WalRun,
};
use crate::archive::{DecodeError, EncodeError, decode_bound, decode_content_type, encode};
use crate::bounds::{WAL_ENCODED_BYTES_MAX, WAL_RUN_FACTS_MAX};
use crate::{DirectoryKey, OwnerToken, PartitionId, StreamUid};

/// # Errors
///
/// Returns [`EncodeError`] when archiving fails or the complete object exceeds
/// [`WAL_ENCODED_BYTES_MAX`].
pub fn encode_wal(object: &WalObject) -> Result<Vec<u8>, EncodeError> {
    encode(object, WAL_ENCODED_BYTES_MAX)
}

/// # Errors
///
/// Returns [`DecodeError`] when the complete byte bound, archive structure,
/// resource bounds, domain reconstruction, or durable identity check fails.
pub fn decode_wal(identity: &WalIdentity, bytes: &[u8]) -> Result<WalObject, DecodeError> {
    decode_bound(bytes, WAL_ENCODED_BYTES_MAX)?;
    let archived = rkyv::access::<ArchivedWalObject, Failure>(bytes)
        .map_err(|_archive_error| DecodeError::MalformedArchive)?;
    match archived {
        ArchivedWalObject::Run(run) => decode_run(identity, run).map(WalObject::Run),
        ArchivedWalObject::Fence(fence) => decode_fence(identity, fence).map(WalObject::Fence),
    }
}

fn decode_run(identity: &WalIdentity, run: &ArchivedWalRun) -> Result<WalRun, DecodeError> {
    let partition = PartitionId::try_from(&run.partition)?;
    let batch = BatchId::from(&run.batch);
    check_identity(identity, partition, batch)?;

    let owner = OwnerToken::from(&run.owner);
    let facts_archived = run.facts.values.as_slice();
    let facts_count = facts_archived.len();
    if facts_count == 0 || facts_count > WAL_RUN_FACTS_MAX {
        return Err(DecodeError::InvalidBody);
    }

    let mut facts = Vec::with_capacity(facts_count);
    for fact in facts_archived {
        facts.push(decode_fact(fact)?);
    }
    let facts =
        BoundedNonEmptyVec::try_from(facts).map_err(|_domain_error| DecodeError::InvalidBody)?;
    Ok(WalRun::new(partition, batch, owner, facts))
}

fn decode_fence(identity: &WalIdentity, fence: &ArchivedWalFence) -> Result<WalFence, DecodeError> {
    let partition = PartitionId::try_from(&fence.partition)?;
    let batch = BatchId::from(&fence.batch);
    check_identity(identity, partition, batch)?;
    let owner = OwnerToken::from(&fence.owner);
    Ok(WalFence::new(partition, batch, owner))
}

fn decode_fact(fact: &ArchivedOperationFact) -> Result<OperationFact, DecodeError> {
    match fact {
        ArchivedOperationFact::StreamCreated {
            path,
            uid,
            content_type,
            expiry,
        } => Ok(OperationFact::StreamCreated {
            path: DirectoryKey::try_from(path)?,
            uid: StreamUid::from(uid),
            content_type: decode_content_type(content_type)?,
            expiry: strom_domain::ExpiryPolicy::try_from(expiry)?,
        }),
        ArchivedOperationFact::StreamClosed { path, uid } => Ok(OperationFact::StreamClosed {
            path: DirectoryKey::try_from(path)?,
            uid: StreamUid::from(uid),
        }),
        ArchivedOperationFact::StreamDeleted { path, uid } => Ok(OperationFact::StreamDeleted {
            path: DirectoryKey::try_from(path)?,
            uid: StreamUid::from(uid),
        }),
    }
}

fn check_identity(
    expected: &WalIdentity,
    partition: PartitionId,
    batch: BatchId,
) -> Result<(), DecodeError> {
    if WalIdentity::new(partition, batch) != *expected {
        return Err(DecodeError::IdentityMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SealGeneration;

    #[test]
    fn fact_count_is_gated_before_result_materialization() -> Result<(), Box<dyn std::error::Error>>
    {
        let partition = partition()?;
        let identity = WalIdentity::new(partition, BatchId::try_from(1)?);
        let owner = OwnerToken::from(SealGeneration::genesis());

        let empty = WalObject::Run(WalRun {
            partition,
            batch: identity.batch(),
            owner,
            facts: BoundedNonEmptyVec { values: Vec::new() },
        });
        assert_eq!(
            decode_wal(&identity, &encode(&empty, WAL_ENCODED_BYTES_MAX)?),
            Err(DecodeError::InvalidBody),
            "a structurally valid archive cannot manufacture an empty run"
        );

        let at_max = run_with_deleted_facts(partition, identity.batch(), owner, WAL_RUN_FACTS_MAX)?;
        let decoded = decode_wal(&identity, &encode_wal(&at_max)?)?;
        let WalObject::Run(decoded) = decoded else {
            return Err("a run archive must decode as a run".into());
        };
        assert_eq!(decoded.facts().len(), WAL_RUN_FACTS_MAX);

        let over_max = run_with_deleted_facts(
            partition,
            identity.batch(),
            owner,
            WAL_RUN_FACTS_MAX.saturating_add(1),
        )?;
        assert_eq!(
            decode_wal(&identity, &encode(&over_max, WAL_ENCODED_BYTES_MAX)?,),
            Err(DecodeError::InvalidBody),
            "an archived count above the named limit is rejected before result allocation"
        );
        Ok(())
    }

    fn run_with_deleted_facts(
        partition: PartitionId,
        batch: BatchId,
        owner: OwnerToken,
        facts_count: usize,
    ) -> Result<WalObject, Box<dyn std::error::Error>> {
        let uid = StreamUid::try_from(1)?;
        let path = "events/abc".parse::<strom_domain::StreamId>()?;
        let values = (0..facts_count)
            .map(|_ordinal| OperationFact::StreamDeleted {
                path: DirectoryKey::from(&path),
                uid,
            })
            .collect();
        Ok(WalObject::Run(WalRun {
            partition,
            batch,
            owner,
            facts: BoundedNonEmptyVec { values },
        }))
    }

    fn partition() -> Result<PartitionId, crate::PartitionIdError> {
        "00112233-4455-6677-8899-aabbccddeeff".parse()
    }
}
