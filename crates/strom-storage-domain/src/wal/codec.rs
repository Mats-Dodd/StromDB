//! Checked rkyv codec for WAL objects.

use rkyv::rancor::Failure;

use super::fact::ArchivedOperationFact;
use super::{ArchivedWalBody, ArchivedWalObject, OperationFact, WalBody, WalFacts, WalObject};
use crate::archive::{
    DecodeError, EncodeError, decode_bound, decode_content_type, decode_stream_path, encode,
};
use crate::bounds::{WAL_ENCODED_BYTES_MAX, WAL_RUN_FACTS_MAX};
use crate::{BatchId, OwnerToken, PartitionId, StreamUid};

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
pub fn decode_wal(
    expected_partition: PartitionId,
    expected_batch: BatchId,
    bytes: &[u8],
) -> Result<WalObject, DecodeError> {
    decode_bound(bytes, WAL_ENCODED_BYTES_MAX)?;
    let archived = rkyv::access::<ArchivedWalObject, Failure>(bytes)
        .map_err(|_archive_error| DecodeError::MalformedArchive)?;

    let partition = PartitionId::try_from(&archived.partition)?;
    let batch = BatchId::from(&archived.batch);
    if partition != expected_partition || batch != expected_batch {
        return Err(DecodeError::IdentityMismatch);
    }
    let owner = OwnerToken::from(&archived.owner);
    let body = decode_body(&archived.body)?;
    Ok(WalObject::new(partition, batch, owner, body))
}

fn decode_body(body: &ArchivedWalBody) -> Result<WalBody, DecodeError> {
    match body {
        ArchivedWalBody::Run(facts) => {
            let facts_archived = facts.facts.as_slice();
            let facts_count = facts_archived.len();
            if facts_count == 0 || facts_count > WAL_RUN_FACTS_MAX {
                return Err(DecodeError::InvalidBody);
            }

            let mut decoded_facts = Vec::with_capacity(facts_count);
            for fact in facts_archived {
                decoded_facts.push(decode_fact(fact)?);
            }
            let facts = WalFacts::try_from(decoded_facts)
                .map_err(|_domain_error| DecodeError::InvalidBody)?;
            Ok(WalBody::Run(facts))
        }
        ArchivedWalBody::Fence => Ok(WalBody::Fence),
    }
}

fn decode_fact(fact: &ArchivedOperationFact) -> Result<OperationFact, DecodeError> {
    match fact {
        ArchivedOperationFact::StreamCreated {
            path,
            uid,
            content_type,
            expiry,
            lifecycle,
        } => Ok(OperationFact::StreamCreated {
            path: decode_stream_path(path)?,
            uid: StreamUid::from(uid),
            content_type: decode_content_type(content_type)?,
            expiry: strom_domain::ExpiryPolicy::try_from(expiry)?,
            lifecycle: strom_domain::StreamLifecycle::from(lifecycle),
        }),
        ArchivedOperationFact::StreamClosed { path, uid } => Ok(OperationFact::StreamClosed {
            path: decode_stream_path(path)?,
            uid: StreamUid::from(uid),
        }),
        ArchivedOperationFact::StreamDeleted { path, uid } => Ok(OperationFact::StreamDeleted {
            path: decode_stream_path(path)?,
            uid: StreamUid::from(uid),
        }),
    }
}

#[cfg(test)]
mod tests {
    use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};

    use super::*;
    use crate::{
        SealGeneration, WAL_FACT_ENCODED_FIXED_BYTES_MAX, WAL_RUN_FIXED_ENCODED_BYTES_MAX,
    };

    #[test]
    fn fact_count_is_gated_before_result_materialization() -> Result<(), Box<dyn std::error::Error>>
    {
        let partition = partition()?;
        let batch = BatchId::try_from(1)?;
        let owner = OwnerToken::from(SealGeneration::genesis());

        let empty = WalObject::new(
            partition,
            batch,
            owner,
            WalBody::Run(WalFacts { facts: Vec::new() }),
        );
        assert_eq!(
            Err(DecodeError::InvalidBody),
            decode_wal(partition, batch, &encode(&empty, WAL_ENCODED_BYTES_MAX)?),
            "a structurally valid archive cannot manufacture an empty run"
        );

        let at_max = run_with_deleted_facts(partition, batch, owner, WAL_RUN_FACTS_MAX)?;
        let decoded = decode_wal(partition, batch, &encode_wal(&at_max)?)?;
        let WalBody::Run(decoded_facts) = decoded.body() else {
            return Err("a run archive must decode as a run".into());
        };
        assert_eq!(WAL_RUN_FACTS_MAX, decoded_facts.as_slice().len());

        let over_max =
            run_with_deleted_facts(partition, batch, owner, WAL_RUN_FACTS_MAX.saturating_add(1))?;
        assert_eq!(
            Err(DecodeError::InvalidBody),
            decode_wal(partition, batch, &encode(&over_max, WAL_ENCODED_BYTES_MAX)?),
            "an archived count above the named limit is rejected before result allocation"
        );
        Ok(())
    }

    #[test]
    fn fact_count_and_field_bounds_imply_the_wal_byte_bound()
    -> Result<(), Box<dyn std::error::Error>> {
        let partition = partition()?;
        let batch = BatchId::try_from(1)?;
        let owner = OwnerToken::from(SealGeneration::genesis());
        let path = "a"
            .repeat(strom_domain::STREAM_PATH_BYTES_MAX)
            .parse::<strom_domain::StreamPath>()?;
        let content_type: StreamContentType =
            format!("a/{}", "b".repeat(strom_domain::CONTENT_TYPE_BYTES_MAX - 2)).parse()?;
        let uid = StreamUid::try_from(1)?;
        let facts: Vec<_> = (0..WAL_RUN_FACTS_MAX)
            .map(|_ordinal| OperationFact::StreamCreated {
                path: path.clone(),
                uid,
                content_type: content_type.clone(),
                expiry: ExpiryPolicy::None,
                lifecycle: StreamLifecycle::Open,
            })
            .collect();
        let object = WalObject::new(
            partition,
            batch,
            owner,
            WalBody::Run(WalFacts::try_from(facts)?),
        );
        let encoded = encode_wal(&object)?;
        let estimate = WAL_RUN_FIXED_ENCODED_BYTES_MAX
            .checked_add(
                WAL_RUN_FACTS_MAX
                    .checked_mul(
                        WAL_FACT_ENCODED_FIXED_BYTES_MAX
                            + strom_domain::STREAM_PATH_BYTES_MAX
                            + strom_domain::CONTENT_TYPE_BYTES_MAX,
                    )
                    .expect("the maximum fact estimate fits in usize"),
            )
            .expect("the maximum RUN estimate fits in usize");
        assert!(
            encoded.len() <= estimate,
            "the incremental estimate dominates a maximum worst-case create RUN"
        );
        assert!(
            estimate <= WAL_ENCODED_BYTES_MAX,
            "the maximum estimated RUN stays inside the hard WAL object bound"
        );
        Ok(())
    }

    #[test]
    fn encoded_vectors_stay_below_the_incremental_byte_estimate()
    -> Result<(), Box<dyn std::error::Error>> {
        let partition = partition()?;
        let batch = BatchId::try_from(1)?;
        let owner = OwnerToken::from(SealGeneration::genesis());
        let path = "a"
            .repeat(strom_domain::STREAM_PATH_BYTES_MAX)
            .parse::<strom_domain::StreamPath>()?;
        let content_type: StreamContentType =
            format!("a/{}", "b".repeat(strom_domain::CONTENT_TYPE_BYTES_MAX - 2)).parse()?;
        let uid = StreamUid::try_from(1)?;
        let vectors = [
            OperationFact::StreamCreated {
                path: path.clone(),
                uid,
                content_type,
                expiry: ExpiryPolicy::None,
                lifecycle: StreamLifecycle::Closed,
            },
            OperationFact::StreamClosed {
                path: path.clone(),
                uid,
            },
            OperationFact::StreamDeleted { path, uid },
        ];

        for fact in vectors {
            let estimate = WAL_RUN_FIXED_ENCODED_BYTES_MAX
                .checked_add(fact.estimated_encoded_bytes())
                .expect("the one-fact estimate fits in usize");
            let object = WalObject::new(
                partition,
                batch,
                owner,
                WalBody::Run(WalFacts::try_from(vec![fact])?),
            );
            assert!(
                encode_wal(&object)?.len() <= estimate,
                "the fixed constants and variable lengths dominate every worst-case fact vector"
            );
        }
        Ok(())
    }

    fn partition() -> Result<PartitionId, crate::PartitionIdError> {
        "00112233-4455-6677-8899-aabbccddeeff".parse()
    }

    fn run_with_deleted_facts(
        partition: PartitionId,
        batch: BatchId,
        owner: OwnerToken,
        facts_count: usize,
    ) -> Result<WalObject, Box<dyn std::error::Error>> {
        let uid = StreamUid::try_from(1)?;
        let path = "events/abc".parse::<strom_domain::StreamPath>()?;
        let facts = (0..facts_count)
            .map(|_ordinal| OperationFact::StreamDeleted {
                path: path.clone(),
                uid,
            })
            .collect();
        Ok(WalObject::new(
            partition,
            batch,
            owner,
            WalBody::Run(WalFacts { facts }),
        ))
    }
}
