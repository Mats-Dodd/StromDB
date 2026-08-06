use std::num::NonZeroU64;

use strom_domain::{ExpiryPolicy, StreamContentType, StreamId};
use strom_storage_domain::{
    AttemptId, BatchId, DecodeError, DirectoryKey, FreshIdentity, OperationFact, OwnerToken,
    PartitionId, SEAL_ENCODED_BYTES_MAX, Seal, SealGeneration, SealIdentity, SortedRun, StoreKind,
    StreamUid, TableObjectId, TableRef, TreeVersion, WAL_ENCODED_BYTES_MAX, WalBody, WalFacts,
    WalIdentity, WalObject, WalReplayPoint, decode_seal, decode_wal, encode_seal, encode_wal,
};

fn partition() -> Result<PartitionId, strom_storage_domain::PartitionIdError> {
    "00112233-4455-6677-8899-aabbccddeeff".parse()
}

fn uid(raw: u64) -> Result<StreamUid, strom_storage_domain::ZeroCoordinate> {
    StreamUid::try_from(raw)
}

fn directory_key(raw: &str) -> Result<DirectoryKey, strom_domain::StreamIdError> {
    raw.parse::<StreamId>()
        .map(|stream_id| DirectoryKey::from(&stream_id))
}

fn genesis_seal() -> Result<Seal, Box<dyn std::error::Error>> {
    Ok(Seal::new(
        partition()?,
        SealGeneration::genesis(),
        WalReplayPoint::Genesis,
        TreeVersion::empty(),
        TreeVersion::empty(),
    )?)
}

fn tree_with_table(
    generation: SealGeneration,
    store: StoreKind,
    ordinal: u32,
) -> Result<TreeVersion, Box<dyn std::error::Error>> {
    let fresh = FreshIdentity::new(
        generation,
        AttemptId::new(SealGeneration::genesis(), 4),
        ordinal,
    )?;
    let table = TableRef::new(
        TableObjectId::new(fresh, store),
        NonZeroU64::new(123).ok_or("table length is nonzero")?,
    )?;
    let run = SortedRun::try_from(vec![table])?;
    Ok(TreeVersion::try_from(vec![run])?)
}

fn representative_seal() -> Result<Seal, Box<dyn std::error::Error>> {
    let generation = SealGeneration::try_from(2)?;
    Ok(Seal::new(
        partition()?,
        generation,
        WalReplayPoint::Through {
            batch: BatchId::try_from(9)?,
            owner: OwnerToken::from(SealGeneration::genesis()),
        },
        tree_with_table(generation, StoreKind::Directory, 0)?,
        tree_with_table(generation, StoreKind::Ledger, 1)?,
    )?)
}

fn one_fact_run() -> Result<WalObject, Box<dyn std::error::Error>> {
    let fact = OperationFact::StreamCreated {
        path: directory_key("events/abc")?,
        uid: uid(7)?,
        content_type: "application/json; charset=utf-8".parse()?,
        expiry: ExpiryPolicy::None,
    };
    Ok(WalObject::new(
        partition()?,
        BatchId::try_from(9)?,
        OwnerToken::from(SealGeneration::try_from(2)?),
        WalBody::Run(WalFacts::try_from(vec![fact])?),
    ))
}

fn fence() -> Result<WalObject, Box<dyn std::error::Error>> {
    Ok(WalObject::new(
        partition()?,
        BatchId::try_from(10)?,
        OwnerToken::from(SealGeneration::try_from(4)?),
        WalBody::Fence,
    ))
}

#[test]
fn seal_archive_fixture_anchors_the_root() -> Result<(), Box<dyn std::error::Error>> {
    let seal = genesis_seal()?;
    let expected = &[
        0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255, 1, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 215, 255, 255, 255, 0, 0, 0, 0,
        207, 255, 255, 255, 0, 0, 0, 0,
    ];
    let encoded = encode_seal(&seal)?;
    let first_difference =
        encoded
            .iter()
            .zip(expected)
            .enumerate()
            .find_map(|(index, (actual, expected))| {
                (actual != expected).then_some((index, *actual, *expected))
            });
    assert_eq!(
        encoded.len(),
        expected.len(),
        "Seal fixture length changed; first difference: {first_difference:?}"
    );
    assert_eq!(
        first_difference, None,
        "Seal bytes are a durable format anchor"
    );
    assert_eq!(
        decode_seal(&seal.identity(), expected)?,
        seal,
        "fixture bytes decode independently of the encoder"
    );
    Ok(())
}

#[test]
fn wal_archive_fixture_anchors_the_root() -> Result<(), Box<dyn std::error::Error>> {
    let object = one_fact_run()?;
    let expected = &[
        101, 118, 101, 110, 116, 115, 47, 97, 98, 99, 97, 112, 112, 108, 105, 99, 97, 116, 105,
        111, 110, 47, 106, 115, 111, 110, 59, 32, 99, 104, 97, 114, 115, 101, 116, 61, 117, 116,
        102, 45, 56, 0, 214, 255, 255, 255, 10, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 159, 0, 0, 0, 208,
        255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 34, 51, 68, 85,
        102, 119, 136, 153, 170, 187, 204, 221, 238, 255, 9, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0,
        0, 0, 0, 181, 255, 255, 255, 1, 0, 0, 0,
    ];
    let encoded = encode_wal(&object)?;
    assert_eq!(encoded.len(), expected.len(), "WAL fixture length changed");
    assert_eq!(encoded, expected, "WAL bytes are a durable format anchor");
    assert_eq!(
        decode_wal(&object.identity(), expected)?,
        object,
        "fixture bytes decode independently of the encoder"
    );
    Ok(())
}

#[test]
fn nonempty_seal_manifest_roundtrips() -> Result<(), Box<dyn std::error::Error>> {
    let seal = representative_seal()?;
    assert_eq!(decode_seal(&seal.identity(), &encode_seal(&seal)?)?, seal);
    Ok(())
}

#[test]
fn every_wal_fact_variant_roundtrips() -> Result<(), Box<dyn std::error::Error>> {
    let path = directory_key("events/abc")?;
    let uid = uid(7)?;
    let object = WalObject::new(
        partition()?,
        BatchId::try_from(9)?,
        OwnerToken::from(SealGeneration::try_from(2)?),
        WalBody::Run(WalFacts::try_from(vec![
            OperationFact::StreamCreated {
                path: path.clone(),
                uid,
                content_type: "application/json".parse()?,
                expiry: ExpiryPolicy::None,
            },
            OperationFact::StreamClosed {
                path: path.clone(),
                uid,
            },
            OperationFact::StreamDeleted { path, uid },
        ])?),
    );
    assert_eq!(
        decode_wal(&object.identity(), &encode_wal(&object)?)?,
        object
    );
    Ok(())
}

#[test]
fn wal_checked_archive_enforces_structure_alignment_bound_and_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let object = fence()?;
    let encoded = encode_wal(&object)?;

    let mut misaligned = Vec::with_capacity(encoded.len().saturating_add(1));
    misaligned.push(0);
    misaligned.extend_from_slice(&encoded);
    let misaligned = misaligned
        .get(1..)
        .ok_or("the prefixed WAL archive contains its payload")?;
    assert_eq!(
        decode_wal(&object.identity(), misaligned),
        Ok(object.clone()),
        "object-store slices do not promise archive alignment"
    );

    let truncated = encoded
        .get(..encoded.len().saturating_sub(1))
        .ok_or("a WAL archive is non-empty")?;
    assert_eq!(
        decode_wal(&object.identity(), truncated),
        Err(DecodeError::MalformedArchive),
        "checked access rejects a truncated archive"
    );

    let wrong_identity = WalIdentity::new(partition()?, BatchId::try_from(11)?);
    assert_eq!(
        decode_wal(&wrong_identity, &encoded),
        Err(DecodeError::IdentityMismatch),
        "the durable location and archived identity are one decoder input"
    );

    let oversized = vec![0; WAL_ENCODED_BYTES_MAX.saturating_add(1)];
    assert_eq!(
        decode_wal(&object.identity(), &oversized),
        Err(DecodeError::EncodedBytesOverMax {
            bytes_max: WAL_ENCODED_BYTES_MAX,
            bytes_actual: oversized.len(),
        }),
        "the complete byte bound runs before structural access"
    );
    Ok(())
}

#[test]
fn seal_checked_archive_enforces_structure_alignment_bound_and_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let seal = genesis_seal()?;
    let encoded = encode_seal(&seal)?;

    let mut misaligned = Vec::with_capacity(encoded.len().saturating_add(1));
    misaligned.push(0);
    misaligned.extend_from_slice(&encoded);
    let misaligned = misaligned
        .get(1..)
        .ok_or("the prefixed Seal archive contains its payload")?;
    assert_eq!(
        decode_seal(&seal.identity(), misaligned),
        Ok(seal.clone()),
        "object-store slices do not promise archive alignment"
    );

    let truncated = encoded
        .get(..encoded.len().saturating_sub(1))
        .ok_or("a Seal archive is non-empty")?;
    assert_eq!(
        decode_seal(&seal.identity(), truncated),
        Err(DecodeError::MalformedArchive),
        "checked access rejects a truncated archive"
    );

    let wrong_identity = SealIdentity::new(partition()?, SealGeneration::try_from(2)?);
    assert_eq!(
        decode_seal(&wrong_identity, &encoded),
        Err(DecodeError::IdentityMismatch),
        "the durable location and archived identity are one decoder input"
    );

    let oversized = vec![0; SEAL_ENCODED_BYTES_MAX.saturating_add(1)];
    assert_eq!(
        decode_seal(&seal.identity(), &oversized),
        Err(DecodeError::EncodedBytesOverMax {
            bytes_max: SEAL_ENCODED_BYTES_MAX,
            bytes_actual: oversized.len(),
        }),
        "the complete byte bound runs before structural access"
    );
    Ok(())
}

#[test]
fn canonical_content_type_boundary_roundtrips_through_wal() -> Result<(), Box<dyn std::error::Error>>
{
    let subtype = "b".repeat(243);
    let content_type: StreamContentType = format!("a/{subtype};charset=x").parse()?;
    assert_eq!(
        content_type.to_string().len(),
        strom_domain::CONTENT_TYPE_BYTES_MAX,
        "the accepted boundary value has an in-bound canonical spelling"
    );
    let object = WalObject::new(
        partition()?,
        BatchId::try_from(1)?,
        OwnerToken::from(SealGeneration::genesis()),
        WalBody::Run(WalFacts::try_from(vec![OperationFact::StreamCreated {
            path: directory_key("events/abc")?,
            uid: uid(1)?,
            content_type,
            expiry: ExpiryPolicy::None,
        }])?),
    );
    let encoded = encode_wal(&object)?;
    assert_eq!(
        decode_wal(&object.identity(), &encoded)?,
        object,
        "every accepted content type remains parseable after durable canonicalization"
    );
    Ok(())
}

#[test]
fn durable_content_types_must_use_the_canonical_spelling() -> Result<(), Box<dyn std::error::Error>>
{
    let object = one_fact_run()?;
    let mut encoded = encode_wal(&object)?;
    let content_type = encoded
        .windows(b"application/json".len())
        .position(|window| window == b"application/json")
        .ok_or("the archived content type exists")?;
    *encoded
        .get_mut(content_type)
        .ok_or("the first content-type byte exists")? = b'A';
    assert_eq!(
        decode_wal(&object.identity(), &encoded),
        Err(DecodeError::InvalidBody)
    );
    Ok(())
}

#[test]
fn unreachable_leading_bytes_are_tolerated_but_never_emitted()
-> Result<(), Box<dyn std::error::Error>> {
    let object = fence()?;
    let encoded = encode_wal(&object)?;
    let mut prefixed = vec![0xaa, 0xbb, 0xcc];
    prefixed.extend_from_slice(&encoded);
    assert_eq!(decode_wal(&object.identity(), &prefixed)?, object);
    assert_ne!(encode_wal(&object)?, prefixed);
    Ok(())
}
