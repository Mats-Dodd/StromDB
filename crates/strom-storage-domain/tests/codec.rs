use std::num::NonZeroU64;

use strom_domain::{ExpiryPolicy, StreamContentType, StreamId, StreamLifecycle, StreamTtl};
use strom_storage_domain::{
    BatchId, BoundedNonEmptyVec, DecodeError, LEDGER_RECORD_BYTES_MAX, LedgerKey, LedgerRecord,
    OperationFact, OwnerToken, PartitionId, PathTombstone, SEAL_ENCODED_BYTES_MAX, Seal,
    SealFormat, SealGeneration, SealIdentity, StreamRecord, StreamUid, TreeVersion,
    WAL_ENCODED_BYTES_MAX, WalFence, WalIdentity, WalObject, WalReplayPoint, WalRun,
    decode_ledger_record, decode_seal, decode_wal, encode_ledger_record, encode_seal, encode_wal,
};

fn partition() -> Result<PartitionId, strom_storage_domain::PartitionIdError> {
    "00112233-4455-6677-8899-aabbccddeeff".parse()
}

fn uid(raw: u64) -> Result<StreamUid, strom_storage_domain::ZeroCoordinate> {
    StreamUid::try_from(raw)
}

fn ledger_key(raw: &str) -> Result<LedgerKey, strom_domain::StreamIdError> {
    raw.parse::<StreamId>()
        .map(|stream_id| LedgerKey::from(&stream_id))
}

fn genesis_seal() -> Result<Seal, Box<dyn std::error::Error>> {
    Ok(Seal::new(
        partition()?,
        SealGeneration::genesis(),
        WalReplayPoint::Genesis,
        SealFormat::V1,
        TreeVersion::empty(),
        TreeVersion::empty(),
        TreeVersion::empty(),
    ))
}

fn replay_seal() -> Result<Seal, Box<dyn std::error::Error>> {
    let owner = OwnerToken::from(SealGeneration::try_from(2)?);
    Ok(Seal::new(
        partition()?,
        SealGeneration::try_from(3)?,
        WalReplayPoint::Through {
            batch: BatchId::try_from(9)?,
            owner,
        },
        SealFormat::V1,
        TreeVersion::empty(),
        TreeVersion::empty(),
        TreeVersion::empty(),
    ))
}

fn one_fact_run() -> Result<WalObject, Box<dyn std::error::Error>> {
    let fact = OperationFact::StreamCreated {
        path: ledger_key("events/abc")?,
        uid: uid(7)?,
        content_type: "application/json; charset=utf-8".parse()?,
        expiry: ExpiryPolicy::None,
    };
    Ok(WalObject::Run(WalRun::new(
        partition()?,
        BatchId::try_from(9)?,
        OwnerToken::from(SealGeneration::try_from(2)?),
        BoundedNonEmptyVec::try_from(vec![fact])?,
    )))
}

fn fence() -> Result<WalObject, Box<dyn std::error::Error>> {
    Ok(WalObject::Fence(WalFence::new(
        partition()?,
        BatchId::try_from(10)?,
        OwnerToken::from(SealGeneration::try_from(4)?),
    )))
}

fn live_record(expiry: ExpiryPolicy) -> Result<LedgerRecord, Box<dyn std::error::Error>> {
    Ok(LedgerRecord::Live(StreamRecord::new(
        uid(7)?,
        "application/json; charset=utf-8".parse()?,
        expiry,
        StreamLifecycle::Closed,
        BatchId::try_from(9)?,
    )))
}

#[test]
fn seal_golden_vectors_anchor_both_replay_variants() -> Result<(), Box<dyn std::error::Error>> {
    let cases: [(Seal, &[u8]); 2] = [
        (
            genesis_seal()?,
            &[
                83, 84, 82, 77, 1, 1, 0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204,
                221, 238, 255, 1, 0, 1, 109, 40, 168, 72,
            ],
        ),
        (
            replay_seal()?,
            &[
                83, 84, 82, 77, 1, 1, 0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204,
                221, 238, 255, 3, 1, 9, 2, 1, 251, 232, 10, 231,
            ],
        ),
    ];
    for (seal, expected) in cases {
        let encoded = encode_seal(&seal)?;
        assert_eq!(encoded, expected, "Seal bytes are a durable format anchor");
        assert_eq!(
            decode_seal(&seal.identity(), expected)?,
            seal,
            "golden Seal bytes must decode independently of the encoder"
        );
    }
    Ok(())
}

#[test]
fn wal_golden_vectors_anchor_run_and_fence() -> Result<(), Box<dyn std::error::Error>> {
    let cases: [(WalObject, &[u8]); 2] = [
        (
            one_fact_run()?,
            &[
                83, 84, 82, 77, 2, 1, 0, 0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204,
                221, 238, 255, 9, 2, 1, 0, 10, 101, 118, 101, 110, 116, 115, 47, 97, 98, 99, 7, 31,
                97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111, 110, 59, 32,
                99, 104, 97, 114, 115, 101, 116, 61, 117, 116, 102, 45, 56, 0, 17, 113, 50, 74,
            ],
        ),
        (
            fence()?,
            &[
                83, 84, 82, 77, 2, 1, 1, 0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204,
                221, 238, 255, 10, 4, 255, 58, 236, 218,
            ],
        ),
    ];
    for (object, expected) in cases {
        let encoded = encode_wal(&object)?;
        assert_eq!(encoded, expected, "WAL bytes are a durable format anchor");
        assert_eq!(
            decode_wal(&object.identity(), expected)?,
            object,
            "golden WAL bytes must decode independently of the encoder"
        );
    }
    Ok(())
}

#[test]
fn ledger_golden_vectors_anchor_every_expiry_and_tombstones()
-> Result<(), Box<dyn std::error::Error>> {
    let ttl = StreamTtl::from(NonZeroU64::new(3600).ok_or("3600 is nonzero")?);
    let cases: [(LedgerRecord, &[u8]); 4] = [
        (
            live_record(ExpiryPolicy::None)?,
            &[
                0, 7, 31, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111,
                110, 59, 32, 99, 104, 97, 114, 115, 101, 116, 61, 117, 116, 102, 45, 56, 0, 1, 9,
            ],
        ),
        (
            live_record(ExpiryPolicy::SlidingTtl(ttl))?,
            &[
                0, 7, 31, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111,
                110, 59, 32, 99, 104, 97, 114, 115, 101, 116, 61, 117, 116, 102, 45, 56, 1, 144,
                28, 1, 9,
            ],
        ),
        (
            live_record(ExpiryPolicy::AbsoluteExpiry(
                "2030-01-01T00:00:00Z".parse()?,
            ))?,
            &[
                0, 7, 31, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106, 115, 111,
                110, 59, 32, 99, 104, 97, 114, 115, 101, 116, 61, 117, 116, 102, 45, 56, 2, 128,
                128, 168, 221, 230, 140, 244, 198, 52, 1, 9,
            ],
        ),
        (
            LedgerRecord::Tombstone(PathTombstone::new(uid(7)?)),
            &[1, 7],
        ),
    ];
    for (record, expected) in cases {
        let encoded = encode_ledger_record(&record)?;
        assert_eq!(
            encoded, expected,
            "Ledger bytes are a durable format anchor"
        );
        assert_eq!(
            decode_ledger_record(expected)?,
            record,
            "golden Ledger bytes must decode independently of the encoder"
        );
    }
    Ok(())
}

#[test]
fn envelope_gates_run_in_the_normative_order() -> Result<(), Box<dyn std::error::Error>> {
    let seal = genesis_seal()?;
    let identity = seal.identity();
    let valid = encode_seal(&seal)?;

    let oversized = vec![0u8; SEAL_ENCODED_BYTES_MAX.saturating_add(1)];
    assert_eq!(
        decode_seal(&identity, &oversized),
        Err(DecodeError::EncodedBytesOverMax {
            bytes_max: SEAL_ENCODED_BYTES_MAX,
            bytes_actual: oversized.len(),
        }),
        "length bounds all work before frame parsing"
    );
    assert_eq!(
        decode_seal(&identity, b"STRM"),
        Err(DecodeError::FrameTooShort {
            bytes_min: 10,
            bytes_actual: 4,
        }),
        "a partial header fails the length gate"
    );

    let mut bad_magic = valid.clone();
    *bad_magic.first_mut().ok_or("frame has magic")? = b'X';
    assert!(
        matches!(
            decode_seal(&identity, &bad_magic),
            Err(DecodeError::MagicMismatch { .. })
        ),
        "magic is observed before the now-invalid checksum"
    );

    let mut bad_kind_and_checksum = valid.clone();
    *bad_kind_and_checksum.get_mut(4).ok_or("frame has kind")? = 99;
    assert!(
        matches!(
            decode_seal(&identity, &bad_kind_and_checksum),
            Err(DecodeError::ChecksumMismatch { .. })
        ),
        "checksum is observed before an unknown kind"
    );

    let bad_kind = rewrite_frame_byte(&valid, 4, 99)?;
    assert_eq!(
        decode_seal(&identity, &bad_kind),
        Err(DecodeError::ObjectKindMismatch {
            expected: 1,
            observed: 99,
        }),
        "unknown kinds and real objects in the wrong location share one gate"
    );
    let reserved_version = rewrite_frame_byte(&valid, 5, 0)?;
    assert_eq!(
        decode_seal(&identity, &reserved_version),
        Err(DecodeError::UnsupportedVersion { observed: 0 }),
        "reserved version zero reaches the upgrade-path gate"
    );
    Ok(())
}

#[test]
fn decoders_reject_trailing_bytes_and_location_identity_mismatches()
-> Result<(), Box<dyn std::error::Error>> {
    let seal = genesis_seal()?;
    let encoded = encode_seal(&seal)?;
    let trailing = insert_body_byte(&encoded, 0)?;
    assert_eq!(
        decode_seal(&seal.identity(), &trailing),
        Err(DecodeError::TrailingBytes { bytes_actual: 1 }),
        "a checksummed but noncanonical body cannot drift silently"
    );

    let wrong_identity = SealIdentity::new(partition()?, SealGeneration::try_from(2)?);
    assert_eq!(
        decode_seal(&wrong_identity, &trailing),
        Err(DecodeError::TrailingBytes { bytes_actual: 1 }),
        "body canonicality is established before the location identity cross-check"
    );
    assert_eq!(
        decode_seal(&wrong_identity, &encoded),
        Err(DecodeError::IdentityMismatch),
        "the durable location and body identity are inseparable decoder inputs"
    );

    let fence = fence()?;
    let wrong_wal_identity = WalIdentity::new(partition()?, BatchId::try_from(11)?);
    assert_eq!(
        decode_wal(&wrong_wal_identity, &encode_wal(&fence)?),
        Err(DecodeError::IdentityMismatch),
        "WAL coordinates receive the same key/body cross-check"
    );

    let record = live_record(ExpiryPolicy::None)?;
    let mut row = encode_ledger_record(&record)?;
    row.push(0);
    assert_eq!(
        decode_ledger_record(&row),
        Err(DecodeError::TrailingBytes { bytes_actual: 1 }),
        "frameless rows are canonical postcard values too"
    );
    Ok(())
}

#[test]
fn every_decoder_rejects_over_bound_input_before_parsing() -> Result<(), Box<dyn std::error::Error>>
{
    let seal_identity = genesis_seal()?.identity();
    let wal_identity = WalIdentity::new(partition()?, BatchId::try_from(1)?);
    assert!(matches!(
        decode_seal(
            &seal_identity,
            &vec![0; SEAL_ENCODED_BYTES_MAX.saturating_add(1)]
        ),
        Err(DecodeError::EncodedBytesOverMax { .. })
    ));
    assert!(matches!(
        decode_wal(
            &wal_identity,
            &vec![0; WAL_ENCODED_BYTES_MAX.saturating_add(1)]
        ),
        Err(DecodeError::EncodedBytesOverMax { .. })
    ));
    assert!(matches!(
        decode_ledger_record(&vec![0; LEDGER_RECORD_BYTES_MAX.saturating_add(1)]),
        Err(DecodeError::EncodedBytesOverMax { .. })
    ));
    Ok(())
}

#[test]
fn canonical_content_type_boundary_roundtrips_through_storage()
-> Result<(), Box<dyn std::error::Error>> {
    let subtype = "b".repeat(243);
    let content_type: StreamContentType = format!("a/{subtype};charset=x").parse()?;
    assert_eq!(
        content_type.to_string().len(),
        strom_domain::CONTENT_TYPE_BYTES_MAX,
        "the accepted boundary value has an in-bound canonical spelling"
    );
    let record = LedgerRecord::Live(StreamRecord::new(
        uid(1)?,
        content_type,
        ExpiryPolicy::None,
        StreamLifecycle::Open,
        BatchId::try_from(1)?,
    ));
    let encoded = encode_ledger_record(&record)?;
    assert_eq!(
        decode_ledger_record(&encoded)?,
        record,
        "every accepted content type must remain parseable after durable canonicalization"
    );
    Ok(())
}

#[expect(
    clippy::big_endian_bytes,
    reason = "RFC 0002 fixes the trailing CRC-32C spelling as big-endian"
)]
fn rewrite_frame_byte(
    frame: &[u8],
    offset: usize,
    value: u8,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let covered_bytes = frame.len().checked_sub(4).ok_or("frame has checksum")?;
    let mut rewritten = frame
        .get(..covered_bytes)
        .ok_or("frame has coverage")?
        .to_vec();
    *rewritten.get_mut(offset).ok_or("offset lies in frame")? = value;
    let checksum = crc32c::crc32c(&rewritten);
    rewritten.extend_from_slice(&checksum.to_be_bytes());
    Ok(rewritten)
}

#[expect(
    clippy::big_endian_bytes,
    reason = "RFC 0002 fixes the trailing CRC-32C spelling as big-endian"
)]
fn insert_body_byte(frame: &[u8], value: u8) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let covered_bytes = frame.len().checked_sub(4).ok_or("frame has checksum")?;
    let mut rewritten = frame
        .get(..covered_bytes)
        .ok_or("frame has coverage")?
        .to_vec();
    rewritten.push(value);
    let checksum = crc32c::crc32c(&rewritten);
    rewritten.extend_from_slice(&checksum.to_be_bytes());
    Ok(rewritten)
}
