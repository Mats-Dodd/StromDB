use std::cmp::Ordering;
use std::num::NonZeroU64;

use proptest::prelude::*;
use strom_domain::StreamId;
use strom_storage_domain::{
    AttemptId, BatchId, CoordinateExhausted, DirectoryKey, FreshIdentity, KeySpellingError,
    OperationFact, PartitionId, PartitionIdError, SealGeneration, SealIdentity, SealKey, StoreKind,
    StreamUid, TableKey, TableObjectId, WAL_RUN_FACTS_MAX, WalFacts, WalFactsError, WalIdentity,
    WalKey,
};

#[test]
fn partition_id_accepts_every_non_nil_uuid_bit_pattern_and_canonicalizes_spelling()
-> Result<(), Box<dyn std::error::Error>> {
    let partition: PartitionId = "00112233-4455-6677-8899-aabbccddeeff".parse()?;
    assert_eq!(
        partition.to_string(),
        "00112233-4455-6677-8899-aabbccddeeff",
        "the durable spelling is lowercase and hyphenated"
    );
    assert_eq!(
        "00112233-4455-6677-8899-AABBCCDDEEFF".parse::<PartitionId>(),
        Err(PartitionIdError::Malformed),
        "uppercase aliases are noncanonical"
    );
    assert_eq!(
        "00000000-0000-0000-0000-000000000000".parse::<PartitionId>(),
        Err(PartitionIdError::Nil),
        "nil is the only rejected UUID bit pattern"
    );
    Ok(())
}

#[test]
fn nonzero_coordinates_have_checked_successors() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        SealGeneration::genesis().get(),
        1,
        "generation one is canonical genesis"
    );
    assert_eq!(
        SealGeneration::genesis().successor()?.get(),
        2,
        "ordinary generations advance by exactly one"
    );
    let exhausted = SealGeneration::from(NonZeroU64::MAX);
    assert_eq!(
        exhausted.successor(),
        Err(CoordinateExhausted),
        "successor arithmetic never wraps into reserved zero"
    );
    assert!(
        BatchId::try_from(0).is_err(),
        "WAL coordinate zero is virtual genesis, not an object coordinate"
    );
    Ok(())
}

#[test]
fn wal_facts_crosses_both_count_boundaries() -> Result<(), Box<dyn std::error::Error>> {
    let uid = StreamUid::try_from(1)?;
    let path = DirectoryKey::from(&"events/abc".parse::<StreamId>()?);
    let deleted = OperationFact::StreamDeleted { path, uid };

    assert_eq!(
        WalFacts::try_from(Vec::new()),
        Err(WalFactsError::Empty),
        "a WAL run cannot represent no mutation"
    );
    assert!(
        WalFacts::try_from(vec![deleted.clone(); WAL_RUN_FACTS_MAX]).is_ok(),
        "the published fact-count bound itself is accepted"
    );
    assert_eq!(
        WalFacts::try_from(vec![deleted; WAL_RUN_FACTS_MAX.saturating_add(1)]),
        Err(WalFactsError::OverMax {
            facts_actual: WAL_RUN_FACTS_MAX.saturating_add(1),
        }),
        "the first over-bound count is rejected"
    );
    Ok(())
}

#[test]
fn durable_key_golden_spellings_select_newest_coordinates_first()
-> Result<(), Box<dyn std::error::Error>> {
    let partition: PartitionId = "00112233-4455-6677-8899-aabbccddeeff".parse()?;
    let generation_one = SealKey::from(SealIdentity::new(partition, SealGeneration::genesis()));
    let generation_two = SealKey::from(SealIdentity::new(
        partition,
        SealGeneration::genesis().successor()?,
    ));
    assert_eq!(
        generation_one.to_string(),
        "partition/00112233-4455-6677-8899-aabbccddeeff/seal/v1/18446744073709551614",
        "generation one anchors the reverse-coordinate spelling"
    );
    assert_eq!(
        generation_two.to_string(),
        "partition/00112233-4455-6677-8899-aabbccddeeff/seal/v1/18446744073709551613",
        "generation two decrements the storage ordinal"
    );
    assert_eq!(
        generation_two.to_string().cmp(&generation_one.to_string()),
        Ordering::Less,
        "ascending listing puts the newer generation first"
    );
    assert_eq!(
        generation_one.to_string().parse::<SealKey>()?,
        generation_one,
        "the canonical key parser recovers its typed identity"
    );

    let wal_key = WalKey::from(WalIdentity::new(partition, BatchId::try_from(42)?));
    assert_eq!(
        wal_key.to_string(),
        "partition/00112233-4455-6677-8899-aabbccddeeff/wal/v1/18446744073709551573",
        "WAL keys use the independent batch coordinate in the same namespace scheme"
    );
    assert_eq!(wal_key.to_string().parse::<WalKey>()?, wal_key);
    Ok(())
}

#[test]
fn malformed_durable_key_spellings_fail_closed() {
    let cases = [
        (
            "partition/00112233-4455-6677-8899-aabbccddeeff/seal/v1/0000000000000000000",
            KeySpellingError::ReverseOrdinal,
        ),
        (
            "partition/00112233-4455-6677-8899-aabbccddeeff/seal/v1/18446744073709551615",
            KeySpellingError::ZeroCoordinate,
        ),
        (
            "partition/00112233-4455-6677-8899-AABBCCDDEEFF/seal/v1/18446744073709551614",
            KeySpellingError::Partition(PartitionIdError::Malformed),
        ),
        (
            "partition/00000000-0000-0000-0000-000000000000/seal/v1/18446744073709551614",
            KeySpellingError::Partition(PartitionIdError::Nil),
        ),
        (
            "partition/00112233-4455-6677-8899-aabbccddeeff/seal/v2/18446744073709551614",
            KeySpellingError::UnsupportedNamespace,
        ),
        (
            "partition/00112233-4455-6677-8899-aabbccddeeff/seal/v1/99999999999999999999",
            KeySpellingError::ReverseOrdinal,
        ),
    ];
    for (raw, expected) in cases {
        assert_eq!(
            raw.parse::<SealKey>(),
            Err(expected),
            "noncanonical durable key must be rejected: {raw}"
        );
    }
}

#[test]
fn table_key_golden_spellings_anchor_every_store() -> Result<(), Box<dyn std::error::Error>> {
    let partition: PartitionId = "00112233-4455-6677-8899-aabbccddeeff".parse()?;
    let fresh = FreshIdentity::new(
        SealGeneration::try_from(2)?,
        AttemptId::new(SealGeneration::genesis(), 4),
        7,
    )?;
    let cases = [
        (
            StoreKind::Directory,
            "partition/00112233-4455-6677-8899-aabbccddeeff/table/v1/directory/00000000000000000002/00000000000000000001-00000000000000000004/0000000007",
        ),
        (
            StoreKind::Ledger,
            "partition/00112233-4455-6677-8899-aabbccddeeff/table/v1/ledger/00000000000000000002/00000000000000000001-00000000000000000004/0000000007",
        ),
        (
            StoreKind::Tally,
            "partition/00112233-4455-6677-8899-aabbccddeeff/table/v1/tally/00000000000000000002/00000000000000000001-00000000000000000004/0000000007",
        ),
        (
            StoreKind::Annals,
            "partition/00112233-4455-6677-8899-aabbccddeeff/table/v1/annals/00000000000000000002/00000000000000000001-00000000000000000004/0000000007",
        ),
    ];
    for (store, expected) in cases {
        let key = TableKey::new(partition, TableObjectId::new(fresh, store));
        assert_eq!(
            key.to_string(),
            expected,
            "each store has one lowercase spelling"
        );
        assert_eq!(
            expected.parse::<TableKey>()?,
            key,
            "the golden key roundtrips"
        );
    }
    Ok(())
}

#[test]
fn malformed_table_key_spellings_fail_closed() {
    let valid = "partition/00112233-4455-6677-8899-aabbccddeeff/table/v1/directory/00000000000000000002/00000000000000000001-00000000000000000004/0000000007";
    let cases = [
        valid.replace("directory", "Directory"),
        valid.replace("00000000000000000002", "0000000000000000002"),
        valid.replace("00000000000000000004", "+0000000000000000004"),
        valid.replace("0000000007", "00000000007"),
        valid.replace("0000000007", "4294967296"),
        valid.replace("00000000000000000002", "00000000000000000000"),
        format!("{valid}/extra"),
    ];
    for malformed in cases {
        assert!(
            malformed.parse::<TableKey>().is_err(),
            "noncanonical table key must be rejected: {malformed}"
        );
    }
}

proptest! {
    #[test]
    fn directory_key_order_is_stream_id_utf8_byte_order(
        left_segments in prop::collection::vec("[a-z0-9_-]{1,8}", 1..4),
        right_segments in prop::collection::vec("[a-z0-9_-]{1,8}", 1..4),
    ) {
        let left_raw = left_segments.join("/");
        let right_raw = right_segments.join("/");
        let left_stream = left_raw.parse::<StreamId>();
        let right_stream = right_raw.parse::<StreamId>();
        prop_assert!(left_stream.is_ok() && right_stream.is_ok());
        if let (Ok(left_stream), Ok(right_stream)) = (left_stream, right_stream) {
            let left_key = DirectoryKey::from(&left_stream);
            let right_key = DirectoryKey::from(&right_stream);
            prop_assert_eq!(left_key.cmp(&right_key), left_raw.as_bytes().cmp(right_raw.as_bytes()));
        }
    }

    #[test]
    fn reverse_spelling_inverts_every_distinct_generation_pair(
        left in 1u64..,
        right in 1u64..,
    ) {
        prop_assume!(left != right);
        let partition = PartitionId::try_from([1u8; 16]);
        prop_assert!(partition.is_ok());
        if let Ok(partition) = partition {
            let left_generation = SealGeneration::try_from(left);
            let right_generation = SealGeneration::try_from(right);
            prop_assert!(left_generation.is_ok() && right_generation.is_ok());
            if let (Ok(left_generation), Ok(right_generation)) = (left_generation, right_generation) {
                let left_key = SealKey::from(SealIdentity::new(partition, left_generation)).to_string();
                let right_key = SealKey::from(SealIdentity::new(partition, right_generation)).to_string();
                prop_assert_eq!(left_key.cmp(&right_key), right.cmp(&left));
            }
        }
    }
}
