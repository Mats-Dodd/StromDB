#![cfg(feature = "proptest")]

use proptest::prelude::*;
use strom_storage_domain::{
    AttemptId, FreshIdentity, SealGeneration, StoreKind, TableKey, TableObjectId,
    decode_directory_sst, decode_ledger_sst, decode_seal, decode_stream_record, decode_wal,
    encode_directory_sst, encode_ledger_sst, encode_seal, encode_stream_record, encode_wal,
    strategy,
};

fn table_key(partition: strom_storage_domain::PartitionId, store: StoreKind) -> TableKey {
    let fresh = FreshIdentity::new(
        SealGeneration::try_from(2).expect("two is a nonzero generation"),
        AttemptId::new(SealGeneration::genesis(), 0),
        0,
    )
    .expect("genesis predates generation two");
    TableKey::new(partition, TableObjectId::new(fresh, store))
}

#[expect(
    clippy::big_endian_bytes,
    reason = "the property independently inspects RFC 0003 big-endian row lengths"
)]
fn encoded_directory_shared_prefixes(encoded: &[u8], row_count: usize) -> Option<Vec<usize>> {
    let mut offset = strom_storage_domain::SST_HEADER_BYTES;
    let mut shared_prefixes = Vec::with_capacity(row_count);
    for _row in 0..row_count {
        let shared_end = offset.checked_add(2)?;
        let shared_raw: [u8; 2] = encoded.get(offset..shared_end)?.try_into().ok()?;
        shared_prefixes.push(usize::from(u16::from_be_bytes(shared_raw)));

        let suffix_start = shared_end;
        let suffix_end = suffix_start.checked_add(2)?;
        let suffix_raw: [u8; 2] = encoded.get(suffix_start..suffix_end)?.try_into().ok()?;
        let suffix_length = usize::from(u16::from_be_bytes(suffix_raw));
        offset = offset.checked_add(13)?.checked_add(suffix_length)?;
    }
    (offset == encoded.len()).then_some(shared_prefixes)
}

proptest! {
    #[test]
    fn generated_seals_roundtrip_through_the_durable_codec(seal in strategy::seal()) {
        let encoded = encode_seal(&seal);
        prop_assert!(encoded.is_ok());
        if let Ok(encoded) = encoded {
            prop_assert_eq!(decode_seal(&seal.identity(), &encoded), Ok(seal));
        }
    }

    #[test]
    fn generated_wal_objects_roundtrip_through_the_durable_codec(object in strategy::wal_object()) {
        let encoded = encode_wal(&object);
        prop_assert!(encoded.is_ok());
        if let Ok(encoded) = encoded {
            prop_assert_eq!(decode_wal(&object.identity(), &encoded), Ok(object));
        }
    }

    #[test]
    fn generated_stream_records_roundtrip_through_the_durable_codec(
        record in strategy::stream_record(),
    ) {
        let encoded = encode_stream_record(&record);
        prop_assert!(encoded.is_ok());
        if let Ok(encoded) = encoded {
            prop_assert_eq!(decode_stream_record(&encoded), Ok(record));
        }
    }

    #[test]
    fn generated_directory_maps_roundtrip_and_use_longest_prefixes(
        partition in strategy::partition_id(),
        rows in strategy::directory_rows(),
    ) {
        let key = table_key(partition, StoreKind::Directory);
        let encoded = encode_directory_sst(&key, &rows);
        prop_assert!(encoded.is_ok());
        if let Ok(encoded) = encoded {
            prop_assert_eq!(decode_directory_sst(&key, &encoded), Ok(rows.clone()));
            let mut previous: &[u8] = &[];
            let mut expected_prefixes = Vec::with_capacity(rows.len());
            for (row_key, _entry) in &rows {
                expected_prefixes.push(
                    previous
                        .iter()
                        .zip(row_key.as_bytes())
                        .take_while(|(left, right)| left == right)
                        .count(),
                );
                previous = row_key.as_bytes();
            }
            prop_assert_eq!(
                encoded_directory_shared_prefixes(&encoded, rows.len()),
                Some(expected_prefixes),
            );
        }
    }

    #[test]
    fn generated_ledger_maps_roundtrip(
        partition in strategy::partition_id(),
        rows in strategy::ledger_rows(),
    ) {
        let key = table_key(partition, StoreKind::Ledger);
        let encoded = encode_ledger_sst(&key, &rows);
        prop_assert!(encoded.is_ok());
        if let Ok(encoded) = encoded {
            prop_assert_eq!(decode_ledger_sst(&key, &encoded), Ok(rows));
        }
    }
}
