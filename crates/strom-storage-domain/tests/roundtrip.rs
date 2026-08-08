#![cfg(feature = "proptest")]

use proptest::prelude::*;
use strom_storage_domain::{
    AttemptId, FreshIdentity, SealGeneration, StoreKind, TableKey, TableObjectId,
    WAL_RUN_FIXED_ENCODED_BYTES_MAX, WalBody, decode_directory_sst, decode_ledger_sst, decode_seal,
    decode_wal, encode_directory_sst, encode_ledger_sst, encode_seal, encode_wal, strategy,
};

fn table_key(store: StoreKind) -> TableKey {
    let fresh = FreshIdentity::new(
        SealGeneration::try_from(2).expect("two is a nonzero generation"),
        AttemptId::new(SealGeneration::genesis(), 0),
        0,
    )
    .expect("genesis predates generation two");
    TableKey::new(TableObjectId::new(fresh, store))
}

proptest! {
    #[test]
    fn generated_seals_roundtrip_through_the_durable_codec(seal in strategy::seal()) {
        let encoded = encode_seal(&seal);
        prop_assert!(encoded.is_ok());
        if let Ok(encoded) = encoded {
            prop_assert_eq!(decode_seal(seal.generation(), &encoded), Ok(seal));
        }
    }

    #[test]
    fn generated_wal_objects_roundtrip_through_the_durable_codec(object in strategy::wal_object()) {
        let encoded = encode_wal(&object);
        prop_assert!(encoded.is_ok());
        if let Ok(encoded) = encoded {
            if let WalBody::Run(facts) = object.body() {
                let estimate = facts.as_slice().iter().try_fold(
                    WAL_RUN_FIXED_ENCODED_BYTES_MAX,
                    |bytes, fact| bytes.checked_add(fact.estimated_encoded_bytes()),
                );
                prop_assert!(estimate.is_some());
                if let Some(estimate) = estimate {
                    prop_assert!(encoded.len() <= estimate);
                }
            }
            prop_assert_eq!(decode_wal(object.partition(), object.batch(), &encoded), Ok(object));
        }
    }

    #[test]
    fn generated_directory_maps_roundtrip(
        partition in strategy::partition_id(),
        rows in strategy::directory_rows(),
    ) {
        let key = table_key(StoreKind::Directory);
        let encoded = encode_directory_sst(partition, &key, &rows);
        prop_assert!(encoded.is_ok());
        if let Ok(encoded) = encoded {
            prop_assert_eq!(decode_directory_sst(partition, &key, &encoded), Ok(rows));
        }
    }

    #[test]
    fn generated_ledger_maps_roundtrip(
        partition in strategy::partition_id(),
        rows in strategy::ledger_rows(),
    ) {
        let key = table_key(StoreKind::Ledger);
        let encoded = encode_ledger_sst(partition, &key, &rows);
        prop_assert!(encoded.is_ok());
        if let Ok(encoded) = encoded {
            prop_assert_eq!(decode_ledger_sst(partition, &key, &encoded), Ok(rows));
        }
    }
}
