#![cfg(feature = "proptest")]

use proptest::prelude::*;
use strom_storage_domain::{
    AttemptId, FreshIdentity, SealGeneration, StoreKind, TableKey, TableObjectId,
    decode_directory_sst, decode_ledger_sst, decode_seal, decode_wal, encode_directory_sst,
    encode_ledger_sst, encode_seal, encode_wal, strategy,
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
    fn generated_directory_maps_roundtrip(
        partition in strategy::partition_id(),
        rows in strategy::directory_rows(),
    ) {
        let key = table_key(partition, StoreKind::Directory);
        let encoded = encode_directory_sst(&key, &rows);
        prop_assert!(encoded.is_ok());
        if let Ok(encoded) = encoded {
            prop_assert_eq!(decode_directory_sst(&key, &encoded), Ok(rows));
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
