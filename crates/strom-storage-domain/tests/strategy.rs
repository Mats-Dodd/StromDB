#![cfg(feature = "proptest")]

use proptest::prelude::*;
use strom_storage_domain::{
    decode_ledger_record, decode_seal, decode_wal, encode_ledger_record, encode_seal, encode_wal,
    strategy,
};

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
    fn generated_ledger_records_roundtrip_through_the_durable_codec(
        record in strategy::ledger_record(),
    ) {
        let encoded = encode_ledger_record(&record);
        prop_assert!(encoded.is_ok());
        if let Ok(encoded) = encoded {
            prop_assert_eq!(decode_ledger_record(&encoded), Ok(record));
        }
    }
}
