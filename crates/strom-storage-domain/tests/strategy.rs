#![cfg(feature = "proptest")]

use proptest::prelude::*;
use strom_storage_domain::{
    decode_seal, decode_stream_record, decode_wal, encode_seal, encode_stream_record, encode_wal,
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
    fn generated_stream_records_roundtrip_through_the_durable_codec(
        record in strategy::stream_record(),
    ) {
        let encoded = encode_stream_record(&record);
        prop_assert!(encoded.is_ok());
        if let Ok(encoded) = encoded {
            prop_assert_eq!(decode_stream_record(&encoded), Ok(record));
        }
    }
}
