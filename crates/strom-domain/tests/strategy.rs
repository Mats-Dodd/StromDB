#![cfg(feature = "proptest")]
//! The `proptest` feature promises downstream crates valid domain values
//! without a copy of the parsing rules. Each property here holds a strategy to
//! that promise: the value it draws survives a round trip through the canonical
//! parser for its type, so a drift between generator and parser fails here
//! rather than inside an unrelated downstream test.

use proptest::prelude::*;
use strom_domain::{ExpiresAt, StreamContentType, StreamPath, StreamTtl, strategy};

proptest! {
    #[test]
    fn generated_stream_paths_reparse(stream_path in strategy::stream_path()) {
        let reparsed = stream_path.as_str().parse::<StreamPath>();
        prop_assert_eq!(reparsed.ok(), Some(stream_path));
    }

    #[test]
    fn generated_content_types_reparse(content_type in strategy::stream_content_type()) {
        let reparsed = content_type.to_string().parse::<StreamContentType>();
        prop_assert_eq!(reparsed.ok(), Some(content_type));
    }

    #[test]
    fn generated_ttls_reparse(ttl in strategy::stream_ttl()) {
        let reparsed = ttl.to_string().parse::<StreamTtl>();
        prop_assert_eq!(reparsed.ok(), Some(ttl));
    }

    #[test]
    fn generated_expiry_instants_reparse(expires_at in strategy::expires_at()) {
        let reparsed = expires_at.to_string().parse::<ExpiresAt>();
        prop_assert_eq!(reparsed.ok(), Some(expires_at));
    }
}
