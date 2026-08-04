//! Proptest strategies for valid domain values (feature `proptest`).
//!
//! Every strategy builds a value that is valid by construction and then states
//! that as an assertion rather than a filter. A filter would answer a broken
//! parser by quietly generating fewer values; the assertion names the rule that
//! the parser stopped honouring.

use std::num::NonZeroU64;

use jiff::Timestamp;
use proptest::prelude::{Just, Strategy, prop_oneof};

use crate::{ExpiresAt, ExpiryPolicy, StreamContentType, StreamId, StreamLifecycle, StreamTtl};

/// # Panics
///
/// Panics if a path of alphanumeric segments stops parsing as a stream id.
pub fn stream_id() -> impl Strategy<Value = StreamId> {
    proptest::collection::vec("[a-z0-9]{1,8}", 1..4usize).prop_map(|segments| {
        segments
            .join("/")
            .parse::<StreamId>()
            .expect("alphanumeric segments joined by `/` are neither empty, relative, nor reserved")
    })
}

/// # Panics
///
/// Panics if a `token/token` essence with a token charset stops parsing.
pub fn stream_content_type() -> impl Strategy<Value = StreamContentType> {
    let essence = ("[a-z]{1,10}", "[a-z0-9.+-]{1,10}");
    let charset = proptest::option::of("[a-z0-9-]{1,8}");
    (essence, charset).prop_map(|((type_raw, subtype_raw), charset)| {
        let source = match charset {
            Some(value) => format!("{type_raw}/{subtype_raw}; charset={value}"),
            None => format!("{type_raw}/{subtype_raw}"),
        };
        source
            .parse::<StreamContentType>()
            .expect("a token type, token subtype, and optional token charset are a media type")
    })
}

/// # Panics
///
/// Panics if a second count drawn from one upwards stops being nonzero.
pub fn stream_ttl() -> impl Strategy<Value = StreamTtl> {
    (1u64..=u64::MAX).prop_map(|seconds| {
        StreamTtl::from(NonZeroU64::new(seconds).expect("the range starts at one second"))
    })
}

/// # Panics
///
/// Panics if a nanosecond count inside the instant range stops converting.
pub fn expires_at() -> impl Strategy<Value = ExpiresAt> {
    (Timestamp::MIN.as_nanosecond()..=Timestamp::MAX.as_nanosecond()).prop_map(|unix_nanoseconds| {
        ExpiresAt::try_from(unix_nanoseconds)
            .expect("the range spans exactly the representable instants")
    })
}

pub fn expiry_policy() -> impl Strategy<Value = ExpiryPolicy> {
    prop_oneof![
        Just(ExpiryPolicy::None),
        stream_ttl().prop_map(ExpiryPolicy::SlidingTtl),
        expires_at().prop_map(ExpiryPolicy::AbsoluteExpiry),
    ]
}

pub fn stream_lifecycle() -> impl Strategy<Value = StreamLifecycle> {
    prop_oneof![Just(StreamLifecycle::Open), Just(StreamLifecycle::Closed)]
}
