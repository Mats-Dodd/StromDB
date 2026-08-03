//! Proptest strategies that construct valid domain values.
//!
//! Compiled only under the `proptest` feature. Every strategy goes through
//! the type's canonical constructor, so a generated value is valid by the
//! same rules production parsing applies — downstream crates never copy the
//! grammar. Strategies deliberately explore a modest, readable subspace;
//! totality over hostile input is the parsers' own test surface.

use std::num::NonZeroU64;

use jiff::Timestamp;
use proptest::prelude::{Just, Strategy, prop_oneof};

use crate::{ExpiresAt, ExpiryPolicy, StreamContentType, StreamId, StreamLifecycle, StreamTtl};

/// Valid stream ids: one to three lowercase alphanumeric segments. The
/// alphabet cannot spell `.`, `..`, or `__ds`, so every draw parses.
pub fn stream_id() -> impl Strategy<Value = StreamId> {
    proptest::collection::vec("[a-z0-9]{1,8}", 1..4usize)
        .prop_filter_map("joined segments must parse as a stream id", |segments| {
            segments.join("/").parse().ok()
        })
}

/// Valid content types: a lowercase essence with an optional charset.
pub fn stream_content_type() -> impl Strategy<Value = StreamContentType> {
    let essence = ("[a-z]{1,10}", "[a-z0-9.+-]{1,10}");
    let charset = proptest::option::of("[a-z0-9-]{1,8}");
    (essence, charset).prop_filter_map(
        "constructed media type must parse",
        |((type_raw, subtype_raw), charset)| {
            let source = match charset {
                Some(value) => format!("{type_raw}/{subtype_raw}; charset={value}"),
                None => format!("{type_raw}/{subtype_raw}"),
            };
            source.parse().ok()
        },
    )
}

/// Valid idle windows: every nonzero second count.
pub fn stream_ttl() -> impl Strategy<Value = StreamTtl> {
    (1u64..=u64::MAX).prop_filter_map("nonzero seconds form a TTL", |seconds| {
        NonZeroU64::new(seconds).map(StreamTtl::from)
    })
}

/// Valid absolute expiry instants across the full representable range.
pub fn expires_at() -> impl Strategy<Value = ExpiresAt> {
    (Timestamp::MIN.as_nanosecond()..=Timestamp::MAX.as_nanosecond())
        .prop_filter_map("in-range nanoseconds form an instant", |unix_nanoseconds| {
            ExpiresAt::try_from(unix_nanoseconds).ok()
        })
}

/// All three expiry policies, weighted evenly.
pub fn expiry_policy() -> impl Strategy<Value = ExpiryPolicy> {
    prop_oneof![
        Just(ExpiryPolicy::None),
        stream_ttl().prop_map(ExpiryPolicy::SlidingTtl),
        expires_at().prop_map(ExpiryPolicy::AbsoluteExpiry),
    ]
}

/// Both lifecycle states.
pub fn stream_lifecycle() -> impl Strategy<Value = StreamLifecycle> {
    prop_oneof![Just(StreamLifecycle::Open), Just(StreamLifecycle::Closed)]
}
