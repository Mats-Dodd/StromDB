use proptest::prelude::*;
use strom_domain::{
    CONTENT_TYPE_BYTES_MAX, ContentTypeError, ExpiresAt, ExpiresAtError, ExpiresAtRangeError,
    ExpiryPolicy, ExpiryPolicyConflict, STREAM_PATH_BYTES_MAX, StreamContentType, StreamLifecycle,
    StreamPath, StreamPathError, StreamTtl, StreamTtlError,
};

#[test]
fn stream_path_accepts_protocol_examples() -> Result<(), StreamPathError> {
    let events: StreamPath = "events/abc".parse()?;
    assert_eq!(
        "events/abc",
        events.as_str(),
        "parsing must preserve the exact path"
    );
    let wake: StreamPath = "wake/pool".parse()?;
    assert_eq!(
        "wake/pool",
        wake.as_str(),
        "parsing must preserve the exact path"
    );
    Ok(())
}

#[test]
fn stream_path_reserves_ds_root_but_not_lookalikes() {
    assert_eq!(
        Some(StreamPathError::ReservedRootSegment),
        "__ds".parse::<StreamPath>().err(),
        "§6: the bare control root is reserved"
    );
    assert_eq!(
        Some(StreamPathError::ReservedRootSegment),
        "__ds/subscriptions/sub-1".parse::<StreamPath>().err(),
        "§6: control paths must not be application stream paths"
    );
    assert_eq!(
        None,
        "__dsx/foo".parse::<StreamPath>().err(),
        "one character past the reserved segment is an ordinary stream"
    );
    assert_eq!(
        None,
        "events/__ds".parse::<StreamPath>().err(),
        "§6 reserves the first segment only"
    );
}

#[test]
fn stream_path_rejects_structural_hazards() {
    let hazards = [
        ("", StreamPathError::EmptySegment),
        ("/events", StreamPathError::EmptySegment),
        ("events/", StreamPathError::EmptySegment),
        ("events//abc", StreamPathError::EmptySegment),
        (".", StreamPathError::RelativeSegment),
        ("..", StreamPathError::RelativeSegment),
        ("events/../secrets", StreamPathError::RelativeSegment),
        ("events/\u{7}bell", StreamPathError::ControlCharacter),
        ("events/line\nbreak", StreamPathError::ControlCharacter),
    ];
    for (input, expected) in hazards {
        assert_eq!(
            Some(expected),
            input.parse::<StreamPath>().err(),
            "structural hazard must be rejected: {input:?}"
        );
    }
}

#[test]
fn stream_path_length_boundary_is_exact() {
    let at_max = "a".repeat(STREAM_PATH_BYTES_MAX);
    assert_eq!(
        None,
        at_max.parse::<StreamPath>().err(),
        "a path at the bound is valid"
    );
    let over_max = "a".repeat(STREAM_PATH_BYTES_MAX.saturating_add(1));
    assert_eq!(
        Some(StreamPathError::OverMaxBytes),
        over_max.parse::<StreamPath>().err(),
        "one byte over the bound is rejected"
    );
}

proptest! {
    #[test]
    fn stream_path_parse_is_total_and_identity(input in any::<String>()) {
        if let Ok(stream_path) = input.parse::<StreamPath>() {
            prop_assert_eq!(stream_path.as_str(), input.as_str());
        }
    }

    #[test]
    fn stream_path_valid_paths_roundtrip(
        segments in prop::collection::vec("[a-z0-9._-]{1,12}", 1..6),
    ) {
        prop_assume!(segments.iter().all(|segment| segment != "." && segment != ".."));
        prop_assume!(segments.first().map(String::as_str) != Some("__ds"));
        let path = segments.join("/");
        let parsed = path.parse::<StreamPath>();
        prop_assert!(parsed.is_ok(), "constructively valid path must parse: {:?}", parsed);
        if let Ok(stream_path) = parsed {
            prop_assert_eq!(stream_path.to_string(), path);
        }
    }

    #[test]
    fn stream_path_order_is_byte_order(
        left_segments in prop::collection::vec("[a-z0-9_-]{1,8}", 1..4),
        right_segments in prop::collection::vec("[a-z0-9_-]{1,8}", 1..4),
    ) {
        let left_path = left_segments.join("/");
        let right_path = right_segments.join("/");
        let left = left_path.parse::<StreamPath>();
        let right = right_path.parse::<StreamPath>();
        prop_assume!(left.is_ok() && right.is_ok());
        if let (Ok(left), Ok(right)) = (left, right) {
            prop_assert_eq!(left.cmp(&right), left_path.as_bytes().cmp(right_path.as_bytes()));
        }
    }
}

#[test]
fn content_type_default_is_octet_stream() -> Result<(), ContentTypeError> {
    let parsed: StreamContentType = "application/octet-stream".parse()?;
    assert_eq!(
        StreamContentType::octet_stream(),
        parsed,
        "the named default must equal its parsed spelling"
    );
    Ok(())
}

#[test]
fn content_type_mode_predicates_follow_the_essence() -> Result<(), ContentTypeError> {
    let json: StreamContentType = "application/json; charset=utf-8".parse()?;
    assert!(
        json.is_json(),
        "§9.1: charset must not defeat JSON-mode detection"
    );
    let ndjson: StreamContentType = "application/ndjson".parse()?;
    assert!(
        !ndjson.is_json(),
        "§9: ndjson is byte-oriented, not JSON mode"
    );
    let text: StreamContentType = "text/plain".parse()?;
    assert!(text.is_text(), "§5.8: text/* rides SSE without base64");
    assert!(
        !StreamContentType::octet_stream().is_text(),
        "§5.8: octet-stream data events are base64-encoded"
    );
    Ok(())
}

#[test]
fn content_type_equality_is_case_insensitive() -> Result<(), ContentTypeError> {
    let canonical: StreamContentType = "application/json;charset=utf-8".parse()?;
    let shouted: StreamContentType = "Application/JSON; Charset=UTF-8".parse()?;
    assert_eq!(
        canonical, shouted,
        "§5.1: case variants are one configuration"
    );
    Ok(())
}

#[test]
fn content_type_rejects_malformed_and_unknown_parameters() {
    let malformed = [
        "",
        "application",
        "application/",
        "/json",
        "application/json;",
        "application/json; charset",
        "application/json; charset=utf-8; charset=utf-8",
        "application/js on",
    ];
    for input in malformed {
        assert_eq!(
            Some(ContentTypeError::Malformed),
            input.parse::<StreamContentType>().err(),
            "not `type/subtype [; charset=token]`: {input:?}"
        );
    }
    assert_eq!(
        Some(ContentTypeError::UnsupportedParameter),
        "multipart/form-data; boundary=x"
            .parse::<StreamContentType>()
            .err(),
        "only the charset parameter is understood in version 1"
    );
}

#[test]
fn content_type_length_boundary_is_exact() {
    let subtype_length = CONTENT_TYPE_BYTES_MAX.saturating_sub("application/".len());
    let at_max = format!("application/{}", "a".repeat(subtype_length));
    assert_eq!(
        None,
        at_max.parse::<StreamContentType>().err(),
        "a value at the bound is valid"
    );
    let over_max = format!(
        "application/{}",
        "a".repeat(subtype_length.saturating_add(1))
    );
    assert_eq!(
        Some(ContentTypeError::OverMaxBytes),
        over_max.parse::<StreamContentType>().err(),
        "one byte over the bound is rejected"
    );
}

#[test]
fn content_type_bound_covers_its_canonical_spelling() {
    let source = format!("a/{};charset=x", "b".repeat(244));
    assert_eq!(
        CONTENT_TYPE_BYTES_MAX,
        source.len(),
        "the regression input reaches the request bound exactly"
    );
    assert_eq!(
        Err(ContentTypeError::OverMaxBytes),
        source.parse::<StreamContentType>(),
        "normalizing parameter whitespace must not manufacture an over-bound domain value"
    );
}

proptest! {
    #[test]
    fn content_type_parse_is_total_and_display_reparses(input in any::<String>()) {
        if let Ok(content_type) = input.parse::<StreamContentType>() {
            let redisplayed = content_type.to_string();
            prop_assert_eq!(redisplayed.parse::<StreamContentType>().ok(), Some(content_type));
        }
    }

    #[test]
    fn content_type_valid_inputs_parse_and_case_fold(
        type_raw in "[a-z]{1,10}",
        subtype_raw in "[a-z0-9.+-]{1,10}",
        charset in prop::option::of("[a-z0-9-]{1,8}"),
    ) {
        let source = match &charset {
            Some(value) => format!("{type_raw}/{subtype_raw}; charset={value}"),
            None => format!("{type_raw}/{subtype_raw}"),
        };
        let parsed = source.parse::<StreamContentType>();
        prop_assert!(parsed.is_ok(), "constructively valid media type must parse: {:?}", parsed);
        let uppercased = source.to_ascii_uppercase().parse::<StreamContentType>();
        prop_assert_eq!(uppercased.ok(), parsed.ok());
    }
}

#[test]
fn ttl_spec_anchors_from_section_5_1() {
    assert_eq!(
        Ok(3600),
        "3600".parse::<StreamTtl>().map(|ttl| ttl.seconds().get()),
        "§5.1: `3600` is the valid example"
    );
    for invalid in ["+3600", "03600", "3600.0", "3.6e3"] {
        assert_eq!(
            Err(StreamTtlError::Malformed),
            invalid.parse::<StreamTtl>(),
            "§5.1 lists {invalid:?} as invalid"
        );
    }
}

#[test]
fn ttl_value_boundaries_are_exact() {
    assert_eq!(
        Err(StreamTtlError::Zero),
        "0".parse::<StreamTtl>(),
        "a zero idle window is dead on arrival and always a client bug"
    );
    assert_eq!(
        Ok(1),
        "1".parse::<StreamTtl>().map(|ttl| ttl.seconds().get()),
        "one second is the smallest window"
    );
    assert_eq!(
        Ok(u64::MAX),
        u64::MAX
            .to_string()
            .parse::<StreamTtl>()
            .map(|ttl| ttl.seconds().get()),
        "the representable maximum parses"
    );
    assert_eq!(
        Err(StreamTtlError::OverMax),
        "18446744073709551616".parse::<StreamTtl>(),
        "one past u64::MAX is rejected, never wrapped"
    );
}

#[test]
fn ttl_from_nonzero_agrees_with_parsing() -> Result<(), Box<dyn std::error::Error>> {
    let seconds = std::num::NonZeroU64::new(3600).ok_or("3600 is nonzero")?;
    let widened = StreamTtl::from(seconds);
    let parsed: StreamTtl = "3600".parse()?;
    assert_eq!(widened, parsed, "one idle window, two construction paths");
    Ok(())
}

proptest! {
    #[test]
    fn ttl_roundtrips_for_all_nonzero_values(seconds in 1u64..) {
        let source = seconds.to_string();
        let parsed = source.parse::<StreamTtl>();
        prop_assert_eq!(parsed.map(|ttl| ttl.seconds().get()), Ok(seconds));
        if let Ok(ttl) = source.parse::<StreamTtl>() {
            prop_assert_eq!(ttl.to_string(), source);
        }
    }

    /// §5.1 spells a TTL exactly one way, so an accepted header is already its
    /// own canonical form. This rejects any parser that quietly tolerates
    /// `+3600`, `03600`, or surrounding whitespace.
    #[test]
    fn ttl_accepts_only_its_own_canonical_spelling(input in any::<String>()) {
        if let Ok(ttl) = input.parse::<StreamTtl>() {
            prop_assert_eq!(ttl.to_string(), input);
        }
    }
}

#[test]
fn expires_at_compares_instants_not_spellings() -> Result<(), ExpiresAtError> {
    let utc: ExpiresAt = "2030-01-01T00:00:00Z".parse()?;
    let offset: ExpiresAt = "2030-01-01T01:00:00+01:00".parse()?;
    assert_eq!(
        utc, offset,
        "RFC 3339 offsets denote instants, not local labels"
    );
    let later: ExpiresAt = "2030-01-01T00:00:01Z".parse()?;
    assert!(utc < later, "ordering must follow the instant");
    Ok(())
}

#[test]
fn expires_at_rejects_non_instants() {
    let non_instants = [
        "",
        "2030-01-01",
        "2030-01-01T00:00:00",
        "2030-01-01T00:00:00Z[Europe/Paris]",
        "now",
        "not-a-date",
    ];
    for input in non_instants {
        assert_eq!(
            Err(ExpiresAtError),
            input.parse::<ExpiresAt>(),
            "not an RFC 3339 instant: {input:?}"
        );
    }
}

#[test]
fn expires_at_nanosecond_range_is_exact() -> Result<(), Box<dyn std::error::Error>> {
    let epoch = ExpiresAt::try_from(0i128)?;
    let parsed: ExpiresAt = "1970-01-01T00:00:00Z".parse()?;
    assert_eq!(
        epoch, parsed,
        "nanosecond zero names the Unix epoch instant"
    );
    assert_eq!(
        Err(ExpiresAtRangeError),
        ExpiresAt::try_from(i128::MAX),
        "a count above the instant range is rejected"
    );
    assert_eq!(
        Err(ExpiresAtRangeError),
        ExpiresAt::try_from(i128::MIN),
        "a count below the instant range is rejected"
    );
    Ok(())
}

proptest! {
    #[test]
    fn expires_at_roundtrips_canonical_utc(
        year in 1970i32..=9999i32,
        month in 1u32..=12,
        day in 1u32..=28,
        hour in 0u32..=23,
        minute in 0u32..=59,
        second in 0u32..=59,
    ) {
        let source =
            format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z");
        let parsed = source.parse::<ExpiresAt>();
        prop_assert!(parsed.is_ok(), "constructively valid instant must parse: {}", source);
        if let Ok(expires_at) = parsed {
            prop_assert_eq!(expires_at.to_string(), source);
        }
    }

    /// An instant has many spellings but one canonical rendering, and that
    /// rendering must be one this same parser accepts. Rejects a `Display` that
    /// emits a form `FromStr` refuses, such as an RFC 9557 zone annotation.
    #[test]
    fn expires_at_display_reparses_to_the_same_instant(input in any::<String>()) {
        if let Ok(expires_at) = input.parse::<ExpiresAt>() {
            let redisplayed = expires_at.to_string();
            prop_assert_eq!(redisplayed.parse::<ExpiresAt>().ok(), Some(expires_at));
        }
    }
}

#[test]
fn expiry_policy_enumerates_header_combinations() -> Result<(), Box<dyn std::error::Error>> {
    let ttl: StreamTtl = "60".parse()?;
    let expires_at: ExpiresAt = "2030-01-01T00:00:00Z".parse()?;
    assert_eq!(
        Ok(ExpiryPolicy::None),
        ExpiryPolicy::try_from((None::<StreamTtl>, None::<ExpiresAt>)),
        "no expiry headers means the stream never expires"
    );
    assert_eq!(
        Ok(ExpiryPolicy::SlidingTtl(ttl)),
        ExpiryPolicy::try_from((Some(ttl), None)),
        "Stream-TTL alone selects the sliding window"
    );
    assert_eq!(
        Ok(ExpiryPolicy::AbsoluteExpiry(expires_at)),
        ExpiryPolicy::try_from((None, Some(expires_at))),
        "Stream-Expires-At alone selects the absolute deadline"
    );
    assert_eq!(
        Err(ExpiryPolicyConflict),
        ExpiryPolicy::try_from((Some(ttl), Some(expires_at))),
        "§5.1: both headers together are rejected"
    );
    Ok(())
}

#[test]
fn lifecycle_close_is_monotonic_and_idempotent() {
    assert_eq!(
        StreamLifecycle::Closed,
        StreamLifecycle::Open.close(),
        "closing an open stream closes it"
    );
    assert_eq!(
        StreamLifecycle::Closed,
        StreamLifecycle::Closed.close(),
        "§4.1: closing a closed stream is idempotent success"
    );
    assert!(
        !StreamLifecycle::Open.is_closed(),
        "§4: an open stream accepts appends"
    );
    assert!(
        StreamLifecycle::Closed.is_closed(),
        "§4.1: a closed stream rejects appends"
    );
}
