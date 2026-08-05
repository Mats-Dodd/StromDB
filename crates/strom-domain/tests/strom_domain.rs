use proptest::prelude::*;
use strom_domain::{
    CONTENT_TYPE_BYTES_MAX, ContentTypeError, ExpiresAt, ExpiresAtError, ExpiresAtRangeError,
    ExpiryPolicy, ExpiryPolicyConflict, STREAM_ID_BYTES_MAX, StreamContentType, StreamId,
    StreamIdError, StreamLifecycle, StreamTtl, StreamTtlError,
};

#[test]
fn stream_id_accepts_protocol_example_paths() -> Result<(), StreamIdError> {
    let events: StreamId = "events/abc".parse()?;
    assert_eq!(
        events.as_str(),
        "events/abc",
        "parsing must preserve the exact path"
    );
    let wake: StreamId = "wake/pool".parse()?;
    assert_eq!(
        wake.as_str(),
        "wake/pool",
        "parsing must preserve the exact path"
    );
    Ok(())
}

#[test]
fn stream_id_reserves_ds_root_but_not_lookalikes() {
    assert_eq!(
        "__ds".parse::<StreamId>().err(),
        Some(StreamIdError::ReservedRootSegment),
        "§6: the bare control root is reserved"
    );
    assert_eq!(
        "__ds/subscriptions/sub-1".parse::<StreamId>().err(),
        Some(StreamIdError::ReservedRootSegment),
        "§6: control paths must not be stream ids"
    );
    assert_eq!(
        "__dsx/foo".parse::<StreamId>().err(),
        None,
        "one character past the reserved segment is an ordinary stream"
    );
    assert_eq!(
        "events/__ds".parse::<StreamId>().err(),
        None,
        "§6 reserves the first segment only"
    );
}

#[test]
fn stream_id_rejects_structural_hazards() {
    let hazards = [
        ("", StreamIdError::EmptySegment),
        ("/events", StreamIdError::EmptySegment),
        ("events/", StreamIdError::EmptySegment),
        ("events//abc", StreamIdError::EmptySegment),
        (".", StreamIdError::RelativeSegment),
        ("..", StreamIdError::RelativeSegment),
        ("events/../secrets", StreamIdError::RelativeSegment),
        ("events/\u{7}bell", StreamIdError::ControlCharacter),
        ("events/line\nbreak", StreamIdError::ControlCharacter),
    ];
    for (input, expected) in hazards {
        assert_eq!(
            input.parse::<StreamId>().err(),
            Some(expected),
            "structural hazard must be rejected: {input:?}"
        );
    }
}

#[test]
fn stream_id_length_boundary_is_exact() {
    let at_max = "a".repeat(STREAM_ID_BYTES_MAX);
    assert_eq!(
        at_max.parse::<StreamId>().err(),
        None,
        "an id at the bound is valid"
    );
    let over_max = "a".repeat(STREAM_ID_BYTES_MAX.saturating_add(1));
    assert_eq!(
        over_max.parse::<StreamId>().err(),
        Some(StreamIdError::OverMaxBytes),
        "one byte over the bound is rejected"
    );
}

proptest! {
    #[test]
    fn stream_id_parse_is_total_and_identity(input in any::<String>()) {
        if let Ok(stream_id) = input.parse::<StreamId>() {
            prop_assert_eq!(stream_id.as_str(), input.as_str());
        }
    }

    #[test]
    fn stream_id_valid_paths_roundtrip(
        segments in prop::collection::vec("[a-z0-9._-]{1,12}", 1..6),
    ) {
        prop_assume!(segments.iter().all(|segment| segment != "." && segment != ".."));
        prop_assume!(segments.first().map(String::as_str) != Some("__ds"));
        let path = segments.join("/");
        let parsed = path.parse::<StreamId>();
        prop_assert!(parsed.is_ok(), "constructively valid path must parse: {:?}", parsed);
        if let Ok(stream_id) = parsed {
            prop_assert_eq!(stream_id.to_string(), path);
        }
    }

    #[test]
    fn stream_id_order_is_byte_order(
        left_segments in prop::collection::vec("[a-z0-9_-]{1,8}", 1..4),
        right_segments in prop::collection::vec("[a-z0-9_-]{1,8}", 1..4),
    ) {
        let left_path = left_segments.join("/");
        let right_path = right_segments.join("/");
        let left = left_path.parse::<StreamId>();
        let right = right_path.parse::<StreamId>();
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
            input.parse::<StreamContentType>().err(),
            Some(ContentTypeError::Malformed),
            "not `type/subtype [; charset=token]`: {input:?}"
        );
    }
    assert_eq!(
        "multipart/form-data; boundary=x"
            .parse::<StreamContentType>()
            .err(),
        Some(ContentTypeError::UnsupportedParameter),
        "only the charset parameter is understood in version 1"
    );
}

#[test]
fn content_type_length_boundary_is_exact() {
    let subtype_length = CONTENT_TYPE_BYTES_MAX.saturating_sub("application/".len());
    let at_max = format!("application/{}", "a".repeat(subtype_length));
    assert_eq!(
        at_max.parse::<StreamContentType>().err(),
        None,
        "a value at the bound is valid"
    );
    let over_max = format!(
        "application/{}",
        "a".repeat(subtype_length.saturating_add(1))
    );
    assert_eq!(
        over_max.parse::<StreamContentType>().err(),
        Some(ContentTypeError::OverMaxBytes),
        "one byte over the bound is rejected"
    );
}

#[test]
fn content_type_bound_covers_its_canonical_spelling() {
    let source = format!("a/{};charset=x", "b".repeat(244));
    assert_eq!(
        source.len(),
        CONTENT_TYPE_BYTES_MAX,
        "the regression input reaches the request bound exactly"
    );
    assert_eq!(
        source.parse::<StreamContentType>(),
        Err(ContentTypeError::OverMaxBytes),
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
        "3600".parse::<StreamTtl>().map(|ttl| ttl.seconds().get()),
        Ok(3600),
        "§5.1: `3600` is the valid example"
    );
    for invalid in ["+3600", "03600", "3600.0", "3.6e3"] {
        assert_eq!(
            invalid.parse::<StreamTtl>(),
            Err(StreamTtlError::Malformed),
            "§5.1 lists {invalid:?} as invalid"
        );
    }
}

#[test]
fn ttl_value_boundaries_are_exact() {
    assert_eq!(
        "0".parse::<StreamTtl>(),
        Err(StreamTtlError::Zero),
        "a zero idle window is dead on arrival and always a client bug"
    );
    assert_eq!(
        "1".parse::<StreamTtl>().map(|ttl| ttl.seconds().get()),
        Ok(1),
        "one second is the smallest window"
    );
    assert_eq!(
        u64::MAX
            .to_string()
            .parse::<StreamTtl>()
            .map(|ttl| ttl.seconds().get()),
        Ok(u64::MAX),
        "the representable maximum parses"
    );
    assert_eq!(
        "18446744073709551616".parse::<StreamTtl>(),
        Err(StreamTtlError::OverMax),
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
            input.parse::<ExpiresAt>(),
            Err(ExpiresAtError),
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
        ExpiresAt::try_from(i128::MAX),
        Err(ExpiresAtRangeError),
        "a count above the instant range is rejected"
    );
    assert_eq!(
        ExpiresAt::try_from(i128::MIN),
        Err(ExpiresAtRangeError),
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
        ExpiryPolicy::try_from((None::<StreamTtl>, None::<ExpiresAt>)),
        Ok(ExpiryPolicy::None),
        "no expiry headers means the stream never expires"
    );
    assert_eq!(
        ExpiryPolicy::try_from((Some(ttl), None)),
        Ok(ExpiryPolicy::SlidingTtl(ttl)),
        "Stream-TTL alone selects the sliding window"
    );
    assert_eq!(
        ExpiryPolicy::try_from((None, Some(expires_at))),
        Ok(ExpiryPolicy::AbsoluteExpiry(expires_at)),
        "Stream-Expires-At alone selects the absolute deadline"
    );
    assert_eq!(
        ExpiryPolicy::try_from((Some(ttl), Some(expires_at))),
        Err(ExpiryPolicyConflict),
        "§5.1: both headers together are rejected"
    );
    Ok(())
}

#[test]
fn lifecycle_close_is_monotonic_and_idempotent() {
    assert_eq!(
        StreamLifecycle::Open.close(),
        StreamLifecycle::Closed,
        "closing an open stream closes it"
    );
    assert_eq!(
        StreamLifecycle::Closed.close(),
        StreamLifecycle::Closed,
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
