//! Parse foreign HTTP bytes into domain values at the edge.
//!
//! No storage lookups. Domain `FromStr` / `TryFrom` owns every validation rule.

use axum::http::{HeaderMap, header};
use strom_domain::{ExpiresAt, ExpiryPolicy, StreamContentType, StreamTtl};
use stromdb::StreamId;

use crate::error::ApiError;
use crate::headers::{STREAM_CLOSED, STREAM_EXPIRES_AT, STREAM_TTL};

/// Parse a stream-root-relative path into a [`StreamId`].
///
/// # Errors
///
/// Returns [`ApiError::BadRequest`] when the path is not a valid stream id.
pub(crate) fn stream_id(path: &str) -> Result<StreamId, ApiError> {
    path.parse()
        .map_err(|error: stromdb::StreamIdError| ApiError::BadRequest(error.to_string()))
}

/// Parse `Content-Type`, defaulting to `application/octet-stream` when absent.
///
/// # Errors
///
/// Returns [`ApiError::BadRequest`] when the header is present but invalid.
pub(crate) fn content_type(headers: &HeaderMap) -> Result<StreamContentType, ApiError> {
    match headers.get(header::CONTENT_TYPE) {
        None => Ok(StreamContentType::octet_stream()),
        Some(value) => {
            let text = value.to_str().map_err(|_invalid| {
                ApiError::BadRequest("invalid Content-Type encoding".to_owned())
            })?;
            text.parse()
                .map_err(|error: strom_domain::ContentTypeError| {
                    ApiError::BadRequest(error.to_string())
                })
        }
    }
}

/// Parse optional `Stream-TTL` / `Stream-Expires-At` into an [`ExpiryPolicy`].
///
/// # Errors
///
/// Returns [`ApiError::BadRequest`] on malformed values or the both-present conflict.
pub(crate) fn expiry(headers: &HeaderMap) -> Result<ExpiryPolicy, ApiError> {
    let ttl = match headers.get(STREAM_TTL) {
        None => None,
        Some(value) => {
            let text = value.to_str().map_err(|_invalid| {
                ApiError::BadRequest("invalid Stream-TTL encoding".to_owned())
            })?;
            Some(
                text.parse::<StreamTtl>()
                    .map_err(|error| ApiError::BadRequest(error.to_string()))?,
            )
        }
    };
    let expires_at = match headers.get(STREAM_EXPIRES_AT) {
        None => None,
        Some(value) => {
            let text = value.to_str().map_err(|_invalid| {
                ApiError::BadRequest("invalid Stream-Expires-At encoding".to_owned())
            })?;
            Some(
                text.parse::<ExpiresAt>()
                    .map_err(|error| ApiError::BadRequest(error.to_string()))?,
            )
        }
    };
    ExpiryPolicy::try_from((ttl, expires_at))
        .map_err(|error| ApiError::BadRequest(error.to_string()))
}

/// True only when `Stream-Closed` is exactly `true` (case-insensitive, §4.1).
#[must_use]
pub(crate) fn stream_closed(headers: &HeaderMap) -> bool {
    headers
        .get(STREAM_CLOSED)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header};

    use super::{content_type, expiry, stream_closed, stream_id};
    use crate::error::ApiError;
    use crate::headers::{STREAM_CLOSED, STREAM_EXPIRES_AT, STREAM_TTL};

    #[test]
    fn stream_id_rejects_reserved_root() {
        assert!(matches!(
            stream_id("__ds/subscriptions/x"),
            Err(ApiError::BadRequest(_))
        ));
    }

    #[test]
    fn stream_id_accepts_nested_path() {
        let id = stream_id("events/a").expect("valid stream id");
        assert_eq!("events/a", id.as_str());
    }

    #[test]
    fn content_type_defaults_to_octet_stream() {
        let headers = HeaderMap::new();
        let parsed = content_type(&headers).expect("default content type");
        assert_eq!("application/octet-stream", parsed.as_str());
    }

    #[test]
    fn content_type_parses_present_header() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        let parsed = content_type(&headers).expect("valid content type");
        assert_eq!("text/plain", parsed.as_str());
    }

    #[test]
    fn ttl_with_leading_zero_is_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert(STREAM_TTL, HeaderValue::from_static("03600"));
        assert!(matches!(expiry(&headers), Err(ApiError::BadRequest(_))));
    }

    #[test]
    fn both_expiry_headers_are_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert(STREAM_TTL, HeaderValue::from_static("3600"));
        headers.insert(
            STREAM_EXPIRES_AT,
            HeaderValue::from_static("2030-01-01T00:00:00Z"),
        );
        assert!(matches!(expiry(&headers), Err(ApiError::BadRequest(_))));
    }

    #[test]
    fn sliding_ttl_parses() {
        let mut headers = HeaderMap::new();
        headers.insert(STREAM_TTL, HeaderValue::from_static("3600"));
        let policy = expiry(&headers).expect("valid sliding ttl");
        assert!(matches!(policy, strom_domain::ExpiryPolicy::SlidingTtl(_)));
    }

    #[test]
    fn stream_closed_true_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert(STREAM_CLOSED, HeaderValue::from_static("TRUE"));
        assert!(stream_closed(&headers));
    }

    #[test]
    fn stream_closed_yes_is_ignored() {
        let mut headers = HeaderMap::new();
        headers.insert(STREAM_CLOSED, HeaderValue::from_static("yes"));
        assert!(!stream_closed(&headers));
    }
}
