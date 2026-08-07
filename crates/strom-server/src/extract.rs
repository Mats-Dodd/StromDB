//! Typed extractors that parse foreign HTTP bytes into domain values at the edge.
//!
//! No storage lookups. Domain `FromStr` / `TryFrom` owns every validation rule;
//! each fallible extractor rejects with a protocol 400 before its handler runs.

use std::convert::Infallible;

use axum::extract::{FromRequestParts, Path};
use axum::http::request::Parts;
use axum::http::{HeaderMap, header};
use strom_db::StreamPath;
use strom_domain::{ExpiresAt, ExpiryPolicy, StreamContentType, StreamLifecycle, StreamTtl};

use crate::error::ApiError;
use crate::headers::{STREAM_CLOSED, STREAM_EXPIRES_AT, STREAM_TTL};

/// The stream-root-relative request path parsed into a [`StreamPath`].
#[derive(Debug)]
pub(crate) struct RequestStreamPath(pub(crate) StreamPath);

impl<S: Send + Sync> FromRequestParts<S> for RequestStreamPath {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Path(path) = Path::<String>::from_request_parts(parts, state)
            .await
            .map_err(|rejection| ApiError::BadRequest(rejection.to_string()))?;
        stream_path(&path).map(Self)
    }
}

/// `Content-Type` parsed into a [`StreamContentType`]; absent defaults to
/// `application/octet-stream` (§5.1).
#[derive(Debug)]
pub(crate) struct RequestContentType(pub(crate) StreamContentType);

impl<S: Send + Sync> FromRequestParts<S> for RequestContentType {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        content_type(&parts.headers).map(Self)
    }
}

/// Optional `Stream-TTL` / `Stream-Expires-At` parsed into an [`ExpiryPolicy`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct Expiry(pub(crate) ExpiryPolicy);

impl<S: Send + Sync> FromRequestParts<S> for Expiry {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        expiry(&parts.headers).map(Self)
    }
}

/// `Stream-Closed` parsed into a [`StreamLifecycle`]: exactly `true`
/// (case-insensitive, §4.1) closes; anything else leaves the stream open.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Lifecycle(pub(crate) StreamLifecycle);

impl<S: Send + Sync> FromRequestParts<S> for Lifecycle {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self(lifecycle(&parts.headers)))
    }
}

fn stream_path(path: &str) -> Result<StreamPath, ApiError> {
    path.parse()
        .map_err(|error: strom_db::StreamPathError| ApiError::BadRequest(error.to_string()))
}

fn content_type(headers: &HeaderMap) -> Result<StreamContentType, ApiError> {
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

fn expiry(headers: &HeaderMap) -> Result<ExpiryPolicy, ApiError> {
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

fn lifecycle(headers: &HeaderMap) -> StreamLifecycle {
    let closed = headers
        .get(STREAM_CLOSED)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    if closed {
        StreamLifecycle::Closed
    } else {
        StreamLifecycle::Open
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header};
    use strom_domain::StreamLifecycle;

    use super::{content_type, expiry, lifecycle, stream_path};
    use crate::error::ApiError;
    use crate::headers::{STREAM_CLOSED, STREAM_EXPIRES_AT, STREAM_TTL};

    #[test]
    fn stream_path_rejects_reserved_root() {
        assert!(matches!(
            stream_path("__ds/subscriptions/x"),
            Err(ApiError::BadRequest(_))
        ));
    }

    #[test]
    fn stream_path_accepts_nested_path() {
        let path = stream_path("events/a").expect("valid stream path");
        assert_eq!("events/a", path.as_str());
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
    fn lifecycle_true_is_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert(STREAM_CLOSED, HeaderValue::from_static("TRUE"));
        assert_eq!(StreamLifecycle::Closed, lifecycle(&headers));
    }

    #[test]
    fn lifecycle_yes_is_ignored() {
        let mut headers = HeaderMap::new();
        headers.insert(STREAM_CLOSED, HeaderValue::from_static("yes"));
        assert_eq!(StreamLifecycle::Open, lifecycle(&headers));
    }
}
