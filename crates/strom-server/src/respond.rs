//! Typed response headers for the Durable Streams surface.
//!
//! Each type owns one protocol header encoding, so handlers compose responses
//! from plain tuples and every `HeaderValue` proof lives in exactly one place.

use std::convert::Infallible;

use axum::http::{HeaderValue, Uri, header};
use axum::response::{IntoResponse, IntoResponseParts, Response, ResponseParts};
use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};

use crate::headers::{STREAM_CLOSED, STREAM_EXPIRES_AT, STREAM_TTL};

/// `Location` echoing the created stream's request path (§5.1).
#[derive(Debug)]
pub(crate) struct Location(pub(crate) Uri);

impl IntoResponseParts for Location {
    type Error = Infallible;

    #[expect(
        clippy::unwrap_in_result,
        reason = "hyper admits only visible-ASCII request paths, which are valid header values"
    )]
    fn into_response_parts(self, mut response: ResponseParts) -> Result<ResponseParts, Infallible> {
        response.headers_mut().insert(
            header::LOCATION,
            HeaderValue::from_str(self.0.path())
                .expect("request path is a valid Location header value"),
        );
        Ok(response)
    }
}

/// `Content-Type` reporting the stream's canonical content type (§5.1 / §5.5).
#[derive(Debug)]
pub(crate) struct ContentTypeHeader(pub(crate) StreamContentType);

impl IntoResponseParts for ContentTypeHeader {
    type Error = Infallible;

    #[expect(
        clippy::unwrap_in_result,
        reason = "canonical stream content types are valid header values by construction"
    )]
    fn into_response_parts(self, mut response: ResponseParts) -> Result<ResponseParts, Infallible> {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str(self.0.as_str())
                .expect("canonical stream content types are valid header values"),
        );
        Ok(response)
    }
}

/// `Stream-TTL` or `Stream-Expires-At` reporting the stream's expiry (§5.5).
#[derive(Debug, Clone, Copy)]
pub(crate) struct ExpiryHeaders(pub(crate) ExpiryPolicy);

impl IntoResponseParts for ExpiryHeaders {
    type Error = Infallible;

    #[expect(
        clippy::unwrap_in_result,
        reason = "RFC 3339 expires-at spellings are valid header values by construction"
    )]
    fn into_response_parts(self, mut response: ResponseParts) -> Result<ResponseParts, Infallible> {
        match self.0 {
            ExpiryPolicy::None => {}
            ExpiryPolicy::SlidingTtl(ttl) => {
                response
                    .headers_mut()
                    .insert(STREAM_TTL, HeaderValue::from(ttl.seconds().get()));
            }
            ExpiryPolicy::AbsoluteExpiry(expires_at) => {
                response.headers_mut().insert(
                    STREAM_EXPIRES_AT,
                    HeaderValue::from_str(&expires_at.to_string())
                        .expect("RFC 3339 expires-at values are valid header values"),
                );
            }
        }
        Ok(response)
    }
}

/// `Stream-Closed: true` when the stream refuses appends; silent when open (§4.1).
#[derive(Debug, Clone, Copy)]
pub(crate) struct ClosedHeader(pub(crate) StreamLifecycle);

impl IntoResponseParts for ClosedHeader {
    type Error = Infallible;

    fn into_response_parts(self, mut response: ResponseParts) -> Result<ResponseParts, Infallible> {
        match self.0 {
            StreamLifecycle::Open => {}
            StreamLifecycle::Closed => {
                response
                    .headers_mut()
                    .insert(STREAM_CLOSED, HeaderValue::from_static("true"));
            }
        }
        Ok(response)
    }
}

// The last element of an axum response tuple must be an `IntoResponse`;
// `ClosedHeader` closes every lifecycle tuple, so it implements both.
impl IntoResponse for ClosedHeader {
    fn into_response(self) -> Response {
        (self, ()).into_response()
    }
}
