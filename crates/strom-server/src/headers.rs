//! Protocol header names used by the Durable Streams HTTP surface.

use axum::http::HeaderName;

/// Sliding idle TTL in seconds (`Stream-TTL`, protocol §5.1 / §5.5).
pub(crate) const STREAM_TTL: HeaderName = HeaderName::from_static("stream-ttl");

/// Absolute expiry instant (`Stream-Expires-At`, protocol §5.1 / §5.5).
pub(crate) const STREAM_EXPIRES_AT: HeaderName = HeaderName::from_static("stream-expires-at");

/// Stream closure flag (`Stream-Closed`, protocol §4.1 / §5.1–§5.5).
pub(crate) const STREAM_CLOSED: HeaderName = HeaderName::from_static("stream-closed");
