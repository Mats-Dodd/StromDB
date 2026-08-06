//! HTTP API errors with plain-text bodies.
//!
//! | Status | Variant |
//! |--------|---------|
//! | 400 | [`BadRequest`](ApiError::BadRequest) |
//! | 404 | [`NotFound`](ApiError::NotFound) |
//! | 409 | [`Conflict`](ApiError::Conflict) |
//! | 410 | [`Gone`](ApiError::Gone) |
//! | 413 | body-limit layer (not this type) |
//! | 429 | [`TooManyRequests`](ApiError::TooManyRequests) |
//! | 500 | [`Indeterminate`](ApiError::Indeterminate) |
//! | 501 | [`NotImplemented`](ApiError::NotImplemented) |
//! | 503 | [`Unavailable`](ApiError::Unavailable) |

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use strom_db::StreamError;

/// A protocol-facing refusal or failure at the HTTP edge.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("Not found")]
    NotFound,
    #[error("{0}")]
    Conflict(String),
    #[error("Gone")]
    Gone,
    #[error("Too many requests")]
    TooManyRequests,
    #[error("operation outcome is indeterminate")]
    Indeterminate,
    #[error("{0}")]
    NotImplemented(String),
    #[error("Unavailable")]
    Unavailable,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Gone => StatusCode::GONE,
            Self::TooManyRequests => StatusCode::TOO_MANY_REQUESTS,
            Self::Indeterminate => StatusCode::INTERNAL_SERVER_ERROR,
            Self::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        };
        let body = self.to_string();
        (
            status,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            )],
            body,
        )
            .into_response()
    }
}

impl From<StreamError> for ApiError {
    fn from(error: StreamError) -> Self {
        match error {
            StreamError::Occupied => {
                Self::Conflict("stream exists with a different configuration".to_owned())
            }
            StreamError::CapacityExhausted | StreamError::Overloaded => Self::TooManyRequests,
            StreamError::Unavailable => Self::Unavailable,
            StreamError::Indeterminate => Self::Indeterminate,
            // Call sites must split 404/410 via a status read before converting.
            StreamError::NotLive => Self::NotFound,
        }
    }
}
