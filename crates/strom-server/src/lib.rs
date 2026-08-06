//! HTTP server for the Durable Streams protocol lifecycle subset over `StromDB`.
//!
//! The engine does not yet support appends or data reads. This crate exposes
//! create, close, metadata, and delete, and returns 501 for append/read.

use std::sync::Arc;

use axum::Router;
use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue, header};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{any, put};
use stromdb::Db;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

pub mod config;
mod error;
mod handlers;
mod headers;
mod parse;

/// One mebibyte request-body ceiling for the lifecycle-only surface.
const REQUEST_BODY_BYTES_MAX: usize = 1024 * 1024;

/// Build the Durable Streams HTTP router over `db`.
pub fn router(db: Arc<Db>) -> Router {
    Router::new()
        .route("/__ds", any(handlers::reserved_not_found))
        .route("/__ds/{*rest}", any(handlers::reserved_not_found))
        .route(
            "/{*path}",
            put(handlers::put)
                .post(handlers::post)
                .get(handlers::get)
                .head(handlers::head)
                .delete(handlers::delete),
        )
        .layer(TraceLayer::new_for_http())
        .layer(RequestBodyLimitLayer::new(REQUEST_BODY_BYTES_MAX))
        .layer(middleware::from_fn(security_headers))
        .with_state(db)
}

/// Stamp every response with protocol §12.7 browser security headers.
async fn security_headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        HeaderName::from_static("cross-origin-resource-policy"),
        HeaderValue::from_static("cross-origin"),
    );
    response
}
