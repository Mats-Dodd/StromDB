//! HTTP server for the Durable Streams protocol lifecycle subset over `StromDB`.
//!
//! The engine does not yet support appends or data reads. This crate exposes
//! create, close, metadata, and delete, and returns 501 for append/read.

use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderName, HeaderValue, header};
use axum::routing::{any, put};
use strom_db::Db;
use tower::ServiceBuilder;
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::TraceLayer;

pub mod config;
mod error;
mod extract;
mod handlers;
mod headers;
mod respond;

/// One mebibyte request-body ceiling for the lifecycle-only surface.
const REQUEST_BODY_BYTES_MAX: usize = 1024 * 1024;

/// Build the Durable Streams HTTP router over `db`.
///
/// The outermost layers stamp every response with the protocol §12.7 browser
/// security headers, so refusals from inner layers carry them too.
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
        .layer(
            ServiceBuilder::new()
                .layer(SetResponseHeaderLayer::overriding(
                    header::X_CONTENT_TYPE_OPTIONS,
                    HeaderValue::from_static("nosniff"),
                ))
                .layer(SetResponseHeaderLayer::overriding(
                    HeaderName::from_static("cross-origin-resource-policy"),
                    HeaderValue::from_static("cross-origin"),
                ))
                .layer(TraceLayer::new_for_http())
                .layer(DefaultBodyLimit::max(REQUEST_BODY_BYTES_MAX)),
        )
        .with_state(db)
}
