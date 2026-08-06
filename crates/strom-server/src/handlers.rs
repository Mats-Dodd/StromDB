//! Durable Streams verb handlers over an embedded [`strom_db::Db`].

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, Uri, header};
use axum::response::IntoResponse;
use strom_db::{
    CloseStreamOutcome, CreateOutcome, Db, StreamError, StreamId, StreamLifecycle, StreamStatus,
};

use crate::error::ApiError;
use crate::extract::{Expiry, Lifecycle, RequestContentType, StreamPath};
use crate::respond::{ClosedHeader, ContentTypeHeader, ExpiryHeaders, Location};

/// Create or ensure a stream (`PUT`, §5.1).
///
/// Known deviation from §5.1: responses omit `Stream-Next-Offset` until the
/// engine owns offsets.
pub(crate) async fn put(
    State(db): State<Arc<Db>>,
    StreamPath(id): StreamPath,
    RequestContentType(content_type): RequestContentType,
    Expiry(expiry): Expiry,
    Lifecycle(lifecycle): Lifecycle,
    uri: Uri,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    if !body.is_empty() {
        return Err(ApiError::BadRequest(
            "initial content is not supported yet".to_owned(),
        ));
    }
    let outcome = db
        .create_stream(&id, content_type.clone(), expiry, lifecycle)
        .await?;
    let status = match outcome {
        CreateOutcome::Created => StatusCode::CREATED,
        CreateOutcome::AlreadyExists => StatusCode::OK,
    };
    Ok((
        status,
        Location(uri),
        ContentTypeHeader(content_type),
        ClosedHeader(lifecycle),
    ))
}

/// Close a stream, or refuse append (`POST`, §5.2 / §5.3).
pub(crate) async fn post(
    State(db): State<Arc<Db>>,
    StreamPath(id): StreamPath,
    Lifecycle(lifecycle): Lifecycle,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    match (lifecycle, body.is_empty()) {
        (StreamLifecycle::Closed, true) => close_stream(&db, &id).await,
        (StreamLifecycle::Open | StreamLifecycle::Closed, false) => Err(ApiError::NotImplemented(
            "append is not implemented".to_owned(),
        )),
        (StreamLifecycle::Open, true) => Err(ApiError::BadRequest(
            "empty body requires Stream-Closed: true".to_owned(),
        )),
    }
}

/// Stream metadata without a body (`HEAD`, §5.5).
///
/// Known deviation from §5.5: responses omit `Stream-Next-Offset` until the
/// engine owns offsets.
pub(crate) async fn head(
    State(db): State<Arc<Db>>,
    StreamPath(id): StreamPath,
) -> Result<impl IntoResponse, ApiError> {
    match db.stream(&id)? {
        StreamStatus::Missing => Err(ApiError::NotFound),
        StreamStatus::Deleted => Err(ApiError::Gone),
        StreamStatus::Live {
            content_type,
            expiry,
            lifecycle,
        } => Ok((
            StatusCode::OK,
            [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
            ContentTypeHeader(content_type),
            ExpiryHeaders(expiry),
            ClosedHeader(lifecycle),
        )),
    }
}

/// Soft-delete a stream (`DELETE`, §5.4).
pub(crate) async fn delete(
    State(db): State<Arc<Db>>,
    StreamPath(id): StreamPath,
) -> Result<StatusCode, ApiError> {
    match db.delete_stream(&id).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(StreamError::NotLive) => Err(refuse_not_live(&db, &id)),
        Err(error) => Err(ApiError::from(error)),
    }
}

/// Read is not implemented yet (`GET`, §5.6+).
///
/// [`StreamPath`] must stay in the signature: a malformed stream id is a
/// protocol 400 and takes precedence over this 501.
pub(crate) async fn get(StreamPath(_id): StreamPath) -> Result<(), ApiError> {
    Err(ApiError::NotImplemented(
        "read is not implemented".to_owned(),
    ))
}

/// Reserved `__ds` control prefix (§6): not implemented; always 404.
pub(crate) async fn reserved_not_found() -> impl IntoResponse {
    ApiError::NotFound
}

async fn close_stream(db: &Db, id: &StreamId) -> Result<(StatusCode, ClosedHeader), ApiError> {
    match db.close_stream(id).await {
        Ok(CloseStreamOutcome::Closed | CloseStreamOutcome::AlreadyClosed) => Ok((
            StatusCode::NO_CONTENT,
            ClosedHeader(StreamLifecycle::Closed),
        )),
        Err(StreamError::NotLive) => Err(refuse_not_live(db, id)),
        Err(error) => Err(ApiError::from(error)),
    }
}

/// Split [`StreamError::NotLive`] into protocol 404 vs 410 via one status read.
fn refuse_not_live(db: &Db, id: &StreamId) -> ApiError {
    match db.stream(id) {
        Ok(StreamStatus::Missing) => ApiError::NotFound,
        Ok(StreamStatus::Deleted) => ApiError::Gone,
        Ok(StreamStatus::Live { .. }) => ApiError::Indeterminate,
        Err(error) => ApiError::from(error),
    }
}
