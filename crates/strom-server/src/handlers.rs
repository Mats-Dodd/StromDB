//! Durable Streams verb handlers over an embedded [`stromdb::Db`].

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use stromdb::{
    CloseStreamOutcome, CreateOutcome, Db, ExpiryPolicy, StreamContentType, StreamError, StreamId,
    StreamLifecycle, StreamStatus,
};

use crate::error::ApiError;
use crate::headers::{STREAM_CLOSED, STREAM_EXPIRES_AT, STREAM_TTL};
use crate::parse;

/// Create or ensure a stream (`PUT`, §5.1).
///
/// Known deviation from §5.1: responses omit `Stream-Next-Offset` until the
/// engine owns offsets.
pub(crate) async fn put(
    State(db): State<Arc<Db>>,
    Path(path): Path<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    if !body.is_empty() {
        return Err(ApiError::BadRequest(
            "initial content is not supported yet".to_owned(),
        ));
    }
    let id = parse::stream_id(&path)?;
    let content_type = parse::content_type(&headers)?;
    let expiry = parse::expiry(&headers)?;
    let lifecycle = if parse::stream_closed(&headers) {
        StreamLifecycle::Closed
    } else {
        StreamLifecycle::Open
    };
    let outcome = db
        .create_stream(&id, content_type.clone(), expiry, lifecycle)
        .await?;
    let status = match outcome {
        CreateOutcome::Created => StatusCode::CREATED,
        CreateOutcome::AlreadyExists => StatusCode::OK,
    };
    Ok(create_response(
        status,
        uri.path(),
        &content_type,
        lifecycle.is_closed(),
    ))
}

/// Close a stream, or refuse append (`POST`, §5.2 / §5.3).
pub(crate) async fn post(
    State(db): State<Arc<Db>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let id = parse::stream_id(&path)?;
    match (parse::stream_closed(&headers), body.is_empty()) {
        (true, true) => close_stream(&db, &id).await,
        (_, false) => Err(ApiError::NotImplemented(
            "append is not implemented".to_owned(),
        )),
        (false, true) => Err(ApiError::BadRequest(
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
    Path(path): Path<String>,
) -> Result<Response, ApiError> {
    let id = parse::stream_id(&path)?;
    match db.stream(&id)? {
        StreamStatus::Missing => Err(ApiError::NotFound),
        StreamStatus::Deleted => Err(ApiError::Gone),
        StreamStatus::Live {
            content_type,
            expiry,
            lifecycle,
        } => Ok(metadata_response(&content_type, expiry, lifecycle)),
    }
}

/// Soft-delete a stream (`DELETE`, §5.4).
pub(crate) async fn delete(
    State(db): State<Arc<Db>>,
    Path(path): Path<String>,
) -> Result<StatusCode, ApiError> {
    let id = parse::stream_id(&path)?;
    match db.delete_stream(&id).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(StreamError::NotLive) => Err(refuse_not_live(&db, &id)),
        Err(error) => Err(ApiError::from(error)),
    }
}

/// Read is not implemented yet (`GET`, §5.6+).
pub(crate) async fn get(Path(path): Path<String>) -> Result<(), ApiError> {
    let _id = parse::stream_id(&path)?;
    Err(ApiError::NotImplemented(
        "read is not implemented".to_owned(),
    ))
}

/// Reserved `__ds` control prefix (§6): not implemented; always 404.
pub(crate) async fn reserved_not_found() -> impl IntoResponse {
    ApiError::NotFound
}

async fn close_stream(db: &Db, id: &StreamId) -> Result<Response, ApiError> {
    match db.close_stream(id).await {
        Ok(CloseStreamOutcome::Closed | CloseStreamOutcome::AlreadyClosed) => {
            let mut headers = HeaderMap::new();
            headers.insert(STREAM_CLOSED, HeaderValue::from_static("true"));
            Ok((StatusCode::NO_CONTENT, headers).into_response())
        }
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

fn create_response(
    status: StatusCode,
    location: &str,
    content_type: &StreamContentType,
    closed: bool,
) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::LOCATION,
        HeaderValue::from_str(location).expect("request path is a valid Location header value"),
    );
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type.as_str())
            .expect("canonical stream content types are valid header values"),
    );
    if closed {
        headers.insert(STREAM_CLOSED, HeaderValue::from_static("true"));
    }
    (status, headers).into_response()
}

fn metadata_response(
    content_type: &StreamContentType,
    expiry: ExpiryPolicy,
    lifecycle: StreamLifecycle,
) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type.as_str())
            .expect("canonical stream content types are valid header values"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    match expiry {
        ExpiryPolicy::None => {}
        ExpiryPolicy::SlidingTtl(ttl) => {
            headers.insert(
                STREAM_TTL,
                HeaderValue::from_str(&ttl.to_string())
                    .expect("decimal TTL seconds are valid header values"),
            );
        }
        ExpiryPolicy::AbsoluteExpiry(expires_at) => {
            headers.insert(
                STREAM_EXPIRES_AT,
                HeaderValue::from_str(&expires_at.to_string())
                    .expect("RFC 3339 expires-at values are valid header values"),
            );
        }
    }
    if lifecycle.is_closed() {
        headers.insert(STREAM_CLOSED, HeaderValue::from_static("true"));
    }
    (StatusCode::OK, headers).into_response()
}
