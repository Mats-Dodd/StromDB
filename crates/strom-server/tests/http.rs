//! Behavioral claims for the Durable Streams HTTP lifecycle surface.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt as _;
use strom_server::router;
use stromdb::Db;
use stromdb::object_store::ObjectStore;
use stromdb::object_store::memory::InMemory;
use tower::ServiceExt as _;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn put_creates_then_idempotent_put_returns_ok() -> TestResult {
    let app = open_app().await?;
    let created = app
        .clone()
        .oneshot(put_request("/events/a", None, None, None))
        .await?;
    assert_eq!(StatusCode::CREATED, created.status());
    assert_security_headers(&created);
    assert_eq!(
        Some("/events/a".as_bytes()),
        created.headers().get(header::LOCATION).map(AsRef::as_ref),
    );
    assert_eq!(
        Some(b"application/octet-stream".as_slice()),
        created
            .headers()
            .get(header::CONTENT_TYPE)
            .map(AsRef::as_ref),
    );

    let again = app
        .oneshot(put_request("/events/a", None, None, None))
        .await?;
    assert_eq!(StatusCode::OK, again.status());
    assert_security_headers(&again);
    Ok(())
}

#[tokio::test]
async fn put_mismatch_returns_conflict() -> TestResult {
    let app = open_app().await?;
    let created = app
        .clone()
        .oneshot(put_request("/events/a", Some("text/plain"), None, None))
        .await?;
    assert_eq!(StatusCode::CREATED, created.status());
    let conflict = app
        .oneshot(put_request(
            "/events/a",
            Some("application/octet-stream"),
            None,
            None,
        ))
        .await?;
    assert_eq!(StatusCode::CONFLICT, conflict.status());
    assert_security_headers(&conflict);
    Ok(())
}

#[tokio::test]
async fn put_with_body_is_rejected() -> TestResult {
    let app = open_app().await?;
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/events/a")
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(Body::from("nope"))?,
        )
        .await?;
    assert_eq!(StatusCode::BAD_REQUEST, response.status());
    let body = body_text(response).await?;
    assert!(body.contains("initial content is not supported yet"));
    Ok(())
}

#[tokio::test]
async fn put_with_stream_closed_creates_closed_stream() -> TestResult {
    let app = open_app().await?;
    let created = app
        .clone()
        .oneshot(put_request("/events/closed", None, None, Some("true")))
        .await?;
    assert_eq!(StatusCode::CREATED, created.status());
    assert_eq!(
        Some(b"true".as_slice()),
        created.headers().get("stream-closed").map(AsRef::as_ref),
    );

    let head = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/events/closed")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(StatusCode::OK, head.status());
    assert_eq!(
        Some(b"true".as_slice()),
        head.headers().get("stream-closed").map(AsRef::as_ref),
    );
    Ok(())
}

#[tokio::test]
async fn post_close_only_is_idempotent() -> TestResult {
    let app = open_app().await?;
    let created = app
        .clone()
        .oneshot(put_request("/events/a", None, None, None))
        .await?;
    assert_eq!(StatusCode::CREATED, created.status());

    let first = app.clone().oneshot(close_request("/events/a")).await?;
    assert_eq!(StatusCode::NO_CONTENT, first.status());
    assert_eq!(
        Some(b"true".as_slice()),
        first.headers().get("stream-closed").map(AsRef::as_ref),
    );

    let second = app.oneshot(close_request("/events/a")).await?;
    assert_eq!(StatusCode::NO_CONTENT, second.status());
    assert_eq!(
        Some(b"true".as_slice()),
        second.headers().get("stream-closed").map(AsRef::as_ref),
    );
    Ok(())
}

#[tokio::test]
async fn post_with_body_is_not_implemented() -> TestResult {
    let app = open_app().await?;
    let created = app
        .clone()
        .oneshot(put_request("/events/a", None, None, None))
        .await?;
    assert_eq!(StatusCode::CREATED, created.status());
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/events/a")
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(Body::from("bytes"))?,
        )
        .await?;
    assert_eq!(StatusCode::NOT_IMPLEMENTED, response.status());
    let body = body_text(response).await?;
    assert!(body.contains("append is not implemented"));
    Ok(())
}

#[tokio::test]
async fn empty_post_without_stream_closed_is_bad_request() -> TestResult {
    let app = open_app().await?;
    let created = app
        .clone()
        .oneshot(put_request("/events/a", None, None, None))
        .await?;
    assert_eq!(StatusCode::CREATED, created.status());
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/events/a")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(StatusCode::BAD_REQUEST, response.status());
    Ok(())
}

#[tokio::test]
async fn head_missing_is_not_found() -> TestResult {
    let app = open_app().await?;
    let response = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/events/missing")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(StatusCode::NOT_FOUND, response.status());
    assert_security_headers(&response);
    Ok(())
}

#[tokio::test]
async fn delete_then_head_and_delete_again_are_gone() -> TestResult {
    let app = open_app().await?;
    let created = app
        .clone()
        .oneshot(put_request("/events/a", None, None, None))
        .await?;
    assert_eq!(StatusCode::CREATED, created.status());

    let deleted = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/events/a")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(StatusCode::NO_CONTENT, deleted.status());

    let head = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/events/a")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(StatusCode::GONE, head.status());

    let again = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/events/a")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(StatusCode::GONE, again.status());
    Ok(())
}

#[tokio::test]
async fn get_is_not_implemented() -> TestResult {
    let app = open_app().await?;
    let created = app
        .clone()
        .oneshot(put_request("/events/a", None, None, None))
        .await?;
    assert_eq!(StatusCode::CREATED, created.status());
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/events/a")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(StatusCode::NOT_IMPLEMENTED, response.status());
    let body = body_text(response).await?;
    assert!(body.contains("read is not implemented"));
    Ok(())
}

#[tokio::test]
async fn reserved_ds_prefix_is_not_found() -> TestResult {
    let app = open_app().await?;
    let root = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/__ds")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(StatusCode::NOT_FOUND, root.status());
    assert_security_headers(&root);

    let nested = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/__ds/subscriptions/x")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(StatusCode::NOT_FOUND, nested.status());
    let body = body_text(nested).await?;
    assert_eq!("Not found", body);
    Ok(())
}

#[tokio::test]
async fn security_headers_are_present_on_errors() -> TestResult {
    let app = open_app().await?;
    let response = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/events/missing")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(StatusCode::NOT_FOUND, response.status());
    assert_security_headers(&response);
    Ok(())
}

#[tokio::test]
async fn invalid_ttl_is_bad_request() -> TestResult {
    let app = open_app().await?;
    let response = app
        .oneshot(put_request("/events/a", None, Some("03600"), None))
        .await?;
    assert_eq!(StatusCode::BAD_REQUEST, response.status());
    assert_security_headers(&response);
    Ok(())
}

async fn open_app() -> TestResult<axum::Router> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::open(store).await?;
    Ok(router(Arc::new(db)))
}

fn put_request(
    path: &str,
    content_type: Option<&str>,
    ttl: Option<&str>,
    stream_closed: Option<&str>,
) -> Request<Body> {
    let mut builder = Request::builder().method("PUT").uri(path);
    if let Some(content_type) = content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    if let Some(ttl) = ttl {
        builder = builder.header("stream-ttl", ttl);
    }
    if let Some(stream_closed) = stream_closed {
        builder = builder.header("stream-closed", stream_closed);
    }
    builder.body(Body::empty()).expect("test request builds")
}

fn close_request(path: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("stream-closed", "true")
        .body(Body::empty())
        .expect("test request builds")
}

fn assert_security_headers<B>(response: &axum::http::Response<B>) {
    assert_eq!(
        Some(b"nosniff".as_slice()),
        response
            .headers()
            .get(header::X_CONTENT_TYPE_OPTIONS)
            .map(AsRef::as_ref),
        "protocol §12.7 requires X-Content-Type-Options: nosniff"
    );
    assert_eq!(
        Some(b"cross-origin".as_slice()),
        response
            .headers()
            .get("cross-origin-resource-policy")
            .map(AsRef::as_ref),
        "protocol §12.7 requires Cross-Origin-Resource-Policy: cross-origin"
    );
}

async fn body_text(response: axum::http::Response<Body>) -> TestResult<String> {
    let bytes = response.into_body().collect().await?.to_bytes();
    Ok(String::from_utf8(bytes.to_vec())?)
}
