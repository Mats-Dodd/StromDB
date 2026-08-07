//! Behavioral claims for the public embeddable interface: every caller-facing
//! verb, protocol-visible stream status, typed refusals, and reopen
//! durability, all through injected `object_store` backends.

use std::sync::Arc;

use strom_db::object_store::ObjectStore;
use strom_db::object_store::memory::InMemory;
use strom_db::{
    CloseOutcome, CloseStreamOutcome, CreateOutcome, Db, ExpiryPolicy, StreamContentType,
    StreamError, StreamLifecycle, StreamPath, StreamStatus,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn every_verb_is_visible_by_status_and_survives_reopen() -> TestResult {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id: StreamPath = "events/a".parse()?;

    let db = Db::open(Arc::clone(&store)).await?;
    let partition = db.partition_id();
    assert_eq!(StreamStatus::Missing, db.stream(&id)?);
    assert_eq!(
        CreateOutcome::Created,
        db.create_stream(
            &id,
            StreamContentType::octet_stream(),
            ExpiryPolicy::None,
            StreamLifecycle::Open,
        )
        .await?
    );
    assert_eq!(
        StreamStatus::Live {
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Open,
        },
        db.stream(&id)?,
        "a success reply is released only after its effect is readable"
    );
    assert_eq!(CloseOutcome::Shutdown, db.close().await);

    let reopened = Db::open(store).await?;
    assert_eq!(partition, reopened.partition_id());
    assert!(
        matches!(reopened.stream(&id)?, StreamStatus::Live { .. }),
        "bootstrap replay reconstructs an acknowledged create"
    );
    assert_eq!(
        CloseStreamOutcome::Closed,
        reopened.close_stream(&id).await?
    );
    assert!(
        matches!(
            reopened.stream(&id)?,
            StreamStatus::Live {
                lifecycle: StreamLifecycle::Closed,
                ..
            }
        ),
        "a closed stream stays readable at its path"
    );
    reopened.delete_stream(&id).await?;
    assert_eq!(
        StreamStatus::Deleted,
        reopened.stream(&id)?,
        "delete leaves the path permanently occupied"
    );
    assert_eq!(
        Err(StreamError::Occupied),
        reopened
            .create_stream(
                &id,
                StreamContentType::octet_stream(),
                ExpiryPolicy::None,
                StreamLifecycle::Open,
            )
            .await,
        "a deleted path refuses re-creation"
    );
    assert_eq!(CloseOutcome::Shutdown, reopened.close().await);
    Ok(())
}

#[tokio::test]
async fn typed_refusal_does_not_change_stream_status() -> TestResult {
    let db = Db::open(Arc::new(InMemory::new())).await?;
    let missing: StreamPath = "events/missing".parse()?;
    assert_eq!(Err(StreamError::NotLive), db.delete_stream(&missing).await);
    assert_eq!(StreamStatus::Missing, db.stream(&missing)?);
    assert_eq!(CloseOutcome::Shutdown, db.close().await);
    Ok(())
}

#[tokio::test]
async fn duplicate_create_returns_already_exists() -> TestResult {
    let db = Db::open(Arc::new(InMemory::new())).await?;
    let id: StreamPath = "events/a".parse()?;
    assert_eq!(
        CreateOutcome::Created,
        db.create_stream(
            &id,
            StreamContentType::octet_stream(),
            ExpiryPolicy::None,
            StreamLifecycle::Open,
        )
        .await?
    );
    assert_eq!(
        CreateOutcome::AlreadyExists,
        db.create_stream(
            &id,
            StreamContentType::octet_stream(),
            ExpiryPolicy::None,
            StreamLifecycle::Open,
        )
        .await?,
        "same-configuration create is idempotent"
    );
    assert_eq!(CloseOutcome::Shutdown, db.close().await);
    Ok(())
}

#[tokio::test]
async fn config_mismatch_create_refuses_occupied() -> TestResult {
    let db = Db::open(Arc::new(InMemory::new())).await?;
    let id: StreamPath = "events/a".parse()?;
    db.create_stream(
        &id,
        StreamContentType::octet_stream(),
        ExpiryPolicy::None,
        StreamLifecycle::Open,
    )
    .await?;
    assert_eq!(
        Err(StreamError::Occupied),
        db.create_stream(
            &id,
            "text/plain".parse()?,
            ExpiryPolicy::None,
            StreamLifecycle::Open,
        )
        .await,
        "content-type mismatch is not idempotent success"
    );
    assert_eq!(CloseOutcome::Shutdown, db.close().await);
    Ok(())
}

#[tokio::test]
async fn create_closed_is_visible_and_survives_reopen() -> TestResult {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id: StreamPath = "events/closed".parse()?;

    let db = Db::open(Arc::clone(&store)).await?;
    let partition = db.partition_id();
    assert_eq!(
        CreateOutcome::Created,
        db.create_stream(
            &id,
            StreamContentType::octet_stream(),
            ExpiryPolicy::None,
            StreamLifecycle::Closed,
        )
        .await?
    );
    assert_eq!(
        StreamStatus::Live {
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Closed,
        },
        db.stream(&id)?
    );
    assert_eq!(CloseOutcome::Shutdown, db.close().await);

    let reopened = Db::open(store).await?;
    assert_eq!(partition, reopened.partition_id());
    assert_eq!(
        StreamStatus::Live {
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Closed,
        },
        reopened.stream(&id)?,
        "create-closed remains Closed after bootstrap replay"
    );
    assert_eq!(CloseOutcome::Shutdown, reopened.close().await);
    Ok(())
}

#[tokio::test]
async fn duplicate_close_returns_already_closed() -> TestResult {
    let db = Db::open(Arc::new(InMemory::new())).await?;
    let id: StreamPath = "events/a".parse()?;
    db.create_stream(
        &id,
        StreamContentType::octet_stream(),
        ExpiryPolicy::None,
        StreamLifecycle::Open,
    )
    .await?;
    assert_eq!(CloseStreamOutcome::Closed, db.close_stream(&id).await?);
    assert_eq!(
        CloseStreamOutcome::AlreadyClosed,
        db.close_stream(&id).await?,
        "close on an already-closed stream is idempotent"
    );
    assert_eq!(CloseOutcome::Shutdown, db.close().await);
    Ok(())
}
