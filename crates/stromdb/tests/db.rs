//! Behavioral claims for the public embeddable interface: every caller-facing
//! verb, protocol-visible stream status, typed refusals, and reopen
//! durability, all through injected `object_store` backends.

use std::sync::Arc;

use stromdb::object_store::ObjectStore;
use stromdb::object_store::memory::InMemory;
use stromdb::{
    CloseOutcome, Db, ExpiryPolicy, PartitionId, StreamContentType, StreamError, StreamId,
    StreamLifecycle, StreamStatus,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn every_verb_is_visible_by_status_and_survives_reopen() -> TestResult {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let partition = partition();
    let id: StreamId = "events/a".parse()?;

    let db = Db::open(Arc::clone(&store), partition).await?;
    assert_eq!(StreamStatus::Missing, db.stream(&id)?);
    db.create_stream(&id, StreamContentType::octet_stream(), ExpiryPolicy::None)
        .await?;
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

    let reopened = Db::open(store, partition).await?;
    assert!(
        matches!(reopened.stream(&id)?, StreamStatus::Live { .. }),
        "bootstrap replay reconstructs an acknowledged create"
    );
    reopened.close_stream(&id).await?;
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
            .create_stream(&id, StreamContentType::octet_stream(), ExpiryPolicy::None)
            .await,
        "a deleted path refuses re-creation"
    );
    assert_eq!(CloseOutcome::Shutdown, reopened.close().await);
    Ok(())
}

#[tokio::test]
async fn typed_refusal_does_not_change_stream_status() -> TestResult {
    let db = Db::open(Arc::new(InMemory::new()), partition()).await?;
    let missing: StreamId = "events/missing".parse()?;
    assert_eq!(Err(StreamError::NotLive), db.delete_stream(&missing).await);
    assert_eq!(StreamStatus::Missing, db.stream(&missing)?);
    assert_eq!(CloseOutcome::Shutdown, db.close().await);
    Ok(())
}

fn partition() -> PartitionId {
    "00112233-4455-6677-8899-aabbccddeeff"
        .parse()
        .expect("test partition is canonical")
}
