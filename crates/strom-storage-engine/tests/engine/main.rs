//! Behavioral claims for the public engine boundary.

mod bootstrap;
mod checkpoint;
mod support;
mod writer;

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use strom_domain::{CreateOutcome, StreamId, StreamStatus};
use strom_storage_engine::{CloseOutcome, Engine, StreamError};

use support::{TestResult, create, entropy};

#[tokio::test]
async fn duplicate_create_is_idempotent_and_survives_reopen() -> TestResult {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id: StreamId = "events/dup".parse()?;
    let engine = Engine::open(Arc::clone(&store), entropy()).await?;
    assert_eq!(CreateOutcome::Created, create(&engine, &id).await?);
    assert_eq!(CreateOutcome::AlreadyExists, create(&engine, &id).await?);
    assert!(matches!(engine.stream(&id)?, StreamStatus::Live { .. }));
    assert_eq!(
        CloseOutcome::Shutdown,
        engine.shutdown().await,
        "an idempotent create does not change the published view"
    );

    let reopened = Engine::open(store, entropy()).await?;
    assert!(matches!(reopened.stream(&id)?, StreamStatus::Live { .. }));
    assert_eq!(
        CloseOutcome::Shutdown,
        reopened.shutdown().await,
        "an idempotent reply does not change the recovered state"
    );
    Ok(())
}

#[tokio::test]
async fn typed_refusal_does_not_change_the_published_view() -> TestResult {
    let engine = Engine::open(Arc::new(InMemory::new()), entropy()).await?;
    let missing: StreamId = "events/missing".parse()?;
    assert_eq!(
        Err(StreamError::NotLive),
        engine.delete_stream(&missing).await
    );
    assert_eq!(StreamStatus::Missing, engine.stream(&missing)?);
    assert_eq!(CloseOutcome::Shutdown, engine.shutdown().await);
    Ok(())
}
