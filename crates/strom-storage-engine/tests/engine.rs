//! Behavioral claims for the public engine boundary.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use strom_common::{Entropy, Seed};
use strom_domain::{
    CreateOutcome, ExpiryPolicy, StreamContentType, StreamId, StreamLifecycle, StreamStatus,
};
use strom_storage_engine::{CloseOutcome, Engine, StreamError};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn success_is_visible_before_reply_and_survives_reopen() -> TestResult {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id: StreamId = "events/a".parse()?;
    let engine = Engine::open(Arc::clone(&store), test_entropy()).await?;
    let partition = engine.partition_id();
    assert_eq!(CreateOutcome::Created, create(&engine, &id).await?,);
    assert!(matches!(engine.stream(&id)?, StreamStatus::Live { .. }));
    assert_eq!(
        CloseOutcome::Shutdown,
        engine.shutdown().await,
        "a success reply is released only after its immutable view is installed"
    );

    let reopened = Engine::open(store, test_entropy()).await?;
    assert_eq!(
        partition,
        reopened.partition_id(),
        "reopen discovers the genesis-born partition identity"
    );
    assert!(matches!(reopened.stream(&id)?, StreamStatus::Live { .. }));
    assert_eq!(
        CloseOutcome::Shutdown,
        reopened.shutdown().await,
        "bootstrap replay reconstructs an acknowledged write"
    );
    Ok(())
}

#[tokio::test]
async fn duplicate_create_is_idempotent_and_survives_reopen() -> TestResult {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id: StreamId = "events/dup".parse()?;
    let engine = Engine::open(Arc::clone(&store), test_entropy()).await?;
    assert_eq!(CreateOutcome::Created, create(&engine, &id).await?);
    assert_eq!(CreateOutcome::AlreadyExists, create(&engine, &id).await?);
    assert!(matches!(engine.stream(&id)?, StreamStatus::Live { .. }));
    assert_eq!(
        CloseOutcome::Shutdown,
        engine.shutdown().await,
        "an idempotent create does not change the published view"
    );

    let reopened = Engine::open(store, test_entropy()).await?;
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
    let engine = Engine::open(Arc::new(InMemory::new()), test_entropy()).await?;
    let missing: StreamId = "events/missing".parse()?;
    assert_eq!(
        Err(StreamError::NotLive),
        engine.delete_stream(&missing).await
    );
    assert_eq!(StreamStatus::Missing, engine.stream(&missing)?);
    assert_eq!(CloseOutcome::Shutdown, engine.shutdown().await);
    Ok(())
}

#[tokio::test]
async fn a_new_engine_revokes_the_previous_writer() -> TestResult {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let previous = Engine::open(Arc::clone(&store), test_entropy()).await?;
    let current = Engine::open(store, test_entropy()).await?;
    let id: StreamId = "events/fenced".parse()?;

    assert_eq!(
        Err(StreamError::Indeterminate),
        create(&previous, &id).await
    );
    assert_eq!(Err(StreamError::Unavailable), previous.stream(&id));
    assert_eq!(CloseOutcome::Fenced, previous.shutdown().await);
    assert_eq!(CloseOutcome::Shutdown, current.shutdown().await);
    Ok(())
}

async fn create(engine: &Engine, id: &StreamId) -> Result<CreateOutcome, StreamError> {
    engine
        .create_stream(
            id,
            StreamContentType::octet_stream(),
            ExpiryPolicy::None,
            StreamLifecycle::Open,
        )
        .await
}

fn test_entropy() -> Entropy {
    const TEST_SEED: u64 = 42;
    Entropy::from_seed(Seed::from(TEST_SEED))
}
