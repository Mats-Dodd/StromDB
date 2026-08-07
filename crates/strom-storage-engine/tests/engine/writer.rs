use strom_domain::{CreateOutcome, StreamId, StreamStatus};
use strom_object_store::test_support::{
    BackendFailure, Fault, FaultStore, Operation, Selection, Target,
};
use strom_storage_engine::{CloseOutcome, Engine, StreamError};

use super::support::{TestResult, create, entropy, wal_key};

#[tokio::test]
async fn ambiguous_wal_create_with_matching_bytes_commits() -> TestResult {
    let wal_key = wal_key(2);
    let store = FaultStore::new().inject(Fault::CreateThenLoseResponse {
        target: Target::Key(wal_key.clone()),
    })?;
    let id: StreamId = "events/ambiguous".parse()?;

    let engine = Engine::open(store.backend(), entropy()).await?;
    assert_eq!(CreateOutcome::Created, create(&engine, &id).await?);
    assert!(matches!(engine.stream(&id)?, StreamStatus::Live { .. }));
    assert_eq!(CloseOutcome::Shutdown, engine.shutdown().await);

    let reopened = Engine::open(store.backend(), entropy()).await?;
    assert!(matches!(reopened.stream(&id)?, StreamStatus::Live { .. }));
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);

    // The durable batch-two RUN moves the reopened bootstrap's FENCE to batch three.
    store.assert_called_once(Operation::Create, &wal_key)?;
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn wal_create_failure_before_effect_never_acknowledges() -> TestResult {
    let wal_key = wal_key(2);
    let store = FaultStore::new().inject(Fault::FailBefore {
        selection: Selection::create(Target::Key(wal_key.clone())),
        failure: BackendFailure::Transport,
    })?;
    let id: StreamId = "events/fail-before".parse()?;

    let engine = Engine::open(store.backend(), entropy()).await?;
    assert_eq!(Err(StreamError::Indeterminate), create(&engine, &id).await);
    assert!(matches!(
        engine.shutdown().await,
        CloseOutcome::Poisoned { .. }
    ));
    // Assert before reopen, whose FENCE fills the batch-two coordinate left empty by the RUN.
    store.assert_called_once(Operation::Create, &wal_key)?;
    store.verify()?;

    let reopened = Engine::open(store.backend(), entropy()).await?;
    assert_eq!(StreamStatus::Missing, reopened.stream(&id)?);
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    Ok(())
}
