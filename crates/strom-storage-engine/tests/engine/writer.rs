use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStoreExt as _, PutPayload};
use strom_domain::{CreateOutcome, StreamId, StreamStatus};
use strom_object_store::test_support::{
    BackendFailure, Fault, FaultStore, Gate, Operation, Selection, Target,
};
use strom_storage_domain::{
    BatchId, OwnerToken, SealGeneration, WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER,
    WAL_SUFFIX_COORDINATES_MAX_V2, WalBody, WalObject, encode_wal,
};
use strom_storage_engine::{CloseOutcome, Engine, OpenError, StreamError};

use super::support::{TestResult, create, entropy, seal_key, wal_key};

#[tokio::test]
async fn direct_wal_create_commits_once_and_survives_reopen() -> TestResult {
    let wal_key = wal_key(2);
    let store = FaultStore::new();
    let id: StreamId = "events/direct".parse()?;

    let engine = Engine::open(store.backend(), entropy()).await?;
    assert_eq!(CreateOutcome::Created, create(&engine, &id).await?);
    assert!(matches!(engine.stream(&id)?, StreamStatus::Live { .. }));
    assert_eq!(CloseOutcome::Shutdown, engine.shutdown().await);
    store.assert_called_once(Operation::Create, &wal_key)?;

    let reopened = Engine::open(store.backend(), entropy()).await?;
    assert!(matches!(reopened.stream(&id)?, StreamStatus::Live { .. }));
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);

    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn ambiguous_wal_create_with_matching_bytes_commits_without_resend() -> TestResult {
    let wal_key = wal_key(2);
    let store = FaultStore::new().inject(Fault::CreateThenLoseResponse {
        target: Target::Key(wal_key.clone()),
    })?;
    let id: StreamId = "events/ambiguous".parse()?;

    let engine = Engine::open(store.backend(), entropy()).await?;
    assert_eq!(CreateOutcome::Created, create(&engine, &id).await?);
    assert!(matches!(engine.stream(&id)?, StreamStatus::Live { .. }));
    assert_eq!(CloseOutcome::Shutdown, engine.shutdown().await);
    store.assert_called_once(Operation::Create, &wal_key)?;
    store.assert_called_once(Operation::Read, &wal_key)?;

    let reopened = Engine::open(store.backend(), entropy()).await?;
    assert!(matches!(reopened.stream(&id)?, StreamStatus::Live { .. }));
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);

    // The durable batch-two RUN moves the reopened bootstrap's FENCE to batch three.
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn ambiguous_wal_create_with_foreign_bytes_fences_without_resend() -> TestResult {
    let wal_key = wal_key(2);
    let create_gate = Gate::new();
    let store = FaultStore::new()
        .inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(wal_key.clone())),
            failure: BackendFailure::Transport,
        })?
        .gate(
            Selection::create(Target::Key(wal_key.clone())),
            create_gate.clone(),
        )?;
    let backend = store.backend();
    let id: StreamId = "events/foreign".parse()?;
    let engine = Engine::open(Arc::clone(&backend), entropy()).await?;
    let partition = engine.partition_id();

    let outcome = {
        let command = create(&engine, &id);
        tokio::pin!(command);
        tokio::select! {
            () = create_gate.wait_until_blocked() => {}
            outcome = &mut command => panic!("WAL create passed its held gate: {outcome:?}"),
        }

        let foreign = WalObject::new(
            partition,
            BatchId::try_from(2)?,
            OwnerToken::from(SealGeneration::genesis()),
            WalBody::Fence,
        );
        backend
            .put(
                &Path::from(wal_key.as_str()),
                PutPayload::from(encode_wal(&foreign)?),
            )
            .await?;
        create_gate.release();
        command.await
    };

    assert_eq!(Err(StreamError::Indeterminate), outcome);
    assert_eq!(Err(StreamError::Unavailable), engine.stream(&id));
    assert_eq!(CloseOutcome::Fenced, engine.shutdown().await);
    store.assert_called_once(Operation::Create, &wal_key)?;
    store.assert_called_once(Operation::Read, &wal_key)?;
    assert!(matches!(
        Engine::open(backend, entropy()).await,
        Err(OpenError::Contradiction { .. })
    ));
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn ambiguous_wal_create_absent_on_reconciliation_poisoned_without_resend() -> TestResult {
    let wal_key = wal_key(2);
    let store = FaultStore::new().inject(Fault::FailBefore {
        selection: Selection::create(Target::Key(wal_key.clone())),
        failure: BackendFailure::Transport,
    })?;
    let id: StreamId = "events/absent".parse()?;

    let engine = Engine::open(store.backend(), entropy()).await?;
    assert_eq!(Err(StreamError::Indeterminate), create(&engine, &id).await);
    assert!(matches!(
        engine.shutdown().await,
        CloseOutcome::Poisoned { .. }
    ));
    // Reopen fills the unused batch-two coordinate, so count the writer attempt first.
    store.assert_called_once(Operation::Create, &wal_key)?;
    store.assert_called_once(Operation::Read, &wal_key)?;

    let reopened = Engine::open(store.backend(), entropy()).await?;
    assert_eq!(StreamStatus::Missing, reopened.stream(&id)?);
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn failed_wal_reconciliation_poisoned_without_resend() -> TestResult {
    let wal_key = wal_key(2);
    let store = FaultStore::new()
        .inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(wal_key.clone())),
            failure: BackendFailure::Transport,
        })?
        .inject(Fault::FailBefore {
            selection: Selection::read(Target::Key(wal_key.clone())),
            failure: BackendFailure::Transport,
        })?;
    let id: StreamId = "events/reconciliation-failed".parse()?;

    let engine = Engine::open(store.backend(), entropy()).await?;
    assert_eq!(Err(StreamError::Indeterminate), create(&engine, &id).await);
    assert!(matches!(
        engine.shutdown().await,
        CloseOutcome::Poisoned { .. }
    ));
    store.assert_called_once(Operation::Create, &wal_key)?;
    store.assert_called_once(Operation::Read, &wal_key)?;

    let reopened = Engine::open(store.backend(), entropy()).await?;
    assert_eq!(StreamStatus::Missing, reopened.stream(&id)?);
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn wal_commit_becomes_visible_with_the_success_reply() -> TestResult {
    let wal_key = wal_key(2);
    let gate = Gate::new();
    let store = FaultStore::new().gate(Selection::create(Target::Key(wal_key)), gate.clone())?;
    let id: StreamId = "events/visibility".parse()?;
    let engine = Engine::open(store.backend(), entropy()).await?;

    let outcome = {
        let command = create(&engine, &id);
        tokio::pin!(command);
        tokio::select! {
            () = gate.wait_until_blocked() => {}
            outcome = &mut command => panic!("WAL create passed its held gate: {outcome:?}"),
        }
        assert_eq!(StreamStatus::Missing, engine.stream(&id)?);
        gate.release();
        command.await
    };

    assert_eq!(Ok(CreateOutcome::Created), outcome);
    assert!(matches!(engine.stream(&id)?, StreamStatus::Live { .. }));
    assert_eq!(CloseOutcome::Shutdown, engine.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn successor_writer_fences_the_previous_writer_and_preserves_its_own_work() -> TestResult {
    let store = FaultStore::new();
    let previous = Engine::open(store.backend(), entropy()).await?;
    let current = Engine::open(store.backend(), entropy()).await?;
    let rejected: StreamId = "events/previous".parse()?;
    let accepted: StreamId = "events/current".parse()?;

    assert_eq!(
        Err(StreamError::Indeterminate),
        create(&previous, &rejected).await
    );
    assert_eq!(Err(StreamError::Unavailable), previous.stream(&rejected));
    assert_eq!(CloseOutcome::Fenced, previous.shutdown().await);

    assert_eq!(CreateOutcome::Created, create(&current, &accepted).await?);
    assert!(matches!(
        current.stream(&accepted)?,
        StreamStatus::Live { .. }
    ));
    assert_eq!(CloseOutcome::Shutdown, current.shutdown().await);

    let reopened = Engine::open(store.backend(), entropy()).await?;
    assert_eq!(StreamStatus::Missing, reopened.stream(&rejected)?);
    assert!(matches!(
        reopened.stream(&accepted)?,
        StreamStatus::Live { .. }
    ));
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn shutdown_waits_for_an_active_wal_flight() -> TestResult {
    let gate = Gate::new();
    let store = FaultStore::new().gate(Selection::create(Target::Key(wal_key(2))), gate.clone())?;
    let id: StreamId = "events/shutdown".parse()?;
    let engine = Engine::open(store.backend(), entropy()).await?;

    {
        let command = create(&engine, &id);
        tokio::pin!(command);
        tokio::select! {
            () = gate.wait_until_blocked() => {}
            outcome = &mut command => panic!("WAL create passed its held gate: {outcome:?}"),
        }
    }

    let shutdown = tokio::spawn(engine.shutdown());
    gate.release();
    assert_eq!(CloseOutcome::Shutdown, shutdown.await?);

    let reopened = Engine::open(store.backend(), entropy()).await?;
    assert!(matches!(reopened.stream(&id)?, StreamStatus::Live { .. }));
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn held_checkpoint_cannot_extend_the_bounded_wal_suffix() -> TestResult {
    let seal_gate = Gate::new();
    let seal_key = seal_key(3);
    let store = FaultStore::new().gate(
        Selection::create(Target::Key(seal_key.clone())),
        seal_gate.clone(),
    )?;
    let engine = Engine::open(store.backend(), entropy()).await?;

    let accepted_count = WAL_SUFFIX_COORDINATES_MAX_V2
        .checked_sub(2)
        .expect("the suffix reserves a genesis FENCE and one successor FENCE");
    let checkpoint_trigger_count = WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER;
    for ordinal in 0..checkpoint_trigger_count {
        let id: StreamId = format!("events/bounded-{ordinal}").parse()?;
        assert_eq!(CreateOutcome::Created, create(&engine, &id).await?);
    }
    seal_gate.wait_until_blocked().await;

    for ordinal in checkpoint_trigger_count..accepted_count {
        let id: StreamId = format!("events/bounded-{ordinal}").parse()?;
        assert_eq!(CreateOutcome::Created, create(&engine, &id).await?);
    }
    let refused: StreamId = "events/bounded-refused".parse()?;
    assert_eq!(
        Err(StreamError::Overloaded),
        create(&engine, &refused).await
    );

    let first: StreamId = "events/bounded-0".parse()?;
    let last: StreamId = format!(
        "events/bounded-{}",
        accepted_count
            .checked_sub(1)
            .expect("the suffix accepts at least one command")
    )
    .parse()?;
    assert_eq!(CreateOutcome::AlreadyExists, create(&engine, &first).await?);
    assert_eq!(StreamStatus::Missing, engine.stream(&refused)?);

    seal_gate.release();
    assert_eq!(CloseOutcome::Shutdown, engine.shutdown().await);

    let reopened = Engine::open(store.backend(), entropy()).await?;
    assert!(matches!(
        reopened.stream(&first)?,
        StreamStatus::Live { .. }
    ));
    assert!(matches!(reopened.stream(&last)?, StreamStatus::Live { .. }));
    assert_eq!(StreamStatus::Missing, reopened.stream(&refused)?);
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    store.assert_called_once(Operation::Create, &seal_key)?;
    store.verify()?;
    Ok(())
}
