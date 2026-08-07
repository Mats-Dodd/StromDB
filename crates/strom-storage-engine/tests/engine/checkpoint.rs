use std::num::NonZeroU64;
use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};
use strom_domain::{CreateOutcome, StreamPath, StreamStatus};
use strom_object_store::ObjectKey;
use strom_object_store::test_support::{
    BackendFailure, Fault, FaultStore, Gate, Operation, Selection, Target,
};
use strom_storage_domain::{
    BatchId, OwnerToken, Seal, SortedRun, TableKey, TableRef, TreeVersion,
    WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER, WalReplayPoint, encode_seal,
};
use strom_storage_engine::{CloseOutcome, Engine, OpenError};

use super::support::{
    CheckpointKeys, TestResult, assert_object_absent, assert_object_present,
    checkpoint_table_key_at_attempt, create, drive_checkpoint_span, entropy,
    observe_checkpoint_keys, wal_key,
};

const CHECKPOINT_CUT: u64 = WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER;

#[tokio::test]
async fn children_are_durable_before_the_seal_is_published() -> TestResult {
    let keys = observe_checkpoint_keys().await?;
    let seal = keys.seal;
    let directory = keys.directory;
    let ledger = keys.ledger;
    let gate = Gate::new();
    let store =
        FaultStore::new().gate(Selection::create(Target::Key(seal.clone())), gate.clone())?;
    let backend = store.backend();
    let engine = Engine::open(Arc::clone(&backend), entropy()).await?;
    let streams = drive_checkpoint_span(&engine).await?;

    gate.wait_until_blocked().await;
    assert_object_present(&backend, &directory).await?;
    assert_object_present(&backend, &ledger).await?;
    assert_object_absent(&backend, &seal).await?;

    gate.release();
    assert_eq!(
        CloseOutcome::Shutdown,
        engine.shutdown().await,
        "shutdown completes after the held Seal publication is released"
    );
    assert_reopens_with_streams(backend, &streams).await?;
    store.assert_called_once(Operation::Create, &directory)?;
    store.assert_called_once(Operation::Create, &ledger)?;
    store.assert_called_once(Operation::Create, &seal)?;
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn applied_child_with_a_lost_reply_reconciles_and_publishes() -> TestResult {
    let keys = observe_checkpoint_keys().await?;
    let directory = keys.directory;
    let seal = keys.seal;
    let gate = Gate::new();
    let store = FaultStore::new()
        .inject(Fault::CreateThenLoseResponse {
            target: Target::Key(directory.clone()),
        })?
        .gate(Selection::create(Target::Key(seal.clone())), gate.clone())?;
    let backend = store.backend();
    let engine = Engine::open(Arc::clone(&backend), entropy()).await?;
    let streams = drive_checkpoint_span(&engine).await?;

    gate.wait_until_blocked().await;
    store.assert_called_once(Operation::Create, &directory)?;
    store.assert_called_once(Operation::Read, &directory)?;
    gate.release();
    assert_eq!(
        CloseOutcome::Shutdown,
        engine.shutdown().await,
        "shutdown completes after the held Seal publication is released"
    );
    assert_object_present(&backend, &directory).await?;
    assert_object_present(&backend, &seal).await?;
    assert_reopens_with_streams(backend, &streams).await?;
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn abandoned_checkpoint_clears_matching_state_and_shell_tickets_before_retry() -> TestResult {
    let keys = observe_checkpoint_keys().await?;
    let directory = keys.directory;
    let retry_directory = checkpoint_table_key_at_attempt(&directory, 1);
    let seal = keys.seal;
    let reconciliation = Gate::new();
    let retry = Gate::new();
    let store = FaultStore::new()
        .inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(directory.clone())),
            failure: BackendFailure::Transport,
        })?
        .gate(
            Selection::read(Target::Key(directory.clone())),
            reconciliation.clone(),
        )?
        .gate(
            Selection::create(Target::Key(retry_directory)),
            retry.clone(),
        )?;
    let backend = store.backend();
    let engine = Engine::open(Arc::clone(&backend), entropy()).await?;
    let streams = drive_checkpoint_span(&engine).await?;

    reconciliation.wait_until_blocked().await;
    reconciliation.release();
    retry.wait_until_blocked().await;
    let shutdown = tokio::spawn(engine.shutdown());
    assert_eq!(CloseOutcome::Shutdown, shutdown.await?);
    retry.release();
    assert_object_absent(&backend, &directory).await?;
    assert_object_absent(&backend, &seal).await?;
    assert_reopens_with_streams(backend, &streams).await?;
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn failed_child_reconciliation_abandons_and_reopens_from_wal() -> TestResult {
    let keys = observe_checkpoint_keys().await?;
    let directory = keys.directory;
    let retry_directory = checkpoint_table_key_at_attempt(&directory, 1);
    let seal = keys.seal;
    let reconciliation = Gate::new();
    let retry = Gate::new();
    let store = FaultStore::new()
        .inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(directory.clone())),
            failure: BackendFailure::Transport,
        })?
        .inject(Fault::FailBefore {
            selection: Selection::read(Target::Key(directory.clone())),
            failure: BackendFailure::Transport,
        })?
        .gate(
            Selection::read(Target::Key(directory.clone())),
            reconciliation.clone(),
        )?
        .gate(
            Selection::create(Target::Key(retry_directory)),
            retry.clone(),
        )?;
    let backend = store.backend();
    let engine = Engine::open(Arc::clone(&backend), entropy()).await?;
    let streams = drive_checkpoint_span(&engine).await?;

    reconciliation.wait_until_blocked().await;
    reconciliation.release();
    retry.wait_until_blocked().await;
    let shutdown = tokio::spawn(engine.shutdown());
    assert_eq!(CloseOutcome::Shutdown, shutdown.await?);
    retry.release();
    assert_object_absent(&backend, &seal).await?;
    assert_reopens_with_streams(backend, &streams).await?;
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn foreign_child_racing_shutdown_never_publishes() -> TestResult {
    let keys = observe_checkpoint_keys().await?;
    let directory = keys.directory;
    let seal = keys.seal;
    let gate = Gate::new();
    let store = FaultStore::new()
        .inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(directory.clone())),
            failure: BackendFailure::Transport,
        })?
        .gate(
            Selection::create(Target::Key(directory.clone())),
            gate.clone(),
        )?;
    let backend = store.backend();
    let engine = Engine::open(Arc::clone(&backend), entropy()).await?;
    let streams = drive_checkpoint_span(&engine).await?;

    gate.wait_until_blocked().await;
    put_foreign(&backend, &directory).await?;
    gate.release();
    assert!(matches!(
        engine.shutdown().await,
        CloseOutcome::Shutdown | CloseOutcome::Contradiction { .. }
    ));
    assert_object_absent(&backend, &seal).await?;
    assert_reopens_with_streams(backend, &streams).await?;
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn shutdown_cancels_before_publication_and_leaves_only_ignored_child_garbage() -> TestResult {
    let keys = observe_checkpoint_keys().await?;
    let directory = keys.directory;
    let ledger = keys.ledger;
    let seal = keys.seal;
    let gate = Gate::new();
    let store =
        FaultStore::new().gate(Selection::create(Target::Key(ledger.clone())), gate.clone())?;
    let backend = store.backend();
    let engine = Engine::open(Arc::clone(&backend), entropy()).await?;
    let streams = drive_checkpoint_span(&engine).await?;

    gate.wait_until_blocked().await;
    assert_object_present(&backend, &directory).await?;
    let shutdown = tokio::spawn(engine.shutdown());
    assert_eq!(CloseOutcome::Shutdown, shutdown.await?);
    gate.release();

    assert_object_present(&backend, &directory).await?;
    assert_object_absent(&backend, &ledger).await?;
    assert_object_absent(&backend, &seal).await?;
    assert_reopens_with_streams(backend, &streams).await?;
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn seal_failure_before_apply_poisoned_but_reopens_from_wal() -> TestResult {
    let seal = observe_checkpoint_keys().await?.seal;
    let gate = Gate::new();
    let store = FaultStore::new()
        .inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(seal.clone())),
            failure: BackendFailure::PermissionDenied,
        })?
        .gate(Selection::create(Target::Key(seal.clone())), gate.clone())?;
    let backend = store.backend();
    let engine = Engine::open(Arc::clone(&backend), entropy()).await?;
    let streams = drive_checkpoint_span(&engine).await?;

    gate.wait_until_blocked().await;
    gate.release();
    assert!(matches!(
        engine.shutdown().await,
        CloseOutcome::Poisoned { .. }
    ));
    assert_object_absent(&backend, &seal).await?;
    assert_reopens_with_streams(backend, &streams).await?;
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn applied_seal_with_a_lost_reply_poisoned_but_reopens_from_the_checkpoint() -> TestResult {
    let seal = observe_checkpoint_keys().await?.seal;
    let gate = Gate::new();
    let store = FaultStore::new()
        .inject(Fault::CreateThenLoseResponse {
            target: Target::Key(seal.clone()),
        })?
        .gate(Selection::create(Target::Key(seal.clone())), gate.clone())?;
    let backend = store.backend();
    let engine = Engine::open(Arc::clone(&backend), entropy()).await?;
    let streams = drive_checkpoint_span(&engine).await?;

    gate.wait_until_blocked().await;
    gate.release();
    assert!(matches!(
        engine.shutdown().await,
        CloseOutcome::Poisoned { .. }
    ));
    assert_object_present(&backend, &seal).await?;
    store.assert_called_once(Operation::Create, &seal)?;
    assert_reopens_with_streams(backend, &streams).await?;
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn matching_seal_occupant_fences_but_is_authoritative_on_reopen() -> TestResult {
    let keys = observe_checkpoint_keys().await?;
    let seal = keys.seal.clone();
    let gate = Gate::new();
    let store =
        FaultStore::new().gate(Selection::create(Target::Key(seal.clone())), gate.clone())?;
    let backend = store.backend();
    let engine = Engine::open(Arc::clone(&backend), entropy()).await?;
    let partition = engine.partition_id();
    let streams = drive_checkpoint_span(&engine).await?;

    gate.wait_until_blocked().await;
    let candidate = checkpoint_seal(&backend, partition, &keys).await?;
    backend
        .put(
            &Path::from(seal.as_str()),
            PutPayload::from(encode_seal(&candidate)?),
        )
        .await?;
    gate.release();

    assert_eq!(CloseOutcome::Fenced, engine.shutdown().await);
    store.assert_called_once(Operation::Create, &seal)?;
    store.assert_called_once(Operation::Read, &seal)?;
    assert_reopens_with_streams(backend, &streams).await?;
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn foreign_seal_occupant_fences_and_is_a_contradiction_on_reopen() -> TestResult {
    let seal = observe_checkpoint_keys().await?.seal;
    let gate = Gate::new();
    let store =
        FaultStore::new().gate(Selection::create(Target::Key(seal.clone())), gate.clone())?;
    let backend = store.backend();
    let engine = Engine::open(Arc::clone(&backend), entropy()).await?;
    let _streams = drive_checkpoint_span(&engine).await?;

    gate.wait_until_blocked().await;
    put_foreign(&backend, &seal).await?;
    gate.release();

    assert_eq!(CloseOutcome::Fenced, engine.shutdown().await);
    assert!(matches!(
        Engine::open(backend, entropy()).await,
        Err(OpenError::Contradiction { .. })
    ));
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn advancing_publication_starts_leak_only_collection_without_poisoning_the_writer()
-> TestResult {
    let delete_gate = Gate::new();
    let failed_run = wal_key(2);
    let store = FaultStore::new()
        .inject(Fault::FailBefore {
            selection: Selection::delete(Target::Key(failed_run.clone())),
            failure: BackendFailure::Transport,
        })?
        .gate(
            Selection::delete(Target::Key(failed_run.clone())),
            delete_gate.clone(),
        )?;
    let backend = store.backend();
    let engine = Engine::open(Arc::clone(&backend), entropy()).await?;
    let mut streams = drive_checkpoint_span(&engine).await?;

    delete_gate.wait_until_blocked().await;
    delete_gate.release();
    delete_gate.wait_until_finished().await;
    let after_collection: StreamPath = "checkpoint/after-collection-fault".parse()?;
    assert_eq!(
        CreateOutcome::Created,
        create(&engine, &after_collection).await?
    );
    streams.push(after_collection);

    assert_eq!(CloseOutcome::Shutdown, engine.shutdown().await);
    assert_object_present(&backend, &failed_run).await?;
    assert_reopens_with_streams(backend, &streams).await?;
    store.verify()?;
    Ok(())
}

async fn assert_reopens_with_streams(
    backend: Arc<dyn ObjectStore>,
    streams: &[StreamPath],
) -> TestResult {
    let reopened = Engine::open(backend, entropy()).await?;
    for id in streams {
        assert!(
            matches!(reopened.stream(id)?, StreamStatus::Live { .. }),
            "reopen lost stream {id}"
        );
    }
    assert_eq!(
        CloseOutcome::Shutdown,
        reopened.shutdown().await,
        "reopen remains healthy after recovering every expected stream"
    );
    Ok(())
}

async fn checkpoint_seal(
    backend: &Arc<dyn ObjectStore>,
    partition: strom_storage_domain::PartitionId,
    keys: &CheckpointKeys,
) -> Result<Seal, Box<dyn std::error::Error>> {
    let directory = table_ref(backend, &keys.directory).await?;
    let ledger = table_ref(backend, &keys.ledger).await?;
    let directory = TreeVersion::try_from(vec![SortedRun::try_from(vec![directory])?])?;
    let ledger = TreeVersion::try_from(vec![SortedRun::try_from(vec![ledger])?])?;
    let checkpoint: TableKey = keys.directory.as_str().parse()?;
    let fresh = checkpoint.object().fresh();
    Ok(Seal::new(
        partition,
        fresh.birth_generation(),
        WalReplayPoint::Through {
            batch: BatchId::try_from(CHECKPOINT_CUT)?,
            owner: OwnerToken::from(fresh.attempt().owner_claim()),
        },
        directory,
        ledger,
    )?)
}

async fn table_ref(
    backend: &Arc<dyn ObjectStore>,
    key: &ObjectKey,
) -> Result<TableRef, Box<dyn std::error::Error>> {
    let metadata = backend.head(&Path::from(key.as_str())).await?;
    let bytes = NonZeroU64::new(metadata.size).ok_or("a checkpoint table is nonempty")?;
    let key: TableKey = key.as_str().parse()?;
    Ok(TableRef::new(key.object(), bytes)?)
}

async fn put_foreign(backend: &Arc<dyn ObjectStore>, key: &ObjectKey) -> TestResult {
    backend
        .put(
            &Path::from(key.as_str()),
            PutPayload::from_static(b"foreign"),
        )
        .await?;
    Ok(())
}
