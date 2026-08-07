use std::num::NonZeroU64;
use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};
use strom_domain::{ExpiryPolicy, StreamContentType, StreamId, StreamLifecycle, StreamStatus};
use strom_object_store::ObjectKey;
use strom_object_store::test_support::{
    BackendFailure, Fault, FaultStore, Gate, Operation, Selection, Target,
};
use strom_storage_domain::{
    AttemptId, BatchId, DirectoryEntry, DirectoryKey, FreshIdentity, LedgerCell, OperationFact,
    OwnerToken, PartitionId, Seal, SealGeneration, SortedRun, StoreKind, StreamRecord, StreamUid,
    TableKey, TableObjectId, TableRef, TreeVersion, WalBody, WalFacts, WalObject, WalReplayPoint,
    encode_directory_sst, encode_ledger_sst, encode_seal, encode_wal,
};
use strom_storage_engine::{CloseOutcome, Engine, OpenError};

use super::support::{
    TestResult, assert_object_absent, assert_object_present, entropy, seal_key, seal_namespace,
    wal_key, wal_namespace,
};

#[tokio::test]
async fn empty_namespace_creates_genesis_and_reopens() -> TestResult {
    let store = FaultStore::new();
    let backend = store.backend();

    let engine = Engine::open(Arc::clone(&backend), entropy()).await?;
    let partition = engine.partition_id();
    assert_eq!(CloseOutcome::Shutdown, engine.shutdown().await);
    store.assert_called_once(Operation::Create, &seal_key(1))?;

    let reopened = Engine::open(backend, entropy()).await?;
    assert_eq!(partition, reopened.partition_id());
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn genesis_race_loser_adopts_the_winning_partition() -> TestResult {
    let gate = Gate::new();
    let genesis_key = seal_key(1);
    let store = FaultStore::new().gate(
        Selection::create(Target::Key(genesis_key.clone())),
        gate.clone(),
    )?;
    let backend = store.backend();
    let winner = partition();

    let opening = Engine::open(Arc::clone(&backend), entropy());
    tokio::pin!(opening);
    tokio::select! {
        () = gate.wait_until_blocked() => {}
        outcome = &mut opening => panic!("genesis create passed its held gate: {outcome:?}"),
    }
    put_seal(&backend, &genesis(winner)).await?;
    gate.release();

    let engine = opening.await?;
    assert_eq!(winner, engine.partition_id());
    assert_eq!(CloseOutcome::Shutdown, engine.shutdown().await);
    store.assert_called_once(Operation::Create, &genesis_key)?;
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn unresolved_genesis_match_is_retryable_and_reopens_from_genesis() -> TestResult {
    let genesis_key = seal_key(1);
    let store = FaultStore::new().inject(Fault::CreateThenLoseResponse {
        target: Target::Key(genesis_key.clone()),
    })?;

    assert!(matches!(
        Engine::open(store.backend(), entropy()).await,
        Err(OpenError::Retryable { .. })
    ));
    store.assert_called_once(Operation::Create, &genesis_key)?;
    let backend = store.backend();
    assert_object_present(&backend, &genesis_key).await?;
    assert_object_absent(&backend, &seal_key(2)).await?;
    assert_object_absent(&backend, &wal_key(1)).await?;

    let reopened = Engine::open(backend, entropy()).await?;
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn unresolved_genesis_absence_is_retryable_and_reopens_from_empty() -> TestResult {
    let genesis_key = seal_key(1);
    let store = FaultStore::new().inject(Fault::FailBefore {
        selection: Selection::create(Target::Key(genesis_key.clone())),
        failure: BackendFailure::Transport,
    })?;

    assert!(matches!(
        Engine::open(store.backend(), entropy()).await,
        Err(OpenError::Retryable { .. })
    ));
    store.assert_called_once(Operation::Create, &genesis_key)?;
    let backend = store.backend();
    assert_object_absent(&backend, &genesis_key).await?;
    let reopened = Engine::open(backend, entropy()).await?;
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn unresolved_writer_claim_match_is_retryable_and_reopens_from_claim() -> TestResult {
    let claim_key = seal_key(2);
    let store = FaultStore::new().inject(Fault::CreateThenLoseResponse {
        target: Target::Key(claim_key.clone()),
    })?;

    assert!(matches!(
        Engine::open(store.backend(), entropy()).await,
        Err(OpenError::Retryable { .. })
    ));
    store.assert_called_once(Operation::Create, &claim_key)?;
    let backend = store.backend();
    assert_object_present(&backend, &claim_key).await?;
    assert_object_absent(&backend, &wal_key(1)).await?;

    let reopened = Engine::open(backend, entropy()).await?;
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn unresolved_absent_writer_claim_reopens_from_genesis() -> TestResult {
    let claim_key = seal_key(2);
    let store = FaultStore::new().inject(Fault::FailBefore {
        selection: Selection::create(Target::Key(claim_key.clone())),
        failure: BackendFailure::Transport,
    })?;

    assert!(matches!(
        Engine::open(store.backend(), entropy()).await,
        Err(OpenError::Retryable { .. })
    ));
    store.assert_called_once(Operation::Create, &claim_key)?;
    let backend = store.backend();
    assert_object_present(&backend, &seal_key(1)).await?;
    assert_object_absent(&backend, &claim_key).await?;
    let reopened = Engine::open(backend, entropy()).await?;
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn seal_list_failure_is_retryable_and_reopens_from_empty() -> TestResult {
    let store = FaultStore::new().inject(Fault::FailBefore {
        selection: Selection::list(seal_namespace()),
        failure: BackendFailure::Transport,
    })?;

    assert!(matches!(
        Engine::open(store.backend(), entropy()).await,
        Err(OpenError::Retryable { .. })
    ));
    let reopened = Engine::open(store.backend(), entropy()).await?;
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn seal_read_failure_is_retryable_and_reopens() -> TestResult {
    let genesis_key = seal_key(1);
    let store = FaultStore::new().inject(Fault::FailBefore {
        selection: Selection::read(Target::Key(genesis_key)),
        failure: BackendFailure::Transport,
    })?;
    let backend = store.backend();
    put_seal(&backend, &genesis(partition())).await?;

    assert!(matches!(
        Engine::open(Arc::clone(&backend), entropy()).await,
        Err(OpenError::Retryable { .. })
    ));
    let reopened = Engine::open(backend, entropy()).await?;
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn wal_list_failure_reopens_from_published_claim() -> TestResult {
    let store = FaultStore::new().inject(Fault::FailBefore {
        selection: Selection::list(wal_namespace()),
        failure: BackendFailure::Transport,
    })?;

    assert!(matches!(
        Engine::open(store.backend(), entropy()).await,
        Err(OpenError::Retryable { .. })
    ));
    let backend = store.backend();
    assert_object_present(&backend, &seal_key(2)).await?;
    assert_object_absent(&backend, &wal_key(1)).await?;
    let reopened = Engine::open(backend, entropy()).await?;
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn wal_read_failure_reopens_from_published_fence() -> TestResult {
    let fence_key = wal_key(1);
    let store = FaultStore::new().inject(Fault::FailBefore {
        selection: Selection::read(Target::Key(fence_key.clone())),
        failure: BackendFailure::Transport,
    })?;

    assert!(matches!(
        Engine::open(store.backend(), entropy()).await,
        Err(OpenError::Retryable { .. })
    ));
    store.assert_called_once(Operation::Create, &fence_key)?;
    store.assert_called_once(Operation::Read, &fence_key)?;
    let backend = store.backend();
    assert_object_present(&backend, &seal_key(2)).await?;
    assert_object_present(&backend, &fence_key).await?;

    let reopened = Engine::open(backend, entropy()).await?;
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn planted_tables_merge_newest_wins_and_replay_continues_after_the_cut() -> TestResult {
    let store = FaultStore::new();
    let backend = store.backend();
    let partition = partition();
    let generation_1 = SealGeneration::genesis();
    let generation_2 = generation_1.successor()?;
    let base = "events/base".parse::<StreamId>()?;
    let deleted = "events/deleted".parse::<StreamId>()?;
    let uid = StreamUid::try_from(1)?;
    let deleted_uid = StreamUid::try_from(2)?;
    let created_at = BatchId::try_from(1)?;

    let directory_key = table_key(generation_2, StoreKind::Directory, 0)?;
    let directory_bytes = encode_directory_sst(
        partition,
        &directory_key,
        &[
            (DirectoryKey::from(&base), DirectoryEntry::Live(uid)),
            (
                DirectoryKey::from(&deleted),
                DirectoryEntry::Tombstone(deleted_uid),
            ),
        ],
    )?;
    let directory_ref = plant_table(&backend, &directory_key, directory_bytes).await?;

    let older_key = table_key(generation_2, StoreKind::Ledger, 1)?;
    let older_bytes = encode_ledger_sst(
        partition,
        &older_key,
        &[
            (
                uid,
                LedgerCell::Value(StreamRecord::new(
                    StreamContentType::octet_stream(),
                    ExpiryPolicy::None,
                    StreamLifecycle::Open,
                    created_at,
                )),
            ),
            (
                deleted_uid,
                LedgerCell::Value(StreamRecord::new(
                    StreamContentType::octet_stream(),
                    ExpiryPolicy::None,
                    StreamLifecycle::Open,
                    created_at,
                )),
            ),
        ],
    )?;
    let older_ref = plant_table(&backend, &older_key, older_bytes).await?;

    let newer_key = table_key(generation_2, StoreKind::Ledger, 2)?;
    let newer_bytes = encode_ledger_sst(
        partition,
        &newer_key,
        &[
            (
                uid,
                LedgerCell::Value(StreamRecord::new(
                    "application/json".parse()?,
                    ExpiryPolicy::None,
                    StreamLifecycle::Closed,
                    created_at,
                )),
            ),
            (deleted_uid, LedgerCell::Delete),
        ],
    )?;
    let newer_ref = plant_table(&backend, &newer_key, newer_bytes).await?;

    put_seal(&backend, &genesis(partition)).await?;
    let directory = TreeVersion::try_from(vec![SortedRun::try_from(vec![directory_ref])?])?;
    let ledger = TreeVersion::try_from(vec![
        SortedRun::try_from(vec![newer_ref])?,
        SortedRun::try_from(vec![older_ref])?,
    ])?;
    put_seal(
        &backend,
        &Seal::new(
            partition,
            generation_2,
            WalReplayPoint::Through {
                batch: created_at,
                owner: OwnerToken::from(generation_1),
            },
            directory,
            ledger,
        )?,
    )
    .await?;

    let engine = Engine::open(Arc::clone(&backend), entropy()).await?;
    assert_eq!(
        StreamStatus::Live {
            content_type: "application/json".parse()?,
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Closed,
        },
        engine.stream(&base)?,
    );
    assert_eq!(StreamStatus::Deleted, engine.stream(&deleted)?);
    assert_object_present(&backend, &wal_key(2)).await?;
    assert_eq!(CloseOutcome::Shutdown, engine.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn replay_gap_below_the_takeover_fence_is_a_contradiction() -> TestResult {
    let store = FaultStore::new();
    let backend = store.backend();
    let partition = partition();
    put_replay_head(&backend, partition).await?;
    put_wal(
        &backend,
        &WalObject::new(
            partition,
            2.try_into()?,
            OwnerToken::from(SealGeneration::try_from(2)?),
            WalBody::Fence,
        ),
    )
    .await?;

    assert!(matches!(
        Engine::open(backend, entropy()).await,
        Err(OpenError::Contradiction { .. })
    ));
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn replay_rejects_a_nonincreasing_fence_owner() -> TestResult {
    let store = FaultStore::new();
    let backend = store.backend();
    let partition = partition();
    let owner = OwnerToken::from(SealGeneration::try_from(2)?);
    put_replay_head(&backend, partition).await?;
    for batch in [1, 2] {
        put_wal(
            &backend,
            &WalObject::new(partition, batch.try_into()?, owner, WalBody::Fence),
        )
        .await?;
    }

    assert!(matches!(
        Engine::open(backend, entropy()).await,
        Err(OpenError::Contradiction { .. })
    ));
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn replay_rejects_facts_that_contradict_recovered_state() -> TestResult {
    let store = FaultStore::new();
    let backend = store.backend();
    let partition = partition();
    let owner = OwnerToken::from(SealGeneration::try_from(2)?);
    put_replay_head(&backend, partition).await?;
    put_wal(
        &backend,
        &WalObject::new(partition, 1.try_into()?, owner, WalBody::Fence),
    )
    .await?;
    let missing = OperationFact::StreamDeleted {
        path: DirectoryKey::try_from(Box::<[u8]>::from(b"events/missing".as_slice()))?,
        uid: StreamUid::try_from(1)?,
    };
    put_wal(
        &backend,
        &WalObject::new(
            partition,
            2.try_into()?,
            owner,
            WalBody::Run(WalFacts::try_from(vec![missing])?),
        ),
    )
    .await?;

    assert!(matches!(
        Engine::open(backend, entropy()).await,
        Err(OpenError::Contradiction { .. })
    ));
    store.verify()?;
    Ok(())
}

async fn put_replay_head(store: &Arc<dyn ObjectStore>, partition: PartitionId) -> TestResult {
    put_seal(store, &genesis(partition)).await?;
    put_seal(
        store,
        &Seal::new(
            partition,
            SealGeneration::try_from(2)?,
            WalReplayPoint::Genesis,
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?,
    )
    .await
}

async fn put_seal(store: &Arc<dyn ObjectStore>, seal: &Seal) -> TestResult {
    put_object(store, seal_key(seal.generation().get()), encode_seal(seal)?).await
}

async fn put_wal(store: &Arc<dyn ObjectStore>, wal: &WalObject) -> TestResult {
    put_object(store, wal_key(wal.batch().get()), encode_wal(wal)?).await
}

async fn put_object(store: &Arc<dyn ObjectStore>, key: ObjectKey, bytes: Vec<u8>) -> TestResult {
    store
        .put(&Path::from(key.as_str()), PutPayload::from(bytes))
        .await?;
    Ok(())
}

fn genesis(partition: PartitionId) -> Seal {
    Seal::new(
        partition,
        SealGeneration::genesis(),
        WalReplayPoint::Genesis,
        TreeVersion::empty(),
        TreeVersion::empty(),
    )
    .expect("canonical genesis is valid")
}

fn table_key(
    birth: SealGeneration,
    store: StoreKind,
    ordinal: u32,
) -> Result<TableKey, Box<dyn std::error::Error>> {
    let fresh = FreshIdentity::new(birth, AttemptId::new(SealGeneration::genesis(), 1), ordinal)?;
    Ok(TableKey::new(TableObjectId::new(fresh, store)))
}

async fn plant_table(
    store: &Arc<dyn ObjectStore>,
    key: &TableKey,
    bytes: Vec<u8>,
) -> Result<TableRef, Box<dyn std::error::Error>> {
    let object_bytes =
        NonZeroU64::new(u64::try_from(bytes.len())?).expect("encoded SST bodies are nonempty");
    put_object(store, ObjectKey::try_from(key.to_string())?, bytes).await?;
    Ok(TableRef::new(key.object(), object_bytes)?)
}

fn partition() -> PartitionId {
    "00112233-4455-6677-8899-aabbccddeeff"
        .parse()
        .expect("test partition is canonical")
}
