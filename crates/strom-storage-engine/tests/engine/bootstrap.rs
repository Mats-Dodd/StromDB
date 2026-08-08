use std::num::NonZeroU64;
use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};
use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle, StreamPath, StreamStatus};
use strom_object_store::ObjectKey;
use strom_object_store::test_support::{
    BackendFailure, Fault, FaultStore, Gate, Operation, Selection, Target,
};
use strom_storage_domain::{
    AttemptId, BatchId, DirectoryEntry, FreshIdentity, LedgerCell, OwnerToken, PartitionId, Seal,
    SealGeneration, SortedRun, StoreKind, StreamRecord, StreamUid, TableKey, TableObjectId,
    TableRef, TreeVersion, WalReplayPoint, encode_directory_sst, encode_ledger_sst, encode_seal,
};
use strom_storage_engine::{CloseOutcome, OpenError};

use super::support::{
    TestResult, assert_object_present, entropy, open_engine, seal_key, seal_namespace, wal_key,
};

#[tokio::test]
async fn empty_namespace_creates_genesis_and_reopens() -> TestResult {
    let store = FaultStore::new();
    let backend = store.backend();

    let engine = open_engine(Arc::clone(&backend), entropy()).await?;
    let partition = engine.partition_id();
    assert_eq!(CloseOutcome::Shutdown, engine.shutdown().await);
    store.assert_called_once(Operation::Create, &seal_key(1))?;

    let reopened = open_engine(backend, entropy()).await?;
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

    let opening = open_engine(Arc::clone(&backend), entropy());
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
async fn retryable_store_failure_crosses_the_interpreter_seam() -> TestResult {
    let store = FaultStore::new().inject(Fault::FailBefore {
        selection: Selection::list(seal_namespace()),
        failure: BackendFailure::Transport,
    })?;

    assert!(matches!(
        open_engine(store.backend(), entropy()).await,
        Err(OpenError::Retryable { .. })
    ));
    let reopened = open_engine(store.backend(), entropy()).await?;
    assert_eq!(CloseOutcome::Shutdown, reopened.shutdown().await);
    store.verify()?;
    Ok(())
}

#[tokio::test]
async fn planted_tables_and_wal_replay_open_the_expected_view() -> TestResult {
    let store = FaultStore::new();
    let backend = store.backend();
    let partition = partition();
    let generation_1 = SealGeneration::genesis();
    let generation_2 = generation_1.successor()?;
    let base = "events/base".parse::<StreamPath>()?;
    let deleted = "events/deleted".parse::<StreamPath>()?;
    let uid = StreamUid::try_from(1)?;
    let deleted_uid = StreamUid::try_from(2)?;
    let created_at = BatchId::try_from(1)?;

    let directory_key = table_key(generation_2, StoreKind::Directory, 0)?;
    let directory_bytes = encode_directory_sst(
        partition,
        &directory_key,
        &[
            (base.clone(), DirectoryEntry::Live(uid)),
            (deleted.clone(), DirectoryEntry::Tombstone(deleted_uid)),
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

    let engine = open_engine(Arc::clone(&backend), entropy()).await?;
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

async fn put_seal(store: &Arc<dyn ObjectStore>, seal: &Seal) -> TestResult {
    put_object(store, seal_key(seal.generation().get()), encode_seal(seal)?).await
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
