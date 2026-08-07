use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt as _;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _};
use strom_common::{Entropy, Seed};
use strom_domain::{CreateOutcome, ExpiryPolicy, StreamContentType, StreamLifecycle, StreamPath};
use strom_object_store::ObjectKey;
use strom_object_store::test_support::FaultStore;
use strom_storage_domain::{
    AttemptId, BatchId, FreshIdentity, SealGeneration, SealKey, SealNamespace, StoreKind, TableKey,
    TableObjectId, WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER, WalKey,
};
use strom_storage_engine::{CloseOutcome, Engine, StreamError};

pub(crate) type TestResult = Result<(), Box<dyn std::error::Error>>;

const CHECKPOINT_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub(crate) struct CheckpointKeys {
    pub(crate) seal: ObjectKey,
    pub(crate) directory: ObjectKey,
    pub(crate) ledger: ObjectKey,
}

pub(crate) async fn create(
    engine: &Engine,
    path: &StreamPath,
) -> Result<CreateOutcome, StreamError> {
    engine
        .create_stream(
            path,
            StreamContentType::octet_stream(),
            ExpiryPolicy::None,
            StreamLifecycle::Open,
        )
        .await
}

pub(crate) fn entropy() -> Entropy {
    const TEST_SEED: u64 = 42;
    Entropy::from_seed(Seed::from(TEST_SEED))
}

pub(crate) fn wal_key(batch: u64) -> ObjectKey {
    let batch = BatchId::try_from(batch).expect("test WAL batch is nonzero");
    WalKey::from(batch)
        .to_string()
        .parse()
        .expect("WAL key spelling is a canonical object key")
}

pub(crate) fn seal_key(generation: u64) -> ObjectKey {
    let generation = SealGeneration::try_from(generation).expect("test Seal generation is nonzero");
    SealKey::from(generation)
        .to_string()
        .parse()
        .expect("Seal key spelling is a canonical object key")
}

/// Drive one fresh engine through the first checkpoint and return the observed keys.
///
/// The Seal generation is derived from the durable Directory/Ledger children, not
/// from a hardcoded coordinate.
pub(crate) async fn observe_checkpoint_keys() -> Result<CheckpointKeys, Box<dyn std::error::Error>>
{
    let store = FaultStore::new();
    let backend = store.backend();
    let engine = Engine::open(Arc::clone(&backend), entropy()).await?;
    drive_checkpoint_span(&engine).await?;

    let (directory, ledger) = tokio::time::timeout(CHECKPOINT_OBSERVATION_TIMEOUT, async {
        loop {
            let mut directory = None;
            let mut ledger = None;
            let mut listing = backend.list(None);
            while let Some(metadata) = listing.try_next().await? {
                let Ok(key) = metadata.location.as_ref().parse::<ObjectKey>() else {
                    continue;
                };
                let Ok(table) = key.as_str().parse::<TableKey>() else {
                    continue;
                };
                match table.object().store() {
                    StoreKind::Directory => directory = Some(key),
                    StoreKind::Ledger => ledger = Some(key),
                    StoreKind::Tally | StoreKind::Annals => {}
                }
            }
            if let (Some(directory), Some(ledger)) = (directory, ledger) {
                return Ok::<_, object_store::Error>((directory, ledger));
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;

    assert_eq!(
        CloseOutcome::Shutdown,
        engine.shutdown().await,
        "the observation run remains healthy after producing checkpoint children"
    );
    let directory_table: TableKey = directory.as_str().parse()?;
    let ledger_table: TableKey = ledger.as_str().parse()?;
    let directory_fresh = directory_table.object().fresh();
    let ledger_fresh = ledger_table.object().fresh();
    assert_eq!(
        directory_fresh.birth_generation(),
        ledger_fresh.birth_generation(),
        "one checkpoint gives every child the same birth generation"
    );
    assert_eq!(
        directory_fresh.attempt(),
        ledger_fresh.attempt(),
        "one checkpoint gives every child the same attempt identity"
    );
    let seal = SealKey::from(directory_fresh.birth_generation())
        .to_string()
        .parse()?;
    Ok(CheckpointKeys {
        seal,
        directory,
        ledger,
    })
}

pub(crate) async fn drive_checkpoint_span(
    engine: &Engine,
) -> Result<Vec<StreamPath>, Box<dyn std::error::Error>> {
    let mut streams = Vec::new();
    for ordinal in 0..WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER {
        let id: StreamPath = format!("checkpoint/{ordinal}").parse()?;
        assert_eq!(
            CreateOutcome::Created,
            create(engine, &id).await?,
            "checkpoint setup command {ordinal} becomes durable"
        );
        streams.push(id);
    }
    Ok(streams)
}

pub(crate) fn checkpoint_table_key_at_attempt(observed: &ObjectKey, attempt: u64) -> ObjectKey {
    let observed: TableKey = observed
        .as_str()
        .parse()
        .expect("an observed checkpoint table has a canonical table key");
    let object = observed.object();
    let fresh = object.fresh();
    let retry = FreshIdentity::new(
        fresh.birth_generation(),
        AttemptId::new(fresh.attempt().owner_claim(), attempt),
        fresh.ordinal(),
    )
    .expect("changing an attempt preserves a fresh table identity");
    TableKey::new(TableObjectId::new(retry, object.store()))
        .to_string()
        .parse()
        .expect("table key spelling is a canonical object key")
}

pub(crate) fn seal_namespace() -> ObjectKey {
    SealNamespace
        .to_string()
        .parse()
        .expect("Seal namespace spelling is a canonical object key")
}

pub(crate) async fn assert_object_present(
    store: &Arc<dyn ObjectStore>,
    key: &ObjectKey,
) -> TestResult {
    store.head(&Path::from(key.as_str())).await?;
    Ok(())
}

pub(crate) async fn assert_object_absent(
    store: &Arc<dyn ObjectStore>,
    key: &ObjectKey,
) -> TestResult {
    match store.head(&Path::from(key.as_str())).await {
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Ok(metadata) => {
            Err(format!("expected durable object {key} to be absent, found {metadata:?}").into())
        }
        Err(error) => Err(error.into()),
    }
}
