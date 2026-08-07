use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _};
use strom_common::{Entropy, Seed};
use strom_domain::{CreateOutcome, ExpiryPolicy, StreamContentType, StreamId, StreamLifecycle};
use strom_object_store::ObjectKey;
use strom_storage_domain::{
    AttemptId, BatchId, FreshIdentity, SealGeneration, SealKey, SealNamespace, TableKey,
    TableObjectId, WalKey, WalNamespace,
};
use strom_storage_engine::{Engine, StreamError};

pub(crate) type TestResult = Result<(), Box<dyn std::error::Error>>;

pub(crate) async fn create(engine: &Engine, id: &StreamId) -> Result<CreateOutcome, StreamError> {
    engine
        .create_stream(
            id,
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

pub(crate) fn wal_namespace() -> ObjectKey {
    WalNamespace
        .to_string()
        .parse()
        .expect("WAL namespace spelling is a canonical object key")
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
