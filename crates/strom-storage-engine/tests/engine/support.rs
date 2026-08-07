use strom_common::{Entropy, Seed};
use strom_domain::{CreateOutcome, ExpiryPolicy, StreamContentType, StreamId, StreamLifecycle};
use strom_object_store::ObjectKey;
use strom_storage_domain::{BatchId, SealGeneration, SealKey, WalKey};
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
