//! Behavioral claims for the public engine boundary.

use strom_common::{Entropy, Seed};
use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};
use strom_object_store::ObjectStoreAdapter;
use strom_storage_domain::{DirectoryEntry, DirectoryKey};
use strom_storage_engine::{
    AdmissionRefusal, CommandError, Engine, StreamCommand, StreamReply, WriterExit,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn success_is_visible_before_reply_and_survives_reopen() -> TestResult {
    let adapter = ObjectStoreAdapter::in_memory();
    let path = directory_key("events/a")?;
    let engine = Engine::open(adapter.clone(), test_entropy()).await?;
    let partition = engine.partition_id();
    let reply = engine.command(create(path.clone())).await?;
    let StreamReply::Created { uid } = reply else {
        return Err("create returned the wrong reply variant".into());
    };
    assert_eq!(
        Some(DirectoryEntry::Live(uid)),
        engine.snapshot()?.resolve(&path),
        "a success reply is released only after its immutable view is installed"
    );
    assert_eq!(WriterExit::Shutdown, engine.shutdown().await);

    let reopened = Engine::open(adapter, test_entropy()).await?;
    assert_eq!(
        partition,
        reopened.partition_id(),
        "reopen discovers the genesis-born partition identity"
    );
    assert_eq!(
        Some(DirectoryEntry::Live(uid)),
        reopened.snapshot()?.resolve(&path),
        "bootstrap replay reconstructs an acknowledged write"
    );
    assert_eq!(WriterExit::Shutdown, reopened.shutdown().await);
    Ok(())
}

#[tokio::test]
async fn duplicate_create_is_idempotent_and_survives_reopen() -> TestResult {
    let adapter = ObjectStoreAdapter::in_memory();
    let path = directory_key("events/dup")?;
    let engine = Engine::open(adapter.clone(), test_entropy()).await?;
    let first = engine.command(create(path.clone())).await?;
    let StreamReply::Created { uid } = first else {
        return Err("first create returned the wrong reply variant".into());
    };
    assert_eq!(
        Ok(StreamReply::AlreadyCreated { uid }),
        engine.command(create(path.clone())).await
    );
    assert_eq!(
        Some(DirectoryEntry::Live(uid)),
        engine.snapshot()?.resolve(&path),
        "an idempotent create does not change the published view"
    );
    assert_eq!(WriterExit::Shutdown, engine.shutdown().await);

    let reopened = Engine::open(adapter, test_entropy()).await?;
    assert_eq!(
        Some(DirectoryEntry::Live(uid)),
        reopened.snapshot()?.resolve(&path),
        "an idempotent reply does not change the recovered state"
    );
    assert_eq!(WriterExit::Shutdown, reopened.shutdown().await);
    Ok(())
}

#[tokio::test]
async fn typed_refusal_does_not_change_the_published_view() -> TestResult {
    let engine = Engine::open(ObjectStoreAdapter::in_memory(), test_entropy()).await?;
    let missing = directory_key("events/missing")?;
    assert_eq!(
        Err(CommandError::Refused(AdmissionRefusal::PathNotLive)),
        engine
            .command(StreamCommand::Delete {
                path: missing.clone(),
            })
            .await
    );
    assert_eq!(None, engine.snapshot()?.resolve(&missing));
    assert_eq!(WriterExit::Shutdown, engine.shutdown().await);
    Ok(())
}

#[tokio::test]
async fn a_new_engine_revokes_the_previous_writer() -> TestResult {
    let adapter = ObjectStoreAdapter::in_memory();
    let previous = Engine::open(adapter.clone(), test_entropy()).await?;
    let current = Engine::open(adapter, test_entropy()).await?;

    assert_eq!(
        Err(CommandError::Indeterminate),
        previous
            .command(create(directory_key("events/fenced")?))
            .await
    );
    assert!(matches!(
        previous.snapshot(),
        Err(CommandError::Unavailable)
    ));
    assert!(matches!(
        previous.shutdown().await,
        WriterExit::Fenced { .. }
    ));
    assert_eq!(WriterExit::Shutdown, current.shutdown().await);
    Ok(())
}

const fn create(path: DirectoryKey) -> StreamCommand {
    StreamCommand::Create {
        path,
        content_type: StreamContentType::octet_stream(),
        expiry: ExpiryPolicy::None,
        lifecycle: StreamLifecycle::Open,
    }
}

fn directory_key(raw: &str) -> Result<DirectoryKey, Box<dyn std::error::Error>> {
    Ok(DirectoryKey::try_from(Box::<[u8]>::from(raw.as_bytes()))?)
}

fn test_entropy() -> Entropy {
    const TEST_SEED: u64 = 42;
    Entropy::from_seed(Seed::from(TEST_SEED))
}
