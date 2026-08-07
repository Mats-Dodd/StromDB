//! Checkpoint publication and its bounded storage pipeline.

#![expect(
    clippy::disallowed_types,
    reason = "the single writer and checkpoint task share this one enumerated publication handshake"
)]

mod collect;
mod prepare;

pub(crate) use collect::collect_advance;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::{StreamExt as _, stream};
use strom_object_store::ObjectStoreAdapter;
use strom_storage_domain::{AttemptId, BatchId, Seal};
use tokio::sync::{Notify, Semaphore, mpsc, oneshot};

use crate::forest::Forest;
use crate::store::{
    EncodedAuthoritySeal, EncodedTable, SealPublication, SealStore, TableEstablishment, TableStore,
    TypedStoreError,
};

use self::prepare::prepare_checkpoint;

const CHECKPOINT_CHILD_CREATES_MAX: usize = 16;
const CHECKPOINT_PREPARATIONS_MAX: usize = 2;
// Keep preparation behind the fixed-width child-create pipeline instead of
// retaining a checkpoint's encoded tables in aggregate.
const CHECKPOINT_TABLE_CHANNEL_MAX: usize = 1;
static CHECKPOINT_PREPARATIONS: Semaphore = Semaphore::const_new(CHECKPOINT_PREPARATIONS_MAX);

#[derive(Debug)]
pub(crate) struct CheckpointInput {
    pub(crate) source: Seal,
    pub(crate) base: Forest,
    pub(crate) snapshot: Forest,
    pub(crate) cut: BatchId,
    pub(crate) attempt: AttemptId,
}

#[derive(Debug)]
struct PreparedCheckpoint {
    source: Seal,
    successor: Seal,
    snapshot: Forest,
    encoded_seal: EncodedAuthoritySeal,
}

impl PreparedCheckpoint {
    fn into_install(self) -> CheckpointInstall {
        CheckpointInstall {
            source: self.source,
            successor: self.successor,
            snapshot: self.snapshot,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CheckpointInstall {
    pub(super) source: Seal,
    pub(super) successor: Seal,
    pub(super) snapshot: Forest,
}

#[derive(Debug)]
pub(crate) enum CheckpointCompletion {
    Installed(CheckpointInstall),
    Abandoned,
    Fenced,
    Poisoned { detail: String },
    Contradiction { detail: String },
}

#[derive(Debug, Clone)]
pub(crate) struct PublicationGate(Arc<PublicationState>);

#[derive(Debug)]
struct PublicationState {
    claimed: AtomicBool,
    notify: Notify,
}

impl PublicationGate {
    pub(crate) fn new() -> Self {
        Self(Arc::new(PublicationState {
            claimed: AtomicBool::new(false),
            notify: Notify::new(),
        }))
    }

    pub(crate) fn cancel_before_publish(&self) -> bool {
        self.claim()
    }

    fn claim(&self) -> bool {
        let claimed = self
            .0
            .claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if claimed {
            self.0.notify.notify_waiters();
        }
        claimed
    }

    fn begin_publish(&self) -> bool {
        self.claim()
    }

    async fn claimed(&self) {
        loop {
            if self.0.claimed.load(Ordering::Acquire) {
                return;
            }
            let notified = self.0.notify.notified();
            if self.0.claimed.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

pub(crate) async fn execute_checkpoint(
    adapter: ObjectStoreAdapter,
    input: CheckpointInput,
    publication: PublicationGate,
) -> CheckpointCompletion {
    let permit = tokio::select! {
        biased;
        () = publication.claimed() => return CheckpointCompletion::Abandoned,
        permit = CHECKPOINT_PREPARATIONS.acquire() => match permit {
            Ok(permit) => permit,
            Err(_closed) => {
                return CheckpointCompletion::Contradiction {
                    detail: "checkpoint preparation gate is closed".into(),
                };
            }
        },
    };
    let (table_sender, table_receiver) = mpsc::channel(CHECKPOINT_TABLE_CHANNEL_MAX);
    let (prepared_sender, prepared_receiver) = oneshot::channel();
    let preparation = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let prepared = prepare_checkpoint(input, &mut |table| {
            table_sender.blocking_send(table).is_ok()
        });
        let _consumer_may_be_gone = prepared_sender.send(prepared);
    });

    let store = TableStore::new(adapter.clone());
    let table_result = establish_tables(&store, table_receiver, publication.clone()).await;
    if let Err(join_error) = preparation.await {
        return CheckpointCompletion::Contradiction {
            detail: format!("checkpoint preparation task failed: {join_error}"),
        };
    }
    match table_result {
        TableEstablishment::Established => {}
        TableEstablishment::Abandoned => return CheckpointCompletion::Abandoned,
        TableEstablishment::Contradiction { detail } => {
            return CheckpointCompletion::Contradiction { detail };
        }
    }
    let prepared = match prepared_receiver.await {
        Ok(Ok(Some(prepared))) => prepared,
        Ok(Ok(None)) => return CheckpointCompletion::Abandoned,
        Ok(Err(error)) => {
            return CheckpointCompletion::Contradiction {
                detail: error.to_string(),
            };
        }
        Err(_sender_dropped) => {
            return CheckpointCompletion::Contradiction {
                detail: "checkpoint preparation ended without a result".into(),
            };
        }
    };

    if !publication.begin_publish() {
        return CheckpointCompletion::Abandoned;
    }
    let seal_store = SealStore::new(adapter);
    match seal_store.publish_authority(&prepared.encoded_seal).await {
        Ok(SealPublication::Authored) => {
            CheckpointCompletion::Installed((*prepared).into_install())
        }
        Ok(SealPublication::NoAuthority) => CheckpointCompletion::Fenced,
        Ok(SealPublication::Unresolved) => CheckpointCompletion::Poisoned {
            detail: "advancing Seal create is unresolved".into(),
        },
        Err(TypedStoreError::Retryable { detail } | TypedStoreError::Rejected { detail }) => {
            CheckpointCompletion::Poisoned { detail }
        }
        Err(TypedStoreError::Contradiction { detail }) => {
            CheckpointCompletion::Contradiction { detail }
        }
    }
}

async fn establish_tables(
    store: &TableStore,
    mut receiver: mpsc::Receiver<EncodedTable>,
    publication: PublicationGate,
) -> TableEstablishment {
    let tables = stream::poll_fn(move |context| receiver.poll_recv(context));
    let creates = tables
        .map(|table| {
            let store = store.clone();
            async move { store.establish_table(&table).await }
        })
        .buffer_unordered(CHECKPOINT_CHILD_CREATES_MAX);
    futures::pin_mut!(creates);
    loop {
        tokio::select! {
            biased;
            () = publication.claimed() => return TableEstablishment::Abandoned,
            result = creates.next() => match result {
                Some(TableEstablishment::Established) => {}
                Some(TableEstablishment::Abandoned) => return TableEstablishment::Abandoned,
                Some(TableEstablishment::Contradiction { detail }) => {
                    return TableEstablishment::Contradiction { detail };
                }
                None => return TableEstablishment::Established,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};
    use strom_object_store::test_support::{BackendFailure, Fault, FaultStore, Selection, Target};
    use strom_object_store::{CreateEvidence, FrozenBytes, ObjectKey};
    use strom_storage_domain::{
        DirectoryKey, FreshIdentity, OperationFact, OwnerToken, SealGeneration, SealKey, StoreKind,
        StreamUid, TableKey, TableObjectId, TreeVersion, WalReplayPoint, encode_seal,
    };

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn checkpoint_installs_the_exact_successor_and_fences_on_an_occupied_coordinate()
    -> TestResult {
        let input = checkpoint_input()?;
        let expected = successor(&input)?;
        let completion = execute_checkpoint(
            ObjectStoreAdapter::in_memory(),
            input,
            PublicationGate::new(),
        )
        .await;
        let install = match completion {
            CheckpointCompletion::Installed(install) => install,
            other @ (CheckpointCompletion::Abandoned
            | CheckpointCompletion::Fenced
            | CheckpointCompletion::Poisoned { .. }
            | CheckpointCompletion::Contradiction { .. }) => {
                return Err(format!("direct successor publication installs, got {other:?}").into());
            }
        };
        assert_eq!(
            expected, install.successor,
            "the exact planned Seal installs"
        );

        let input = checkpoint_input()?;
        let adapter = ObjectStoreAdapter::in_memory();
        let key = seal_key(successor(&input)?.generation())?;
        let competitor = Seal::new(
            input.source.partition(),
            input.source.generation().successor()?,
            WalReplayPoint::Through {
                batch: input.cut,
                owner: OwnerToken::from(SealGeneration::genesis()),
            },
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;
        assert_eq!(
            CreateEvidence::Direct,
            adapter
                .create_if_absent(&key, FrozenBytes::try_from(encode_seal(&competitor)?)?)
                .await?
        );
        assert!(matches!(
            execute_checkpoint(adapter, input, PublicationGate::new()).await,
            CheckpointCompletion::Fenced
        ));

        let input = checkpoint_input()?;
        let matching = successor(&input)?;
        let adapter = ObjectStoreAdapter::in_memory();
        assert_eq!(
            CreateEvidence::Direct,
            adapter
                .create_if_absent(
                    &seal_key(matching.generation())?,
                    FrozenBytes::try_from(encode_seal(&matching)?)?,
                )
                .await?
        );
        assert!(matches!(
            execute_checkpoint(adapter, input, PublicationGate::new()).await,
            CheckpointCompletion::Fenced
        ));
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_maps_table_abandonment_and_contradiction_before_publication() -> TestResult
    {
        let input = checkpoint_input_with_rows()?;
        let key = table_key(&input, 0, StoreKind::Directory)?;
        let object_key = ObjectKey::try_from(key.to_string())?;
        let absent_fault = FaultStore::new().inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(object_key.clone())),
            failure: BackendFailure::Transport,
        })?;
        assert!(matches!(
            execute_checkpoint(
                ObjectStoreAdapter::new(absent_fault.backend()),
                input,
                PublicationGate::new(),
            )
            .await,
            CheckpointCompletion::Abandoned
        ));
        absent_fault.verify()?;

        let input = checkpoint_input_with_rows()?;
        let adapter = ObjectStoreAdapter::in_memory();
        assert_eq!(
            CreateEvidence::Direct,
            adapter
                .create_if_absent(
                    &object_key,
                    FrozenBytes::try_from(b"foreign table".to_vec())?,
                )
                .await?
        );
        assert!(matches!(
            execute_checkpoint(adapter, input, PublicationGate::new()).await,
            CheckpointCompletion::Contradiction { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_completion_preserves_abandon_poison_and_contradiction_classes() -> TestResult
    {
        let publication = PublicationGate::new();
        assert!(publication.cancel_before_publish());
        assert!(matches!(
            execute_checkpoint(
                ObjectStoreAdapter::in_memory(),
                checkpoint_input()?,
                publication
            )
            .await,
            CheckpointCompletion::Abandoned
        ));

        let input = checkpoint_input()?;
        let key = seal_key(successor(&input)?.generation())?;
        let unresolved_fault = FaultStore::new().inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(key.clone())),
            failure: BackendFailure::Transport,
        })?;
        let unresolved = execute_checkpoint(
            ObjectStoreAdapter::new(unresolved_fault.backend()),
            input,
            PublicationGate::new(),
        )
        .await;
        assert!(matches!(
            unresolved,
            CheckpointCompletion::Poisoned { ref detail }
                if detail == "advancing Seal create is unresolved"
        ));
        unresolved_fault.verify()?;

        let rejected_fault = FaultStore::new().inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(key)),
            failure: BackendFailure::PermissionDenied,
        })?;
        let rejected = execute_checkpoint(
            ObjectStoreAdapter::new(rejected_fault.backend()),
            checkpoint_input()?,
            PublicationGate::new(),
        )
        .await;
        assert!(
            matches!(
            rejected,
            CheckpointCompletion::Poisoned { ref detail }
                if detail.contains("injected PermissionDenied failure")
            ),
            "rejected Seal publication maps its detail into Poisoned: {rejected:?}"
        );
        rejected_fault.verify()?;

        let mut invalid = checkpoint_input()?;
        invalid.cut = invalid
            .source
            .replay()
            .batch()
            .unwrap_or(BatchId::try_from(1)?);
        invalid.source = Seal::new(
            invalid.source.partition(),
            invalid.source.generation(),
            WalReplayPoint::Through {
                batch: invalid.cut,
                owner: OwnerToken::from(SealGeneration::genesis()),
            },
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;
        let contradiction = execute_checkpoint(
            ObjectStoreAdapter::in_memory(),
            invalid,
            PublicationGate::new(),
        )
        .await;
        assert!(matches!(
            contradiction,
            CheckpointCompletion::Contradiction { ref detail }
                if detail.contains("does not advance")
        ));
        Ok(())
    }

    fn checkpoint_input_with_rows() -> TestResult<CheckpointInput> {
        let mut input = checkpoint_input()?;
        input.snapshot.strict_fold(
            input.cut,
            &OperationFact::StreamCreated {
                path: DirectoryKey::try_from(Box::<[u8]>::from(b"events/a".as_slice()))?,
                uid: StreamUid::try_from(1)?,
                content_type: StreamContentType::octet_stream(),
                expiry: ExpiryPolicy::None,
                lifecycle: StreamLifecycle::Open,
            },
        )?;
        Ok(input)
    }

    fn checkpoint_input() -> TestResult<CheckpointInput> {
        let partition = "00112233-4455-6677-8899-aabbccddeeff".parse()?;
        let generation = SealGeneration::genesis().successor()?;
        Ok(CheckpointInput {
            source: Seal::new(
                partition,
                generation,
                WalReplayPoint::Genesis,
                TreeVersion::empty(),
                TreeVersion::empty(),
            )?,
            base: Forest::empty(),
            snapshot: Forest::empty(),
            cut: BatchId::try_from(1)?,
            attempt: AttemptId::new(generation, 0),
        })
    }

    fn table_key(input: &CheckpointInput, ordinal: u32, store: StoreKind) -> TestResult<TableKey> {
        let fresh = FreshIdentity::new(
            input.source.generation().successor()?,
            input.attempt,
            ordinal,
        )?;
        Ok(TableKey::new(TableObjectId::new(fresh, store)))
    }

    fn successor(input: &CheckpointInput) -> TestResult<Seal> {
        Ok(Seal::new(
            input.source.partition(),
            input.source.generation().successor()?,
            WalReplayPoint::Through {
                batch: input.cut,
                owner: OwnerToken::from(input.attempt.owner_claim()),
            },
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?)
    }

    fn seal_key(generation: SealGeneration) -> TestResult<ObjectKey> {
        Ok(ObjectKey::try_from(SealKey::from(generation).to_string())?)
    }
}
