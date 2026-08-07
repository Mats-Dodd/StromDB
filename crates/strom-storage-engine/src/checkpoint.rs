//! Checkpoint publication and its bounded storage pipeline.

#![expect(
    clippy::disallowed_types,
    reason = "the single writer and checkpoint task share this one enumerated publication handshake"
)]

mod prepare;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::{StreamExt as _, stream};
use strom_object_store::{CreateEvidence, ObjectStoreAdapter};
use strom_storage_domain::{AttemptId, BatchId, Seal};
use tokio::sync::{Notify, Semaphore, mpsc, oneshot};

use crate::forest::Forest;
use crate::store::{
    CandidateTableEvidence, EncodedSeal, EncodedTable, SealStore, TableStore, TypedStoreError,
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
pub(crate) struct PreparedCheckpoint {
    source: Seal,
    successor: Seal,
    snapshot: Forest,
    encoded_seal: EncodedSeal,
}

impl PreparedCheckpoint {
    pub(crate) fn into_install(self) -> CheckpointInstall {
        CheckpointInstall {
            source: self.source,
            successor: self.successor,
            snapshot: self.snapshot,
        }
    }
}

pub(crate) struct CheckpointInstall {
    pub(super) source: Seal,
    pub(super) successor: Seal,
    pub(super) snapshot: Forest,
}

#[derive(Debug)]
pub(crate) enum CheckpointOutcome {
    Abandoned,
    Contradiction {
        cut: BatchId,
        detail: String,
    },
    Seal {
        prepared: Box<PreparedCheckpoint>,
        evidence: Result<CreateEvidence, TypedStoreError>,
    },
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
) -> CheckpointOutcome {
    let cut = input.cut;
    let permit = tokio::select! {
        biased;
        () = publication.claimed() => return CheckpointOutcome::Abandoned,
        permit = CHECKPOINT_PREPARATIONS.acquire() => match permit {
            Ok(permit) => permit,
            Err(_closed) => {
                return CheckpointOutcome::Contradiction {
                    cut,
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
        return CheckpointOutcome::Contradiction {
            cut,
            detail: format!("checkpoint preparation task failed: {join_error}"),
        };
    }
    match table_result {
        Ok(()) => {}
        Err(EstablishTableError::Abandon) => return CheckpointOutcome::Abandoned,
        Err(EstablishTableError::Contradiction { detail }) => {
            return CheckpointOutcome::Contradiction { cut, detail };
        }
    }
    let prepared = match prepared_receiver.await {
        Ok(Ok(Some(prepared))) => prepared,
        Ok(Ok(None)) => return CheckpointOutcome::Abandoned,
        Ok(Err(error)) => {
            return CheckpointOutcome::Contradiction {
                cut,
                detail: error.to_string(),
            };
        }
        Err(_sender_dropped) => {
            return CheckpointOutcome::Contradiction {
                cut,
                detail: "checkpoint preparation ended without a result".into(),
            };
        }
    };

    if !publication.begin_publish() {
        return CheckpointOutcome::Abandoned;
    }
    let seal_store = SealStore::new(adapter);
    let evidence = seal_store.create_seal(&prepared.encoded_seal).await;
    CheckpointOutcome::Seal { prepared, evidence }
}

#[derive(Debug, thiserror::Error)]
enum EstablishTableError {
    #[error("checkpoint table establishment was abandoned")]
    Abandon,
    #[error("checkpoint table establishment found a contradiction: {detail}")]
    Contradiction { detail: String },
}

async fn establish_tables(
    store: &TableStore,
    mut receiver: mpsc::Receiver<EncodedTable>,
    publication: PublicationGate,
) -> Result<(), EstablishTableError> {
    let tables = stream::poll_fn(move |context| receiver.poll_recv(context));
    let creates = tables
        .map(|table| {
            let store = store.clone();
            async move { establish_table(&store, &table).await }
        })
        .buffer_unordered(CHECKPOINT_CHILD_CREATES_MAX);
    futures::pin_mut!(creates);
    loop {
        tokio::select! {
            biased;
            () = publication.claimed() => return Err(EstablishTableError::Abandon),
            result = creates.next() => match result {
                Some(result) => result?,
                None => return Ok(()),
            }
        }
    }
}

async fn establish_table(
    store: &TableStore,
    candidate: &EncodedTable,
) -> Result<(), EstablishTableError> {
    match store.create_table(candidate).await {
        Ok(CreateEvidence::Direct | CreateEvidence::DurableMatch) => Ok(()),
        Ok(CreateEvidence::NotOurs) => Err(EstablishTableError::Contradiction {
            detail: "foreign bytes occupy a fresh checkpoint table identity".into(),
        }),
        Ok(CreateEvidence::Unresolved) => match store.reconcile_table(candidate).await {
            Ok(CandidateTableEvidence::Match) => Ok(()),
            Ok(CandidateTableEvidence::Foreign) => Err(EstablishTableError::Contradiction {
                detail: "an unresolved fresh checkpoint table contains foreign bytes".into(),
            }),
            Ok(CandidateTableEvidence::Absent)
            | Err(TypedStoreError::Retryable { .. } | TypedStoreError::Rejected { .. }) => {
                Err(EstablishTableError::Abandon)
            }
            Err(TypedStoreError::Contradiction { detail }) => {
                Err(EstablishTableError::Contradiction { detail })
            }
        },
        Err(TypedStoreError::Retryable { .. } | TypedStoreError::Rejected { .. }) => {
            Err(EstablishTableError::Abandon)
        }
        Err(TypedStoreError::Contradiction { detail }) => {
            Err(EstablishTableError::Contradiction { detail })
        }
    }
}
