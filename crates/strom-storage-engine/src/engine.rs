//! Engine startup, immutable views, bounded commands, and graceful drain.

use std::sync::Arc;

use object_store::ObjectStore;
use strom_common::Entropy;
use strom_domain::{
    CloseStreamOutcome, CreateOutcome, ExpiryPolicy, StreamContentType, StreamId, StreamLifecycle,
    StreamStatus,
};
use strom_object_store::ObjectStoreAdapter;
use strom_storage_domain::{
    DirectoryEntry, DirectoryKey, PartitionId, WRITER_INGRESS_COMMANDS_MAX,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::Forest;
use crate::admission::{AdmissionRefusal, CreateStream};
use crate::bootstrap::{BootstrapExit, bootstrap};
use crate::writer::{CommandEnvelope, WriterExit, spawn_writer};

#[derive(Debug, Clone)]
pub(crate) struct PublishedView {
    forest: Forest,
}

impl PublishedView {
    pub(crate) const fn new(forest: Forest) -> Self {
        Self { forest }
    }

    fn stream(&self, id: &StreamId) -> StreamStatus {
        match self.forest.resolve(&DirectoryKey::from(id)) {
            None => StreamStatus::Missing,
            Some(DirectoryEntry::Tombstone(_uid)) => StreamStatus::Deleted,
            Some(DirectoryEntry::Live(uid)) => {
                let record = self
                    .forest
                    .record(uid)
                    .expect("a live directory entry has a ledger record");
                StreamStatus::Live {
                    content_type: record.content_type().clone(),
                    expiry: record.expiry(),
                    lifecycle: record.lifecycle(),
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Engine {
    partition: PartitionId,
    commands: mpsc::Sender<CommandEnvelope>,
    view: watch::Receiver<PublishedView>,
    writer: JoinHandle<WriterExit>,
}

impl Engine {
    /// Bootstrap one partition and start its sole writer.
    ///
    /// # Errors
    ///
    /// Returns [`OpenError`] unless the complete durable state is bounded,
    /// consistent, directly claimed, fenced, replayed, and current.
    pub async fn open(store: Arc<dyn ObjectStore>, entropy: Entropy) -> Result<Self, OpenError> {
        let adapter = ObjectStoreAdapter::new(store);
        let ready = bootstrap(adapter.clone(), entropy)
            .await
            .map_err(open_error)?;
        let partition = ready.partition();
        let initial = PublishedView::new(ready.forest().clone());
        let (view_sender, view) = watch::channel(initial);
        let (commands, ingress) = mpsc::channel(WRITER_INGRESS_COMMANDS_MAX);
        let writer = spawn_writer(adapter, ready, ingress, view_sender);
        Ok(Self {
            partition,
            commands,
            view,
            writer,
        })
    }

    #[must_use]
    pub const fn partition_id(&self) -> PartitionId {
        self.partition
    }

    /// Create one stream or confirm its durable configuration.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError`] when the command is refused, shed, unavailable,
    /// or left without a determinate durable outcome.
    pub async fn create_stream(
        &self,
        id: &StreamId,
        content_type: StreamContentType,
        expiry: ExpiryPolicy,
        lifecycle: StreamLifecycle,
    ) -> Result<CreateOutcome, StreamError> {
        let (reply, outcome) = oneshot::channel();
        let command = CreateStream {
            path: DirectoryKey::from(id),
            content_type,
            expiry,
            lifecycle,
        };
        self.enqueue(CommandEnvelope::Create { command, reply })?;
        match outcome.await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(refusal)) => Err(stream_error(refusal)),
            Err(_writer_dropped_waiter) => Err(StreamError::Indeterminate),
        }
    }

    /// Close one live stream or confirm that it is already closed.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError`] when the command is refused, shed, unavailable,
    /// or left without a determinate durable outcome.
    pub async fn close_stream(&self, id: &StreamId) -> Result<CloseStreamOutcome, StreamError> {
        let (reply, outcome) = oneshot::channel();
        self.enqueue(CommandEnvelope::Close {
            path: DirectoryKey::from(id),
            reply,
        })?;
        match outcome.await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(refusal)) => Err(stream_error(refusal)),
            Err(_writer_dropped_waiter) => Err(StreamError::Indeterminate),
        }
    }

    /// Delete one live stream.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError`] when the command is refused, shed, unavailable,
    /// or left without a determinate durable outcome.
    pub async fn delete_stream(&self, id: &StreamId) -> Result<(), StreamError> {
        let (reply, outcome) = oneshot::channel();
        self.enqueue(CommandEnvelope::Delete {
            path: DirectoryKey::from(id),
            reply,
        })?;
        match outcome.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(refusal)) => Err(stream_error(refusal)),
            Err(_writer_dropped_waiter) => Err(StreamError::Indeterminate),
        }
    }

    /// Report the current protocol-visible state of one stream.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError::Unavailable`] after the writer revokes readiness.
    pub fn stream(&self, id: &StreamId) -> Result<StreamStatus, StreamError> {
        if self.commands.is_closed() {
            return Err(StreamError::Unavailable);
        }
        Ok(self.view.borrow().stream(id))
    }

    fn enqueue(&self, command: CommandEnvelope) -> Result<(), StreamError> {
        match self.commands.try_send(command) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_envelope)) => Err(StreamError::Overloaded),
            Err(mpsc::error::TrySendError::Closed(_envelope)) => Err(StreamError::Unavailable),
        }
    }

    /// Close ingress, drain every accepted command, and join the writer.
    ///
    /// # Panics
    ///
    /// Panics when the writer task was externally cancelled or violated an
    /// in-process invariant.
    pub async fn shutdown(self) -> CloseOutcome {
        let Self {
            partition: _,
            commands,
            view,
            writer,
        } = self;
        drop(commands);
        let exit = writer
            .await
            .expect("the partition writer exits without cancellation or panic");
        drop(view);
        close_outcome(exit)
    }
}

/// Why one partition could not open.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OpenError {
    #[error("open should be retried: {detail}")]
    Retryable { detail: String },
    #[error("another writer took the partition")]
    Fenced,
    #[error("durable state contradicts the storage model: {detail}")]
    Contradiction { detail: String },
}

/// Why one stream operation did not take effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StreamError {
    #[error("stream path is already occupied")]
    Occupied,
    #[error("partition stream capacity is exhausted")]
    CapacityExhausted,
    #[error("stream path is not live")]
    NotLive,
    #[error("partition is at a bounded capacity limit; retry later")]
    Overloaded,
    #[error("partition is no longer serving")]
    Unavailable,
    #[error("operation outcome is indeterminate; reopen and inspect")]
    Indeterminate,
}

/// How one partition stopped serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseOutcome {
    /// Every accepted command drained and the partition stopped cleanly.
    Shutdown,
    /// Another writer took the partition.
    Fenced,
    /// An operation may have taken effect without local evidence.
    Poisoned { detail: String },
    /// Durable state contradicts the storage model.
    Contradiction { detail: String },
}

fn open_error(exit: BootstrapExit) -> OpenError {
    match exit {
        BootstrapExit::Retryable { detail } => OpenError::Retryable { detail },
        BootstrapExit::Fenced { observed: _ } => OpenError::Fenced,
        BootstrapExit::Contradiction { detail } => OpenError::Contradiction { detail },
    }
}

const fn stream_error(refusal: AdmissionRefusal) -> StreamError {
    match refusal {
        AdmissionRefusal::PathOccupied => StreamError::Occupied,
        AdmissionRefusal::PathCapacityExhausted => StreamError::CapacityExhausted,
        AdmissionRefusal::PathNotLive => StreamError::NotLive,
        AdmissionRefusal::Overloaded => StreamError::Overloaded,
    }
}

fn close_outcome(exit: WriterExit) -> CloseOutcome {
    match exit {
        WriterExit::Shutdown => CloseOutcome::Shutdown,
        WriterExit::Fenced { batch: _ } => CloseOutcome::Fenced,
        WriterExit::Poisoned { batch: _, detail } => CloseOutcome::Poisoned { detail },
        WriterExit::Contradiction { batch: _, detail } => CloseOutcome::Contradiction { detail },
    }
}
