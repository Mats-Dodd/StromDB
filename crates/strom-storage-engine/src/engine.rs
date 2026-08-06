//! Engine startup, immutable views, bounded commands, and graceful drain.

use strom_common::Entropy;
use strom_object_store::ObjectStoreAdapter;
use strom_storage_domain::{DirectoryEntry, DirectoryKey, PartitionId, StreamRecord, StreamUid};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::bootstrap::bootstrap;
use crate::writer::{CommandEnvelope, WRITER_INGRESS_COMMANDS_MAX, spawn_writer};
use crate::{AdmissionRefusal, BootstrapExit, Forest, StreamCommand, StreamReply, WriterExit};

#[derive(Debug, Clone)]
pub struct PublishedView {
    forest: Forest,
}

impl PublishedView {
    pub(crate) const fn new(forest: Forest) -> Self {
        Self { forest }
    }

    #[must_use]
    pub fn resolve(&self, path: &DirectoryKey) -> Option<DirectoryEntry> {
        self.forest.resolve(path)
    }

    #[must_use]
    pub fn record(&self, uid: StreamUid) -> Option<&StreamRecord> {
        self.forest.record(uid)
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
    /// Returns [`BootstrapExit`] unless the complete durable state is bounded,
    /// internally consistent, directly claimed, fenced, replayed, and current.
    pub async fn open(
        adapter: ObjectStoreAdapter,
        entropy: Entropy,
    ) -> Result<Self, BootstrapExit> {
        let ready = bootstrap(adapter.clone(), entropy).await?;
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

    /// Acquire an immutable view while this partition remains Ready.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Unavailable`] after the writer has revoked
    /// readiness by exiting.
    pub fn snapshot(&self) -> Result<PublishedView, CommandError> {
        if self.commands.is_closed() {
            return Err(CommandError::Unavailable);
        }
        Ok(self.view.borrow().clone())
    }

    /// Submit one mutation without waiting for queue capacity.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Refused`] when admission sheds or rejects the
    /// command, [`CommandError::Unavailable`] after shutdown, and
    /// [`CommandError::Indeterminate`] when a writer exits with an unresolved
    /// durable outcome.
    pub async fn command(&self, command: StreamCommand) -> Result<StreamReply, CommandError> {
        let (reply, outcome) = oneshot::channel();
        match self.commands.try_send(CommandEnvelope { command, reply }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_envelope)) => {
                return Err(CommandError::Refused(AdmissionRefusal::Overloaded));
            }
            Err(mpsc::error::TrySendError::Closed(_envelope)) => {
                return Err(CommandError::Unavailable);
            }
        }
        match outcome.await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(refusal)) => Err(CommandError::Refused(refusal)),
            Err(_writer_dropped_waiter) => Err(CommandError::Indeterminate),
        }
    }

    /// Close ingress, drain every accepted command, and join the writer.
    ///
    /// # Panics
    ///
    /// Panics when the writer task was externally cancelled or violated an
    /// in-process invariant.
    pub async fn shutdown(self) -> WriterExit {
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
        exit
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CommandError {
    #[error("command was refused: {0}")]
    Refused(#[source] AdmissionRefusal),
    #[error("partition writer is unavailable")]
    Unavailable,
    #[error("command outcome is indeterminate; recover from durable evidence")]
    Indeterminate,
}
