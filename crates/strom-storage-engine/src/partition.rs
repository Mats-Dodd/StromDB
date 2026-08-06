//! Partition startup, immutable views, bounded commands, and graceful drain.

use strom_common::Entropy;
use strom_object_store::ObjectStoreAdapter;
use strom_storage_domain::{
    DirectoryEntry, DirectoryKey, PartitionId, SealGeneration, StreamRecord, StreamUid,
    WalReplayPoint,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::bootstrap::bootstrap;
use crate::writer::{CommandEnvelope, WRITER_INGRESS_COMMANDS_MAX, spawn_writer};
use crate::{AdmissionRefusal, BootstrapExit, Forest, StreamCommand, StreamReply, WriterExit};

#[derive(Debug, Clone)]
pub struct PublishedView {
    generation: SealGeneration,
    replay: WalReplayPoint,
    forest: Forest,
}

impl PublishedView {
    pub(crate) const fn new(
        generation: SealGeneration,
        replay: WalReplayPoint,
        forest: Forest,
    ) -> Self {
        Self {
            generation,
            replay,
            forest,
        }
    }

    #[must_use]
    pub const fn generation(&self) -> SealGeneration {
        self.generation
    }

    #[must_use]
    pub const fn replay(&self) -> WalReplayPoint {
        self.replay
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

#[derive(Debug, Clone, Copy)]
pub struct Partition;

impl Partition {
    /// Bootstrap one partition and start its sole writer.
    ///
    /// # Errors
    ///
    /// Returns [`BootstrapExit`] unless the complete durable state is bounded,
    /// internally consistent, directly claimed, fenced, replayed, and current.
    pub async fn start(
        adapter: ObjectStoreAdapter,
        entropy: Entropy,
    ) -> Result<PartitionHandle, BootstrapExit> {
        let ready = bootstrap(adapter.clone(), entropy).await?;
        let partition = ready.partition();
        let initial = PublishedView::new(
            ready.claim().generation(),
            ready.replay(),
            ready.forest().clone(),
        );
        let (view_sender, view) = watch::channel(initial);
        let (commands, ingress) = mpsc::channel(WRITER_INGRESS_COMMANDS_MAX);
        let writer = spawn_writer(adapter, ready, ingress, view_sender);
        Ok(PartitionHandle {
            partition,
            commands,
            view,
            writer,
        })
    }
}

#[derive(Debug)]
pub struct PartitionHandle {
    partition: PartitionId,
    commands: mpsc::Sender<CommandEnvelope>,
    view: watch::Receiver<PublishedView>,
    writer: JoinHandle<WriterExit>,
}

impl PartitionHandle {
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

#[cfg(test)]
mod tests {
    use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};
    use strom_storage_domain::{BatchId, OwnerToken, WalBody, WalObject};

    use super::*;
    use crate::{CreateEvidence, EncodedWal, WalStore};

    #[tokio::test]
    async fn success_is_visible_before_reply_and_survives_reopen()
    -> Result<(), Box<dyn std::error::Error>> {
        let adapter = ObjectStoreAdapter::in_memory();
        let path = directory_key("events/a")?;
        let handle = Partition::start(adapter.clone(), crate::test_entropy()).await?;
        let reply = handle
            .command(StreamCommand::Create {
                path: path.clone(),
                content_type: StreamContentType::octet_stream(),
                expiry: ExpiryPolicy::None,
                lifecycle: StreamLifecycle::Open,
            })
            .await?;
        let StreamReply::Created { uid } = reply else {
            return Err("create returned the wrong reply variant".into());
        };
        assert_eq!(
            Some(DirectoryEntry::Live(uid)),
            handle.snapshot()?.resolve(&path),
            "a success reply is released only after its immutable view is installed"
        );
        assert_eq!(WriterExit::Shutdown, handle.shutdown().await);

        let reopened = Partition::start(adapter, crate::test_entropy()).await?;
        assert_eq!(
            Some(DirectoryEntry::Live(uid)),
            reopened.snapshot()?.resolve(&path),
            "bootstrap replay reconstructs an acknowledged write"
        );
        assert_eq!(WriterExit::Shutdown, reopened.shutdown().await);
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_create_is_idempotent_and_consumes_no_second_wal_coordinate()
    -> Result<(), Box<dyn std::error::Error>> {
        let adapter = ObjectStoreAdapter::in_memory();
        let path = directory_key("events/dup")?;
        let handle = Partition::start(adapter.clone(), crate::test_entropy()).await?;
        let partition_id = handle.partition_id();
        let first = handle
            .command(StreamCommand::Create {
                path: path.clone(),
                content_type: StreamContentType::octet_stream(),
                expiry: ExpiryPolicy::None,
                lifecycle: StreamLifecycle::Open,
            })
            .await?;
        let StreamReply::Created { uid } = first else {
            return Err("first create must return Created".into());
        };
        let after_create = handle.snapshot()?;
        assert_eq!(
            Ok(StreamReply::AlreadyCreated { uid }),
            handle
                .command(StreamCommand::Create {
                    path: path.clone(),
                    content_type: StreamContentType::octet_stream(),
                    expiry: ExpiryPolicy::None,
                    lifecycle: StreamLifecycle::Open,
                })
                .await
        );
        assert_eq!(
            after_create.resolve(&path),
            handle.snapshot()?.resolve(&path),
            "idempotent create does not change the published view"
        );
        let wal = WalStore::new(adapter);
        let first_run = BatchId::try_from(2)?;
        assert!(
            wal.read_wal(partition_id, first_run).await?.is_some(),
            "the original create occupies the first RUN coordinate"
        );
        assert!(
            wal.read_wal(partition_id, first_run.successor()?)
                .await?
                .is_none(),
            "an idempotent create consumes no second WAL coordinate"
        );
        assert_eq!(WriterExit::Shutdown, handle.shutdown().await);
        Ok(())
    }

    #[tokio::test]
    async fn typed_refusal_does_not_change_the_published_view()
    -> Result<(), Box<dyn std::error::Error>> {
        let handle =
            Partition::start(ObjectStoreAdapter::in_memory(), crate::test_entropy()).await?;
        let missing = directory_key("events/missing")?;
        assert_eq!(
            Err(CommandError::Refused(AdmissionRefusal::PathNotLive)),
            handle
                .command(StreamCommand::Delete {
                    path: missing.clone()
                })
                .await
        );
        assert_eq!(None, handle.snapshot()?.resolve(&missing));
        assert_eq!(WriterExit::Shutdown, handle.shutdown().await);
        Ok(())
    }

    #[tokio::test]
    async fn writer_exit_revokes_new_snapshot_acquisition() -> Result<(), Box<dyn std::error::Error>>
    {
        let adapter = ObjectStoreAdapter::in_memory();
        let handle = Partition::start(adapter.clone(), crate::test_entropy()).await?;
        let partition_id = handle.partition_id();
        let seal = handle.snapshot()?.generation();
        let batch = BatchId::try_from(2)?;
        let foreign = EncodedWal::new(&WalObject::new(
            partition_id,
            batch,
            OwnerToken::from(seal.successor()?),
            WalBody::Fence,
        ))?;
        assert_eq!(
            CreateEvidence::Direct,
            WalStore::new(adapter).create_wal(&foreign).await?
        );

        let path = directory_key("events/fenced")?;
        assert_eq!(
            Err(CommandError::Indeterminate),
            handle
                .command(StreamCommand::Create {
                    path,
                    content_type: StreamContentType::octet_stream(),
                    expiry: ExpiryPolicy::None,
                    lifecycle: StreamLifecycle::Open,
                })
                .await
        );
        assert!(matches!(handle.snapshot(), Err(CommandError::Unavailable)));
        assert_eq!(WriterExit::Fenced { batch }, handle.shutdown().await);
        Ok(())
    }

    fn directory_key(raw: &str) -> Result<DirectoryKey, Box<dyn std::error::Error>> {
        Ok(DirectoryKey::try_from(Box::<[u8]>::from(raw.as_bytes()))?)
    }
}
