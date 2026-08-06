//! `StromDB`, the embeddable durable streams database.
//!
//! [`Db`] is the public interface. Open one partition over any injected
//! [`object_store`] backend, mutate streams through verb methods, read stream
//! status by path, and close gracefully. The interface speaks only the
//! Durable Streams protocol vocabulary; every storage spelling stays inside
//! `strom-storage-engine`.
//!
//! ```
//! use std::sync::Arc;
//!
//! use stromdb::{Db, ExpiryPolicy, StreamContentType, StreamStatus};
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(stromdb::object_store::memory::InMemory::new());
//! let partition: stromdb::PartitionId = "00112233-4455-6677-8899-aabbccddeeff".parse()?;
//! let db = Db::open(store, partition).await?;
//!
//! let id: stromdb::StreamId = "events/a".parse()?;
//! db.create_stream(&id, StreamContentType::octet_stream(), ExpiryPolicy::None)
//!     .await?;
//! assert!(matches!(db.stream(&id)?, StreamStatus::Live { .. }));
//!
//! db.close().await;
//! # Ok(())
//! # }
//! ```

/// Re-export of the `object_store` crate behind [`Db::open`].
///
/// Use these types instead of a separate `object_store` dependency, so the
/// trait object you inject and the one `stromdb` expects are the same type.
pub use object_store;
pub use strom_domain::{ExpiryPolicy, StreamContentType, StreamId, StreamIdError, StreamLifecycle};
pub use strom_storage_domain::PartitionId;

use std::sync::Arc;

use object_store::ObjectStore;
use strom_object_store::ObjectStoreAdapter;
use strom_storage_domain::{DirectoryEntry, DirectoryKey};
use strom_storage_engine::{
    AdmissionRefusal, BootstrapExit, CommandError, Partition, PartitionHandle, PublishedView,
    StreamCommand, StreamReply, WriterExit,
};

/// One open partition of a `StromDB` durable streams database.
#[derive(Debug)]
pub struct Db {
    handle: PartitionHandle,
}

impl Db {
    /// Bootstrap one partition over `store` and take sole writer authority.
    ///
    /// The injected store must not transparently resend a create after an
    /// ambiguous result; `StromDB` owns every retry decision, so configure
    /// transport retries off.
    ///
    /// # Errors
    ///
    /// Returns [`OpenError`] unless the complete durable state is bounded,
    /// internally consistent, directly claimed, fenced, replayed, and current.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        partition: PartitionId,
    ) -> Result<Self, OpenError> {
        let adapter = ObjectStoreAdapter::new(store);
        match Partition::start(adapter, partition).await {
            Ok(handle) => Ok(Self { handle }),
            Err(exit) => Err(open_error(exit)),
        }
    }

    /// Create one stream at `id`.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError`] when the command is refused, shed, or left
    /// without a determinate durable outcome.
    pub async fn create_stream(
        &self,
        id: &StreamId,
        content_type: StreamContentType,
        expiry: ExpiryPolicy,
    ) -> Result<(), StreamError> {
        self.command(StreamCommand::Create {
            path: DirectoryKey::from(id),
            content_type,
            expiry,
        })
        .await
    }

    /// Close the live stream at `id` to further appends.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError`] when the command is refused, shed, or left
    /// without a determinate durable outcome.
    pub async fn close_stream(&self, id: &StreamId) -> Result<(), StreamError> {
        self.command(StreamCommand::Close {
            path: DirectoryKey::from(id),
        })
        .await
    }

    /// Delete the stream at `id`; the path stays permanently occupied.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError`] when the command is refused, shed, or left
    /// without a determinate durable outcome.
    pub async fn delete_stream(&self, id: &StreamId) -> Result<(), StreamError> {
        self.command(StreamCommand::Delete {
            path: DirectoryKey::from(id),
        })
        .await
    }

    /// Report the current status of the stream at `id`.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError::Unavailable`] after this partition has stopped
    /// serving.
    pub fn stream(&self, id: &StreamId) -> Result<StreamStatus, StreamError> {
        match self.handle.snapshot() {
            Ok(view) => Ok(stream_status(&view, &DirectoryKey::from(id))),
            Err(error) => Err(stream_error(error)),
        }
    }

    /// Close ingress, drain every accepted command, and stop serving.
    ///
    /// # Panics
    ///
    /// Panics when the writer task was externally cancelled or violated an
    /// in-process invariant.
    pub async fn close(self) -> CloseOutcome {
        close_outcome(self.handle.shutdown().await)
    }

    async fn command(&self, command: StreamCommand) -> Result<(), StreamError> {
        match self.handle.command(command).await {
            Ok(StreamReply::Created { uid: _ } | StreamReply::Closed | StreamReply::Deleted) => {
                Ok(())
            }
            Err(error) => Err(stream_error(error)),
        }
    }
}

/// The current protocol-visible state of one stream path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamStatus {
    /// No stream has ever occupied this path (protocol `404`).
    Missing,
    /// The stream was deleted; the path stays occupied (protocol `410`).
    Deleted,
    /// The stream exists and is directly readable.
    Live {
        content_type: StreamContentType,
        expiry: ExpiryPolicy,
        lifecycle: StreamLifecycle,
    },
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
    #[error("stream is already closed")]
    AlreadyClosed,
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

/// Returns the `stromdb` crate version.
#[must_use]
pub const fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

fn stream_status(view: &PublishedView, path: &DirectoryKey) -> StreamStatus {
    match view.resolve(path) {
        None => StreamStatus::Missing,
        Some(DirectoryEntry::Tombstone(_uid)) => StreamStatus::Deleted,
        Some(DirectoryEntry::Live(uid)) => {
            let record = view
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

fn open_error(exit: BootstrapExit) -> OpenError {
    match exit {
        BootstrapExit::Retryable { detail } => OpenError::Retryable { detail },
        BootstrapExit::Fenced { observed: _ } => OpenError::Fenced,
        BootstrapExit::Contradiction { detail } => OpenError::Contradiction { detail },
    }
}

const fn stream_error(error: CommandError) -> StreamError {
    match error {
        CommandError::Refused(refusal) => match refusal {
            AdmissionRefusal::PathOccupied => StreamError::Occupied,
            AdmissionRefusal::PathCapacityExhausted => StreamError::CapacityExhausted,
            AdmissionRefusal::PathNotLive => StreamError::NotLive,
            AdmissionRefusal::StreamAlreadyClosed => StreamError::AlreadyClosed,
            AdmissionRefusal::Overloaded => StreamError::Overloaded,
        },
        CommandError::Unavailable => StreamError::Unavailable,
        CommandError::Indeterminate => StreamError::Indeterminate,
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
