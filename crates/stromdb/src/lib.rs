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
//! use stromdb::{
//!     CreateOutcome, Db, ExpiryPolicy, StreamContentType, StreamLifecycle, StreamStatus,
//! };
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let store = Arc::new(stromdb::object_store::memory::InMemory::new());
//! let db = Db::open(store).await?;
//!
//! let id: stromdb::StreamId = "events/a".parse()?;
//! assert_eq!(
//!     CreateOutcome::Created,
//!     db.create_stream(
//!         &id,
//!         StreamContentType::octet_stream(),
//!         ExpiryPolicy::None,
//!         StreamLifecycle::Open,
//!     )
//!     .await?
//! );
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
use strom_common::{Entropy, Seed};
use strom_object_store::ObjectStoreAdapter;
use strom_storage_domain::{DirectoryEntry, DirectoryKey};
use strom_storage_engine::{
    AdmissionRefusal, BootstrapExit, CommandError, Engine, PublishedView, StreamCommand,
    StreamReply, WriterExit,
};

/// One open `StromDB` durable streams database.
#[derive(Debug)]
pub struct Db {
    engine: Engine,
}

impl Db {
    /// Open a database over `store` and take sole writer authority.
    ///
    /// The injected store must not transparently resend a create after an
    /// ambiguous result; `StromDB` owns every retry decision, so configure
    /// transport retries off.
    ///
    /// # Errors
    ///
    /// Returns [`OpenError`] unless the complete durable state is bounded,
    /// internally consistent, directly claimed, fenced, replayed, and current.
    pub async fn open(store: Arc<dyn ObjectStore>) -> Result<Self, OpenError> {
        let adapter = ObjectStoreAdapter::new(store);
        let entropy = Entropy::from_seed(Seed::from_os());
        match Engine::open(adapter, entropy).await {
            Ok(engine) => Ok(Self { engine }),
            Err(exit) => Err(open_error(exit)),
        }
    }

    /// The genesis-born identity of the partition at this store root.
    #[must_use]
    pub const fn partition_id(&self) -> PartitionId {
        self.engine.partition_id()
    }

    /// Create one stream at `id`, or confirm it already exists at the same configuration.
    ///
    /// A same-configuration retry returns [`CreateOutcome::AlreadyExists`]
    /// (protocol §5.1). A path that is occupied with a different content type,
    /// expiry, or lifecycle is refused as [`StreamError::Occupied`].
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
        lifecycle: StreamLifecycle,
    ) -> Result<CreateOutcome, StreamError> {
        let reply = self
            .send(StreamCommand::Create {
                path: DirectoryKey::from(id),
                content_type,
                expiry,
                lifecycle,
            })
            .await?;
        Ok(create_outcome(reply))
    }

    /// Close the live stream at `id` to further appends.
    ///
    /// A close on an already-closed stream returns
    /// [`CloseStreamOutcome::AlreadyClosed`] (protocol §5.1).
    ///
    /// # Errors
    ///
    /// Returns [`StreamError`] when the command is refused, shed, or left
    /// without a determinate durable outcome.
    pub async fn close_stream(&self, id: &StreamId) -> Result<CloseStreamOutcome, StreamError> {
        let reply = self
            .send(StreamCommand::Close {
                path: DirectoryKey::from(id),
            })
            .await?;
        Ok(close_outcome_for_reply(reply))
    }

    /// Delete the stream at `id`; the path stays permanently occupied.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError`] when the command is refused, shed, or left
    /// without a determinate durable outcome.
    pub async fn delete_stream(&self, id: &StreamId) -> Result<(), StreamError> {
        let reply = self
            .send(StreamCommand::Delete {
                path: DirectoryKey::from(id),
            })
            .await?;
        delete_outcome(reply);
        Ok(())
    }

    /// Report the current status of the stream at `id`.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError::Unavailable`] after this partition has stopped
    /// serving.
    pub fn stream(&self, id: &StreamId) -> Result<StreamStatus, StreamError> {
        match self.engine.snapshot() {
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
        close_outcome(self.engine.shutdown().await)
    }

    async fn send(&self, command: StreamCommand) -> Result<StreamReply, StreamError> {
        match self.engine.command(command).await {
            Ok(reply) => Ok(reply),
            Err(error) => Err(stream_error(error)),
        }
    }
}

#[expect(
    clippy::panic,
    reason = "command/reply pairing is an in-process writer invariant"
)]
const fn create_outcome(reply: StreamReply) -> CreateOutcome {
    match reply {
        StreamReply::Created { uid: _ } => CreateOutcome::Created,
        StreamReply::AlreadyCreated { uid: _ } => CreateOutcome::AlreadyExists,
        StreamReply::Closed | StreamReply::AlreadyClosed | StreamReply::Deleted => {
            panic!("create admits only Created or AlreadyCreated")
        }
    }
}

#[expect(
    clippy::panic,
    reason = "command/reply pairing is an in-process writer invariant"
)]
const fn close_outcome_for_reply(reply: StreamReply) -> CloseStreamOutcome {
    match reply {
        StreamReply::Closed => CloseStreamOutcome::Closed,
        StreamReply::AlreadyClosed => CloseStreamOutcome::AlreadyClosed,
        StreamReply::Created { uid: _ }
        | StreamReply::AlreadyCreated { uid: _ }
        | StreamReply::Deleted => {
            panic!("close admits only Closed or AlreadyClosed")
        }
    }
}

#[expect(
    clippy::panic,
    reason = "command/reply pairing is an in-process writer invariant"
)]
const fn delete_outcome(reply: StreamReply) {
    match reply {
        StreamReply::Deleted => {}
        StreamReply::Created { uid: _ }
        | StreamReply::AlreadyCreated { uid: _ }
        | StreamReply::Closed
        | StreamReply::AlreadyClosed => {
            panic!("delete admits only Deleted")
        }
    }
}

/// Whether create found the stream already durable at the same configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateOutcome {
    Created,
    AlreadyExists,
}

/// Whether close found the stream already closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseStreamOutcome {
    Closed,
    AlreadyClosed,
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
