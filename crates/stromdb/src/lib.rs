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
pub use strom_domain::{
    CloseStreamOutcome, CreateOutcome, ExpiryPolicy, StreamContentType, StreamId, StreamIdError,
    StreamLifecycle, StreamStatus,
};
pub use strom_storage_engine::{CloseOutcome, OpenError, PartitionId, StreamError};

use std::sync::Arc;

use object_store::ObjectStore;
use strom_common::{Entropy, Seed};
use strom_storage_engine::Engine;

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
        let entropy = Entropy::from_seed(Seed::from_os());
        Engine::open(store, entropy)
            .await
            .map(|engine| Self { engine })
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
        self.engine
            .create_stream(id, content_type, expiry, lifecycle)
            .await
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
        self.engine.close_stream(id).await
    }

    /// Delete the stream at `id`; the path stays permanently occupied.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError`] when the command is refused, shed, or left
    /// without a determinate durable outcome.
    pub async fn delete_stream(&self, id: &StreamId) -> Result<(), StreamError> {
        self.engine.delete_stream(id).await
    }

    /// Report the current status of the stream at `id`.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError::Unavailable`] after this partition has stopped
    /// serving.
    pub fn stream(&self, id: &StreamId) -> Result<StreamStatus, StreamError> {
        self.engine.stream(id)
    }

    /// Close ingress, drain every accepted command, and stop serving.
    ///
    /// # Panics
    ///
    /// Panics when the writer task was externally cancelled or violated an
    /// in-process invariant.
    pub async fn close(self) -> CloseOutcome {
        self.engine.shutdown().await
    }
}

/// Returns the `stromdb` crate version.
#[must_use]
pub const fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
