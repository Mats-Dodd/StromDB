//! The `StromDB` database engine.

mod admission;
mod bootstrap;
mod checkpoint;
mod forest;
mod partition;
mod store;
mod writer;

pub use admission::{AdmissionRefusal, StreamCommand, StreamReply};
pub use bootstrap::BootstrapExit;
pub use forest::{Applied, FoldContradiction, Forest};
pub use partition::{CommandError, Partition, PartitionHandle, PublishedView};
pub use store::{
    AuthorizedWalRunDelete, EncodedSeal, EncodedWal, ObservedWal, SealStore, SealStoreError,
    WalDeleteRefusal, WalStore, WalStoreError,
};
pub use strom_object_store::{CreateEvidence, Etag, ObjectStoreAdapter};
pub use strom_storage_domain::EncodeError;
pub use writer::WriterExit;

/// Returns the `StromDB` crate version.
#[must_use]
pub const fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
