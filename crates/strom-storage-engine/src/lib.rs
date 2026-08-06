//! The `StromDB` storage engine: writer, bootstrap, admission, forest, and
//! typed stores. The `stromdb` crate is the public embeddable interface.

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
