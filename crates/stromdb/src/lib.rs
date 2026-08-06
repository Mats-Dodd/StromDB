//! The `StromDB` database engine.

mod store;

pub use store::{
    AuthorizedWalRunDelete, EncodedSeal, EncodedWal, ObservedWal, SealStore, SealStoreError,
    WalDeleteRefusal, WalStore, WalStoreError,
};
pub use strom_object_store::{CreateEvidence, Etag, ObjectStoreAdapter};
pub use strom_storage_domain::EncodeError;

/// Returns the `StromDB` crate version.
#[must_use]
pub const fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
