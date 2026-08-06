//! Typed Seal and WAL stores over the raw object-store adapter.

mod seal;
mod table;
mod wal;

use std::fmt;

use strom_object_store::{KeysBound, ObjectKey, PUT_BYTES_MAX, StoreError};
use strom_storage_domain::{SEAL_ENCODED_BYTES_MAX, WAL_ENCODED_BYTES_MAX};

pub use seal::{EncodedSeal, SealStore, SealStoreError};
pub(crate) use table::{TableRows, TableStore, TableStoreError};
pub use wal::{
    AuthorizedWalRunDelete, EncodedWal, ObservedWal, WalDeleteRefusal, WalStore, WalStoreError,
};

// The engine joins the two crates; neither can see the other's bound constant
// (RFC 0002). These asserts keep the typed complete-object bounds inside the
// adapter's put/get ceiling.
#[expect(
    clippy::as_conversions,
    reason = "const comparison across usize (storage-domain) and u64 (adapter) bounds"
)]
const _: () = assert!(
    (SEAL_ENCODED_BYTES_MAX as u64) <= PUT_BYTES_MAX,
    "SEAL_ENCODED_BYTES_MAX must fit inside PUT_BYTES_MAX"
);
const _: () = assert!(
    strom_storage_domain::SST_OBJECT_BYTES_MAX <= PUT_BYTES_MAX,
    "SST_OBJECT_BYTES_MAX must fit inside PUT_BYTES_MAX"
);
#[expect(
    clippy::as_conversions,
    reason = "const comparison across usize (storage-domain) and u64 (adapter) bounds"
)]
const _: () = assert!(
    (WAL_ENCODED_BYTES_MAX as u64) <= PUT_BYTES_MAX,
    "WAL_ENCODED_BYTES_MAX must fit inside PUT_BYTES_MAX"
);

fn object_key(spelling: impl fmt::Display) -> ObjectKey {
    ObjectKey::try_from(spelling.to_string())
        .expect("canonical storage-domain spelling is always a valid ObjectKey")
}

fn newest_keys_bound() -> KeysBound {
    KeysBound::try_from(1).expect("one is a legal keys bound")
}

fn map_store_error(error: StoreError) -> (StoreErrorClass, String) {
    match error {
        StoreError::Retryable { detail } => (StoreErrorClass::Retryable, detail),
        StoreError::Rejected { detail } => (StoreErrorClass::Rejected, detail),
        StoreError::Contradiction(contradiction) => {
            (StoreErrorClass::Contradiction, contradiction.to_string())
        }
    }
}

enum StoreErrorClass {
    Retryable,
    Rejected,
    Contradiction,
}
