//! Typed Seal and WAL stores over the raw object-store adapter.

mod seal;
mod table;
mod wal;

use std::fmt;

use strom_object_store::{KeysBound, ObjectKey, PUT_BYTES_MAX, StoreError};
use strom_storage_domain::{SEAL_ENCODED_BYTES_MAX, WAL_ENCODED_BYTES_MAX};
pub(crate) use strom_storage_protocol::{SealPublication, TypedStoreError, WalEstablishment};

pub(crate) use seal::{GenesisEstablishment, SealStore};
pub(crate) use table::{TableEstablishment, TableStore, targeted_table_deletes};
pub(crate) use wal::WalStore;

fn typed_store_error(error: StoreError) -> TypedStoreError {
    match error {
        StoreError::Retryable { detail } => TypedStoreError::Retryable { detail },
        StoreError::Rejected { detail } => TypedStoreError::Rejected { detail },
        StoreError::Contradiction(contradiction) => TypedStoreError::Contradiction {
            detail: contradiction.to_string(),
        },
    }
}

fn typed_store_contradiction(detail: impl Into<String>) -> TypedStoreError {
    TypedStoreError::Contradiction {
        detail: detail.into(),
    }
}

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
