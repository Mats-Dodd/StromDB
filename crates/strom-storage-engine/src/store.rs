//! Typed Seal and WAL stores over the raw object-store adapter.

mod seal;
mod table;
mod wal;

use std::fmt;

use strom_object_store::{KeysBound, ObjectKey, PUT_BYTES_MAX, StoreError};
use strom_storage_domain::{SEAL_ENCODED_BYTES_MAX, WAL_ENCODED_BYTES_MAX};

pub(crate) use seal::{
    EncodedAuthoritySeal, EncodedGenesisSeal, GenesisEstablishment, SealPublication, SealStore,
};
pub(crate) use table::{
    EncodedTable, TableEstablishment, TableRows, TableStore, targeted_table_deletes,
};
pub(crate) use wal::{EncodedWal, WalEstablishment, WalStore};

/// Failures of typed store operations, shaped for writer and bootstrap exits.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum TypedStoreError {
    /// Transport trouble; a bounded retry of the same idempotent request is legal.
    #[error("retryable store failure: {detail}")]
    Retryable { detail: String },
    /// The backend refused the request definitively; retrying cannot help.
    #[error("store rejected the request: {detail}")]
    Rejected { detail: String },
    /// Durable bytes violate the storage model. The caller fails closed.
    #[error("store durable contradiction: {detail}")]
    Contradiction { detail: String },
}

impl TypedStoreError {
    fn from_store(error: StoreError) -> Self {
        match error {
            StoreError::Retryable { detail } => Self::Retryable { detail },
            StoreError::Rejected { detail } => Self::Rejected { detail },
            StoreError::Contradiction(contradiction) => Self::Contradiction {
                detail: contradiction.to_string(),
            },
        }
    }

    fn contradiction(detail: impl Into<String>) -> Self {
        Self::Contradiction {
            detail: detail.into(),
        }
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
