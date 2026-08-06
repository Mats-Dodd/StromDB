//! Raw object-store adapter for `StromDB`.
//!
//! This crate is the capability layer beneath the typed Seal, WAL, and
//! content stores (`docs/architecture.md`, "storage capability contract").
//! It moves opaque bounded bytes and normalizes conditional results into
//! evidence; it never interprets object bodies, spells durable keys, or retries an
//! authority-bearing create.

mod adapter;
mod bounds;
mod bytes;
mod error;
mod evidence;
mod key;

pub use adapter::ObjectStoreAdapter;
pub use bounds::{KEY_BYTES_MAX, LIST_KEYS_MAX, PUT_BYTES_MAX};
pub use bytes::{ByteBound, EmptyEtag, Etag, FrozenBytes, FrozenBytesError, ZeroByteBound};
pub use error::{StoreContradiction, StoreError};
pub use evidence::{
    CreateEvidence, KeysBound, KeysBoundError, ListPage, ListPageRequest, RawObject,
};
pub use key::{ObjectKey, ObjectKeyError};
