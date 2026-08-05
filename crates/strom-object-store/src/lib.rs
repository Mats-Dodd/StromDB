//! Raw object-store adapter for `StromDB`.
//!
//! This crate is the capability layer beneath the typed Seal, WAL, and
//! content stores (`docs/architecture.md`, "storage capability contract").
//! It moves opaque bounded bytes and normalizes conditional results into
//! evidence; it never decodes envelopes, spells durable keys, or retries an
//! authority-bearing create.

mod adapter;
mod bounds;
mod bytes;
mod error;
mod evidence;
mod key;

pub use adapter::{ObjectStoreAdapter, S3Config, S3Credentials};
pub use bounds::{KEY_BYTES_MAX, LIST_KEYS_MAX, PUT_BYTES_MAX};
pub use bytes::{
    ByteBound, ByteRange, ByteRangeError, Checksum, EmptyEtag, Etag, FrozenBytes, FrozenBytesError,
    ZeroByteBound,
};
pub use error::{S3ConfigError, StoreContradiction, StoreError};
pub use evidence::{
    CreateEvidence, KeysBound, KeysBoundError, ListPage, ListPageRequest, RawObject,
    VerifiedRangeBytes,
};
pub use key::{ObjectKey, ObjectKeyError};
