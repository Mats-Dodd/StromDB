//! Already-decided stream mutation facts.

use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle, StreamPath};

use crate::StreamUid;
use crate::archive::{
    ContentTypeAsString, ExpiryAsArchive, LifecycleAsArchive, StreamPathAsString,
};
use crate::bounds::WAL_FACT_ENCODED_FIXED_BYTES_MAX;

#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
#[rkyv(attr(expect(
    clippy::enum_variant_names,
    reason = "the established domain vocabulary names stream facts explicitly"
)))]
pub enum OperationFact {
    StreamCreated {
        #[rkyv(with = StreamPathAsString)]
        path: StreamPath,
        uid: StreamUid,
        #[rkyv(with = ContentTypeAsString)]
        content_type: StreamContentType,
        #[rkyv(with = ExpiryAsArchive)]
        expiry: ExpiryPolicy,
        #[rkyv(with = LifecycleAsArchive)]
        lifecycle: StreamLifecycle,
    },
    StreamClosed {
        #[rkyv(with = StreamPathAsString)]
        path: StreamPath,
        uid: StreamUid,
    },
    StreamDeleted {
        #[rkyv(with = StreamPathAsString)]
        path: StreamPath,
        uid: StreamUid,
    },
}

impl OperationFact {
    /// Sound upper estimate of this fact's contribution to a WAL archive.
    #[must_use]
    pub fn estimated_encoded_bytes(&self) -> usize {
        let variable_bytes = match self {
            Self::StreamCreated {
                path,
                uid: _,
                content_type,
                expiry: _,
                lifecycle: _,
            } => path
                .as_bytes()
                .len()
                .saturating_add(content_type.as_str().len()),
            Self::StreamClosed { path, uid: _ } | Self::StreamDeleted { path, uid: _ } => {
                path.as_bytes().len()
            }
        };
        WAL_FACT_ENCODED_FIXED_BYTES_MAX.saturating_add(variable_bytes)
    }
}
