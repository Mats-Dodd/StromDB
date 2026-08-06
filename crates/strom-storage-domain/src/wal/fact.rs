//! Already-decided stream mutation facts.

use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};

use crate::archive::{ContentTypeAsString, ExpiryAsArchive, LifecycleAsArchive};
use crate::{DirectoryKey, StreamUid};

#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize)]
#[rkyv(attr(expect(
    clippy::enum_variant_names,
    reason = "the established domain vocabulary names stream facts explicitly"
)))]
pub enum OperationFact {
    StreamCreated {
        path: DirectoryKey,
        uid: StreamUid,
        #[rkyv(with = ContentTypeAsString)]
        content_type: StreamContentType,
        #[rkyv(with = ExpiryAsArchive)]
        expiry: ExpiryPolicy,
        #[rkyv(with = LifecycleAsArchive)]
        lifecycle: StreamLifecycle,
    },
    StreamClosed {
        path: DirectoryKey,
        uid: StreamUid,
    },
    StreamDeleted {
        path: DirectoryKey,
        uid: StreamUid,
    },
}
