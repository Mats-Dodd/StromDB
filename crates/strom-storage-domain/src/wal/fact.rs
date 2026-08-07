//! Already-decided stream mutation facts.

use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle, StreamPath};

use crate::StreamUid;
use crate::archive::{
    ContentTypeAsString, ExpiryAsArchive, LifecycleAsArchive, StreamPathAsString,
};

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
