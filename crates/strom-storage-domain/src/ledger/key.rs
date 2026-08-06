//! Ordered raw spelling of a canonical stream path.

use strom_domain::StreamId;

use crate::archive::DecodeError as ArchiveDecodeError;

/// The key is immutable after construction, so a boxed slice drops the
/// capacity word a `Vec` would carry through every fact and row.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, rkyv::Archive, rkyv::Serialize)]
pub struct DirectoryKey(Box<[u8]>);

impl DirectoryKey {
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl ArchivedDirectoryKey {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub(crate) fn validated_bytes(&self) -> Result<&[u8], ArchiveDecodeError> {
        let bytes = self.as_bytes();
        let raw = std::str::from_utf8(bytes).map_err(|_detail| ArchiveDecodeError::InvalidBody)?;
        StreamId::validate(raw).map_err(|_domain_error| ArchiveDecodeError::InvalidBody)?;
        Ok(bytes)
    }
}

impl TryFrom<&ArchivedDirectoryKey> for DirectoryKey {
    type Error = ArchiveDecodeError;

    fn try_from(key: &ArchivedDirectoryKey) -> Result<Self, Self::Error> {
        key.validated_bytes().map(|bytes| Self(bytes.into()))
    }
}

impl From<&StreamId> for DirectoryKey {
    fn from(stream_id: &StreamId) -> Self {
        Self(stream_id.as_str().as_bytes().into())
    }
}

impl TryFrom<Box<[u8]>> for DirectoryKey {
    type Error = DirectoryKeyError;

    fn try_from(bytes: Box<[u8]>) -> Result<Self, Self::Error> {
        let raw = std::str::from_utf8(&bytes).map_err(|_detail| DirectoryKeyError::InvalidUtf8)?;
        raw.parse::<StreamId>()
            .map(|_stream_id| Self(bytes))
            .map_err(DirectoryKeyError::StreamId)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DirectoryKeyError {
    #[error("Directory key is not UTF-8")]
    InvalidUtf8,
    #[error("Directory key is not a canonical stream id: {0}")]
    StreamId(#[source] strom_domain::StreamIdError),
}
