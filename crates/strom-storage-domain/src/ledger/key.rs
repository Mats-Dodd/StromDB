//! Ordered raw spelling of a canonical stream path.

use serde::Serialize;
use strom_domain::StreamId;

use crate::envelope::DecodeError;

/// The key is immutable after construction, so a boxed slice drops the
/// capacity word a `Vec` would carry through every fact and row.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LedgerKey(Box<[u8]>);

impl LedgerKey {
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<&StreamId> for LedgerKey {
    fn from(stream_id: &StreamId) -> Self {
        Self(stream_id.as_str().as_bytes().into())
    }
}

pub(crate) struct LedgerKeyWire(Vec<u8>);

impl From<Vec<u8>> for LedgerKeyWire {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl TryFrom<LedgerKeyWire> for LedgerKey {
    type Error = DecodeError;

    fn try_from(wire: LedgerKeyWire) -> Result<Self, Self::Error> {
        let bytes = wire.0;
        let raw = std::str::from_utf8(&bytes).map_err(|_detail| DecodeError::InvalidBody)?;
        raw.parse::<StreamId>()
            .map(|_stream_id| Self(bytes.into_boxed_slice()))
            .map_err(|_detail| DecodeError::InvalidBody)
    }
}

impl Serialize for LedgerKey {
    fn serialize<Serializer: serde::Serializer>(
        &self,
        serializer: Serializer,
    ) -> Result<Serializer::Ok, Serializer::Error> {
        serializer.serialize_bytes(&self.0)
    }
}
