//! Ordered raw spelling of a canonical stream path.

use std::fmt;

use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use strom_domain::StreamId;

use crate::bounds::DIRECTORY_KEY_BYTES_MAX;
use crate::envelope::DecodeError;

/// The key is immutable after construction, so a boxed slice drops the
/// capacity word a `Vec` would carry through every fact and row.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DirectoryKey(Box<[u8]>);

impl DirectoryKey {
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
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

#[derive(Debug)]
pub(crate) struct DirectoryKeyWire(Vec<u8>);

impl From<Vec<u8>> for DirectoryKeyWire {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl<'de> Deserialize<'de> for DirectoryKeyWire {
    fn deserialize<DeserializerType: Deserializer<'de>>(
        deserializer: DeserializerType,
    ) -> Result<Self, DeserializerType::Error> {
        deserializer.deserialize_bytes(DirectoryKeyVisitor)
    }
}

#[cfg(test)]
impl Serialize for DirectoryKeyWire {
    fn serialize<Serializer: serde::Serializer>(
        &self,
        serializer: Serializer,
    ) -> Result<Serializer::Ok, Serializer::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

struct DirectoryKeyVisitor;

impl<'de> Visitor<'de> for DirectoryKeyVisitor {
    type Value = DirectoryKeyWire;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "at most {DIRECTORY_KEY_BYTES_MAX} Directory key bytes"
        )
    }

    fn visit_borrowed_bytes<Error: serde::de::Error>(
        self,
        v: &'de [u8],
    ) -> Result<Self::Value, Error> {
        self.visit_bytes(v)
    }

    fn visit_bytes<Error: serde::de::Error>(self, v: &[u8]) -> Result<Self::Value, Error> {
        if v.len() > DIRECTORY_KEY_BYTES_MAX {
            return Err(Error::invalid_length(v.len(), &self));
        }
        Ok(DirectoryKeyWire(v.to_vec()))
    }

    fn visit_byte_buf<Error: serde::de::Error>(self, v: Vec<u8>) -> Result<Self::Value, Error> {
        if v.len() > DIRECTORY_KEY_BYTES_MAX {
            return Err(Error::invalid_length(v.len(), &self));
        }
        Ok(DirectoryKeyWire(v))
    }

    fn visit_seq<Sequence: SeqAccess<'de>>(
        self,
        mut seq: Sequence,
    ) -> Result<Self::Value, Sequence::Error> {
        if seq
            .size_hint()
            .is_some_and(|declared| declared > DIRECTORY_KEY_BYTES_MAX)
        {
            return Err(serde::de::Error::invalid_length(
                seq.size_hint().unwrap_or(DIRECTORY_KEY_BYTES_MAX),
                &self,
            ));
        }
        let mut bytes = Vec::new();
        while bytes.len() < DIRECTORY_KEY_BYTES_MAX {
            let Some(byte) = seq.next_element()? else {
                return Ok(DirectoryKeyWire(bytes));
            };
            bytes.push(byte);
        }
        if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
            return Err(serde::de::Error::invalid_length(
                DIRECTORY_KEY_BYTES_MAX.saturating_add(1),
                &self,
            ));
        }
        Ok(DirectoryKeyWire(bytes))
    }
}

impl TryFrom<DirectoryKeyWire> for DirectoryKey {
    type Error = DecodeError;

    fn try_from(wire: DirectoryKeyWire) -> Result<Self, Self::Error> {
        Self::try_from(wire.0.into_boxed_slice()).map_err(|_detail| DecodeError::InvalidBody)
    }
}

impl Serialize for DirectoryKey {
    fn serialize<Serializer: serde::Serializer>(
        &self,
        serializer: Serializer,
    ) -> Result<Serializer::Ok, Serializer::Error> {
        serializer.serialize_bytes(&self.0)
    }
}
