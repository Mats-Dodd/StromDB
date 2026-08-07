//! Shared rkyv boundary for durable storage objects.

mod adapter;

use rkyv::Serialize;
use rkyv::api::high::{HighSerializer, to_bytes_in_with_alloc};
use rkyv::rancor::Failure;
use rkyv::ser::allocator::{Arena, ArenaHandle};
use rkyv::ser::{Positional, Writer};

pub(crate) use adapter::{
    ContentTypeAsString, ExpiryAsArchive, LifecycleAsArchive, StreamPathAsString,
    decode_content_type, decode_stream_path,
};

/// A durable object could not be encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EncodeError {
    #[error("domain value could not be archived")]
    Serialization,
    #[error("encoded value exceeds the {bytes_max}-byte bound")]
    EncodedBytesOverMax { bytes_max: usize },
}

/// A durable object could not be decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("encoded value is {bytes_actual} bytes; the bound is {bytes_max}")]
    EncodedBytesOverMax {
        bytes_max: usize,
        bytes_actual: usize,
    },
    #[error("archive is structurally malformed")]
    MalformedArchive,
    #[error("archive violates a domain invariant")]
    InvalidBody,
    #[error("body identity differs from the durable location")]
    IdentityMismatch,
}

/// Complete encoded bytes exceeded the named decode bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EncodedBytesOverMax {
    pub(crate) bytes_max: usize,
    pub(crate) bytes_actual: usize,
}

impl From<EncodedBytesOverMax> for DecodeError {
    fn from(error: EncodedBytesOverMax) -> Self {
        Self::EncodedBytesOverMax {
            bytes_max: error.bytes_max,
            bytes_actual: error.bytes_actual,
        }
    }
}

pub(crate) fn encode<Value>(value: &Value, bytes_max: usize) -> Result<Vec<u8>, EncodeError>
where
    Value: for<'arena, 'writer> Serialize<
        HighSerializer<&'writer mut BoundedWriter, ArenaHandle<'arena>, Failure>,
    >,
{
    let mut writer = BoundedWriter::new(bytes_max);
    let mut arena = Arena::new();
    if to_bytes_in_with_alloc::<_, _, Failure>(value, &mut writer, arena.acquire()).is_err() {
        return if writer.over_bound {
            Err(EncodeError::EncodedBytesOverMax { bytes_max })
        } else {
            Err(EncodeError::Serialization)
        };
    }
    Ok(writer.bytes)
}

pub(crate) struct BoundedWriter {
    bytes: Vec<u8>,
    bytes_max: usize,
    over_bound: bool,
}

impl BoundedWriter {
    const fn new(bytes_max: usize) -> Self {
        Self {
            bytes: Vec::new(),
            bytes_max,
            over_bound: false,
        }
    }
}

impl Positional for BoundedWriter {
    fn pos(&self) -> usize {
        self.bytes.len()
    }
}

impl Writer<Failure> for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<(), Failure> {
        let bytes_actual = self.bytes.len().saturating_add(bytes.len());
        if bytes_actual > self.bytes_max {
            self.over_bound = true;
            return Err(Failure);
        }
        if bytes.len() > self.bytes.capacity().saturating_sub(self.bytes.len())
            && self.bytes.try_reserve(bytes.len()).is_err()
        {
            return Err(Failure);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

pub(crate) const fn decode_bound(
    bytes: &[u8],
    bytes_max: usize,
) -> Result<(), EncodedBytesOverMax> {
    if bytes.len() > bytes_max {
        return Err(EncodedBytesOverMax {
            bytes_max,
            bytes_actual: bytes.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rkyv::{Archive, Serialize};

    use super::*;

    #[derive(Archive, Serialize)]
    struct BoundProbe {
        marker: u64,
    }

    #[test]
    fn complete_archive_bound_is_shared_by_encode_and_decode() {
        let probe = BoundProbe { marker: 1 };
        let bytes = encode(&probe, 1_024).expect("the small fixture fits its archive bound");
        let first_crossing = bytes.len().saturating_sub(1);

        assert_eq!(
            encode(&probe, first_crossing),
            Err(EncodeError::EncodedBytesOverMax {
                bytes_max: first_crossing,
            }),
            "encode checks the complete archive length"
        );
        assert_eq!(
            decode_bound(&bytes, first_crossing),
            Err(EncodedBytesOverMax {
                bytes_max: first_crossing,
                bytes_actual: bytes.len(),
            }),
            "decode rejects the same complete archive boundary"
        );
    }
}
