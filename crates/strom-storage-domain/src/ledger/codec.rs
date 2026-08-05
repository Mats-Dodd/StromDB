//! Frameless stream-record codec.

use serde::Deserialize;

use super::StreamRecord;
use crate::BatchId;
use crate::bounds::STREAM_RECORD_BYTES_MAX;
use crate::envelope::{DecodeError, EncodeError, decode_body_bound, encode_body};
use crate::wire::{ExpiryPolicyWire, StreamLifecycleWire, parse_content_type};

/// # Errors
///
/// Returns [`EncodeError`] when serialization fails or the row exceeds
/// [`STREAM_RECORD_BYTES_MAX`].
pub fn encode_stream_record(record: &StreamRecord) -> Result<Vec<u8>, EncodeError> {
    encode_body(record, STREAM_RECORD_BYTES_MAX)
}

/// # Errors
///
/// Returns [`DecodeError`] when the row is over-bound, malformed, violates a
/// domain invariant, or has trailing bytes.
pub fn decode_stream_record(bytes: &[u8]) -> Result<StreamRecord, DecodeError> {
    decode_body_bound(bytes, STREAM_RECORD_BYTES_MAX)?;
    let (wire, trailing) = postcard::take_from_bytes::<StreamRecordWire>(bytes)
        .map_err(|_detail| DecodeError::MalformedBody)?;
    let record = StreamRecord::try_from(wire)?;
    if !trailing.is_empty() {
        return Err(DecodeError::TrailingBytes {
            bytes_actual: trailing.len(),
        });
    }
    Ok(record)
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
struct StreamRecordWire {
    content_type: String,
    expiry: ExpiryPolicyWire,
    lifecycle: StreamLifecycleWire,
    created_at: u64,
}

impl TryFrom<StreamRecordWire> for StreamRecord {
    type Error = DecodeError;

    fn try_from(wire: StreamRecordWire) -> Result<Self, Self::Error> {
        Ok(Self::new(
            parse_content_type(&wire.content_type)?,
            strom_domain::ExpiryPolicy::try_from(wire.expiry)?,
            strom_domain::StreamLifecycle::from(wire.lifecycle),
            BatchId::try_from(wire.created_at).map_err(|_detail| DecodeError::InvalidBody)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::ExpiryPolicyWire;

    #[test]
    fn body_constructors_reject_zero_batch_ids() -> Result<(), Box<dyn std::error::Error>> {
        let wire = StreamRecordWire {
            content_type: String::from("application/octet-stream"),
            expiry: ExpiryPolicyWire::None,
            lifecycle: StreamLifecycleWire::Open,
            created_at: 0,
        };
        let bytes = postcard::to_allocvec(&wire)?;
        assert_eq!(
            decode_stream_record(&bytes),
            Err(DecodeError::InvalidBody),
            "private wire values must re-enter through the batch constructor"
        );
        Ok(())
    }
}
