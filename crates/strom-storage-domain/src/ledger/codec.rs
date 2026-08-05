//! Frameless Ledger record codec.

use serde::Deserialize;

use super::{LedgerRecord, PathTombstone, StreamRecord};
use crate::bounds::LEDGER_RECORD_BYTES_MAX;
use crate::envelope::{DecodeError, EncodeError, decode_body_bound, encode_body};
use crate::wire::{ExpiryPolicyWire, StreamLifecycleWire, parse_content_type};
use crate::{BatchId, StreamUid};

/// # Errors
///
/// Returns [`EncodeError`] when serialization fails or the row exceeds
/// [`LEDGER_RECORD_BYTES_MAX`].
pub fn encode_ledger_record(record: &LedgerRecord) -> Result<Vec<u8>, EncodeError> {
    encode_body(record, LEDGER_RECORD_BYTES_MAX)
}

/// # Errors
///
/// Returns [`DecodeError`] when the row is over-bound, malformed, violates a
/// domain invariant, or has trailing bytes.
pub fn decode_ledger_record(bytes: &[u8]) -> Result<LedgerRecord, DecodeError> {
    decode_body_bound(bytes, LEDGER_RECORD_BYTES_MAX)?;
    let (wire, trailing) = postcard::take_from_bytes::<LedgerRecordWire>(bytes)
        .map_err(|_detail| DecodeError::MalformedBody)?;
    let record = LedgerRecord::try_from(wire)?;
    if !trailing.is_empty() {
        return Err(DecodeError::TrailingBytes {
            bytes_actual: trailing.len(),
        });
    }
    Ok(record)
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
enum LedgerRecordWire {
    Live(StreamRecordWire),
    Tombstone(PathTombstoneWire),
}

impl TryFrom<LedgerRecordWire> for LedgerRecord {
    type Error = DecodeError;

    fn try_from(wire: LedgerRecordWire) -> Result<Self, Self::Error> {
        match wire {
            LedgerRecordWire::Live(record) => StreamRecord::try_from(record).map(Self::Live),
            LedgerRecordWire::Tombstone(tombstone) => {
                PathTombstone::try_from(tombstone).map(Self::Tombstone)
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
struct StreamRecordWire {
    uid: u64,
    content_type: String,
    expiry: ExpiryPolicyWire,
    lifecycle: StreamLifecycleWire,
    created_at: u64,
}

impl TryFrom<StreamRecordWire> for StreamRecord {
    type Error = DecodeError;

    fn try_from(wire: StreamRecordWire) -> Result<Self, Self::Error> {
        Ok(Self::new(
            parse_uid(wire.uid)?,
            parse_content_type(&wire.content_type)?,
            strom_domain::ExpiryPolicy::try_from(wire.expiry)?,
            strom_domain::StreamLifecycle::from(wire.lifecycle),
            BatchId::try_from(wire.created_at).map_err(|_detail| DecodeError::InvalidBody)?,
        ))
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
struct PathTombstoneWire {
    uid: u64,
}

impl TryFrom<PathTombstoneWire> for PathTombstone {
    type Error = DecodeError;

    fn try_from(wire: PathTombstoneWire) -> Result<Self, Self::Error> {
        parse_uid(wire.uid).map(Self::new)
    }
}

fn parse_uid(raw: u64) -> Result<StreamUid, DecodeError> {
    StreamUid::try_from(raw).map_err(|_detail| DecodeError::InvalidBody)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::ExpiryPolicyWire;

    #[test]
    fn body_constructors_reject_zero_stream_uids() -> Result<(), Box<dyn std::error::Error>> {
        let wire = LedgerRecordWire::Live(StreamRecordWire {
            uid: 0,
            content_type: String::from("application/octet-stream"),
            expiry: ExpiryPolicyWire::None,
            lifecycle: StreamLifecycleWire::Open,
            created_at: 1,
        });
        let bytes = postcard::to_allocvec(&wire)?;
        assert_eq!(
            decode_ledger_record(&bytes),
            Err(DecodeError::InvalidBody),
            "private wire values must re-enter through the stream uid constructor"
        );
        Ok(())
    }
}
