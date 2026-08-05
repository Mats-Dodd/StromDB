//! Seal envelope codec.

use serde::Deserialize;

use super::{Seal, SealFormat, SealGeneration, SealIdentity, TreeVersion, WalReplayPoint};
use crate::bounds::SEAL_ENCODED_BYTES_MAX;
use crate::envelope::{DecodeError, EncodeError, ObjectKind, decode_frame, encode_frame};
use crate::{OwnerToken, PartitionId};

const VERSION: u8 = 1;

/// # Errors
///
/// Returns [`EncodeError`] when serialization fails or the complete frame
/// exceeds [`SEAL_ENCODED_BYTES_MAX`].
pub fn encode_seal(seal: &Seal) -> Result<Vec<u8>, EncodeError> {
    encode_frame(ObjectKind::Seal, VERSION, seal, SEAL_ENCODED_BYTES_MAX)
}

/// # Errors
///
/// Returns [`DecodeError`] at the first failed envelope, body, or identity
/// gate.
pub fn decode_seal(identity: &SealIdentity, bytes: &[u8]) -> Result<Seal, DecodeError> {
    let body = decode_frame(ObjectKind::Seal, VERSION, bytes, SEAL_ENCODED_BYTES_MAX)?;
    let (wire, trailing) = postcard::take_from_bytes::<SealWire>(body)
        .map_err(|_detail| DecodeError::MalformedBody)?;
    let seal = Seal::try_from(wire)?;
    if !trailing.is_empty() {
        return Err(DecodeError::TrailingBytes {
            bytes_actual: trailing.len(),
        });
    }
    if seal.identity() != *identity {
        return Err(DecodeError::IdentityMismatch);
    }
    Ok(seal)
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
struct SealWire {
    partition: [u8; 16],
    generation: u64,
    replay: WalReplayPointWire,
    format: u8,
    ledger: (),
    tally: (),
    annals: (),
}

impl TryFrom<SealWire> for Seal {
    type Error = DecodeError;

    fn try_from(wire: SealWire) -> Result<Self, Self::Error> {
        let partition =
            PartitionId::try_from(wire.partition).map_err(|_detail| DecodeError::InvalidBody)?;
        let generation = SealGeneration::try_from(wire.generation)
            .map_err(|_detail| DecodeError::InvalidBody)?;
        let replay = WalReplayPoint::try_from(wire.replay)?;
        let format = match wire.format {
            1 => SealFormat::V1,
            _ => return Err(DecodeError::FormatMismatch),
        };
        let () = wire.ledger;
        let () = wire.tally;
        let () = wire.annals;
        Ok(Self::new(
            partition,
            generation,
            replay,
            format,
            TreeVersion::empty(),
            TreeVersion::empty(),
            TreeVersion::empty(),
        ))
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
enum WalReplayPointWire {
    Genesis,
    Through { batch: u64, owner: u64 },
}

impl TryFrom<WalReplayPointWire> for WalReplayPoint {
    type Error = DecodeError;

    fn try_from(wire: WalReplayPointWire) -> Result<Self, Self::Error> {
        match wire {
            WalReplayPointWire::Genesis => Ok(Self::Genesis),
            WalReplayPointWire::Through { batch, owner } => {
                let batch =
                    crate::BatchId::try_from(batch).map_err(|_detail| DecodeError::InvalidBody)?;
                let generation =
                    SealGeneration::try_from(owner).map_err(|_detail| DecodeError::InvalidBody)?;
                Ok(Self::Through {
                    batch,
                    owner: OwnerToken::from(generation),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_version_one_rejects_another_seal_format() -> Result<(), Box<dyn std::error::Error>>
    {
        let partition: PartitionId = "00112233-4455-6677-8899-aabbccddeeff".parse()?;
        let identity = SealIdentity::new(partition, SealGeneration::genesis());
        let wire = SealWire {
            partition: *partition.as_bytes(),
            generation: 1,
            replay: WalReplayPointWire::Genesis,
            format: 2,
            ledger: (),
            tally: (),
            annals: (),
        };
        let bytes = encode_frame(ObjectKind::Seal, VERSION, &wire, SEAL_ENCODED_BYTES_MAX)?;
        assert_eq!(
            decode_seal(&identity, &bytes),
            Err(DecodeError::FormatMismatch),
            "envelope version one pins the tree and comparator semantics to SealFormat::V1"
        );
        Ok(())
    }

    #[test]
    fn body_constructors_reject_reserved_zero_coordinates() -> Result<(), Box<dyn std::error::Error>>
    {
        let partition: PartitionId = "00112233-4455-6677-8899-aabbccddeeff".parse()?;
        let identity = SealIdentity::new(partition, SealGeneration::genesis());
        let wire = SealWire {
            partition: *partition.as_bytes(),
            generation: 0,
            replay: WalReplayPointWire::Genesis,
            format: 1,
            ledger: (),
            tally: (),
            annals: (),
        };
        let bytes = encode_frame(ObjectKind::Seal, VERSION, &wire, SEAL_ENCODED_BYTES_MAX)?;
        assert_eq!(
            decode_seal(&identity, &bytes),
            Err(DecodeError::InvalidBody),
            "private wire values must re-enter through the generation constructor"
        );
        Ok(())
    }
}
