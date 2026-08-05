//! Checksummed framing shared by Seal and WAL objects.

use serde::Serialize;

const MAGIC: &[u8; 4] = b"STRM";
const HEADER_BYTES: usize = MAGIC.len() + 2;
const CHECKSUM_BYTES: usize = size_of::<u32>();
const FRAME_BYTES_MIN: usize = HEADER_BYTES + CHECKSUM_BYTES;
const KIND_OFFSET: usize = MAGIC.len();
const VERSION_OFFSET: usize = KIND_OFFSET + 1;

const _: () = assert!(
    FRAME_BYTES_MIN == 10,
    "frame layout must remain four magic bytes, two discriminator bytes, and one checksum"
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectKind {
    Seal,
    Wal,
}

impl ObjectKind {
    pub(crate) const fn encoded(self) -> u8 {
        match self {
            Self::Seal => 1,
            Self::Wal => 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EncodeError {
    #[error("domain value could not be serialized")]
    Serialization,
    #[error("encoded value is {bytes_actual} bytes; the bound is {bytes_max}")]
    EncodedBytesOverMax {
        bytes_max: usize,
        bytes_actual: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("encoded value is {bytes_actual} bytes; the bound is {bytes_max}")]
    EncodedBytesOverMax {
        bytes_max: usize,
        bytes_actual: usize,
    },
    #[error("frame is {bytes_actual} bytes; at least {bytes_min} are required")]
    FrameTooShort {
        bytes_min: usize,
        bytes_actual: usize,
    },
    #[error("frame magic is {observed:?}; expected STRM")]
    MagicMismatch { observed: [u8; 4] },
    #[error("frame checksum is {declared:#010x}; computed {computed:#010x}")]
    ChecksumMismatch { declared: u32, computed: u32 },
    #[error("object kind is {observed}; expected {expected} at this location")]
    ObjectKindMismatch { expected: u8, observed: u8 },
    #[error("object format version {observed} is unsupported")]
    UnsupportedVersion { observed: u8 },
    #[error("postcard body is malformed")]
    MalformedBody,
    #[error("postcard body violates a domain invariant")]
    InvalidBody,
    #[error("body identity differs from the durable location")]
    IdentityMismatch,
    #[error("body format does not match the envelope version")]
    FormatMismatch,
    #[error("postcard body has {bytes_actual} trailing bytes")]
    TrailingBytes { bytes_actual: usize },
}

#[expect(
    clippy::big_endian_bytes,
    reason = "RFC 0002 fixes the trailing CRC-32C spelling as big-endian"
)]
pub(crate) fn encode_frame<T: Serialize>(
    kind: ObjectKind,
    version: u8,
    value: &T,
    bytes_max: usize,
) -> Result<Vec<u8>, EncodeError> {
    let mut frame = Vec::with_capacity(FRAME_BYTES_MIN);
    frame.extend_from_slice(MAGIC);
    frame.push(kind.encoded());
    frame.push(version);
    let mut frame =
        postcard::to_extend(value, frame).map_err(|_detail| EncodeError::Serialization)?;
    let checksum = crc32c::crc32c(&frame);
    frame.extend_from_slice(&checksum.to_be_bytes());
    enforce_encode_bound(frame, bytes_max)
}

pub(crate) fn encode_body<T: Serialize>(
    value: &T,
    bytes_max: usize,
) -> Result<Vec<u8>, EncodeError> {
    let bytes = postcard::to_allocvec(value).map_err(|_detail| EncodeError::Serialization)?;
    enforce_encode_bound(bytes, bytes_max)
}

#[expect(
    clippy::big_endian_bytes,
    reason = "RFC 0002 fixes the trailing CRC-32C spelling as big-endian"
)]
pub(crate) fn decode_frame(
    expected_kind: ObjectKind,
    expected_version: u8,
    bytes: &[u8],
    bytes_max: usize,
) -> Result<&[u8], DecodeError> {
    enforce_decode_bound(bytes, bytes_max)?;
    if bytes.len() < FRAME_BYTES_MIN {
        return Err(DecodeError::FrameTooShort {
            bytes_min: FRAME_BYTES_MIN,
            bytes_actual: bytes.len(),
        });
    }
    let observed_magic: [u8; 4] = bytes
        .get(..MAGIC.len())
        .ok_or(DecodeError::MalformedBody)?
        .try_into()
        .map_err(|_detail| DecodeError::MalformedBody)?;
    if observed_magic != *MAGIC {
        return Err(DecodeError::MagicMismatch {
            observed: observed_magic,
        });
    }

    let covered_bytes = bytes
        .len()
        .checked_sub(CHECKSUM_BYTES)
        .expect("the minimum frame length includes the checksum");
    let (covered, checksum_bytes) = bytes.split_at(covered_bytes);
    let checksum_array = checksum_bytes
        .try_into()
        .map_err(|_detail| DecodeError::MalformedBody)?;
    let declared = u32::from_be_bytes(checksum_array);
    let computed = crc32c::crc32c(covered);
    if declared != computed {
        return Err(DecodeError::ChecksumMismatch { declared, computed });
    }

    let observed_kind = bytes
        .get(KIND_OFFSET)
        .copied()
        .expect("the minimum frame length includes the object kind");
    if observed_kind != expected_kind.encoded() {
        return Err(DecodeError::ObjectKindMismatch {
            expected: expected_kind.encoded(),
            observed: observed_kind,
        });
    }
    let observed_version = bytes
        .get(VERSION_OFFSET)
        .copied()
        .expect("the minimum frame length includes the format version");
    if observed_version != expected_version {
        return Err(DecodeError::UnsupportedVersion {
            observed: observed_version,
        });
    }
    covered
        .get(HEADER_BYTES..)
        .ok_or(DecodeError::MalformedBody)
}

pub(crate) const fn decode_body_bound(bytes: &[u8], bytes_max: usize) -> Result<(), DecodeError> {
    enforce_decode_bound(bytes, bytes_max)
}

fn enforce_encode_bound(bytes: Vec<u8>, bytes_max: usize) -> Result<Vec<u8>, EncodeError> {
    if bytes.len() > bytes_max {
        return Err(EncodeError::EncodedBytesOverMax {
            bytes_max,
            bytes_actual: bytes.len(),
        });
    }
    Ok(bytes)
}

const fn enforce_decode_bound(bytes: &[u8], bytes_max: usize) -> Result<(), DecodeError> {
    if bytes.len() > bytes_max {
        return Err(DecodeError::EncodedBytesOverMax {
            bytes_max,
            bytes_actual: bytes.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoder_bounds_include_the_complete_frame_overhead() -> Result<(), EncodeError> {
        let value = vec![0u8; 4];
        let body = encode_body(&value, 5)?;
        assert_eq!(
            body.len(),
            5,
            "postcard sequence length and contents define the frameless bound"
        );
        assert_eq!(
            encode_body(&value, 4),
            Err(EncodeError::EncodedBytesOverMax {
                bytes_max: 4,
                bytes_actual: 5,
            }),
            "the encoder rejects the first body bound crossing"
        );

        let frame = encode_frame(ObjectKind::Seal, 1, &value, 15)?;
        assert_eq!(
            frame.len(),
            15,
            "the complete frame adds six header and four checksum bytes"
        );
        assert_eq!(
            encode_frame(ObjectKind::Seal, 1, &value, 14),
            Err(EncodeError::EncodedBytesOverMax {
                bytes_max: 14,
                bytes_actual: 15,
            }),
            "frame bounds include magic, discriminators, and checksum"
        );
        Ok(())
    }
}
