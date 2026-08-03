//! The client-visible identity of a stream: its stream-root-relative URL path.

use std::fmt;
use std::str::FromStr;

/// The stream-root-relative path that identifies one stream.
///
/// A stream *is* a URL (protocol §3). Courant addresses a stream by the
/// decoded path below the stream root, such as `events/abc`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(String);

/// Upper bound on a stream id, in bytes.
///
/// The protocol sets no limit, so this is a Courant bound. Budget: the id is
/// embedded in every request URL next to an offset of up to 256 bytes
/// (protocol §8), query parameters, and a server prefix. 512 bytes keeps
/// every stream URL comfortably below the ~2 KiB limit common to CDNs and
/// proxies. The id is also the Ledger sort key, where bounded keys keep
/// records bounded.
pub const STREAM_ID_BYTES_MAX: usize = 512;

/// The root segment reserved for Durable Streams control APIs (protocol §6).
const ROOT_SEGMENT_RESERVED: &str = "__ds";

impl StreamId {
    /// Returns the exact path this id was parsed from.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for StreamId {
    type Err = StreamIdError;

    fn from_str(path: &str) -> Result<Self, Self::Err> {
        if path.len() > STREAM_ID_BYTES_MAX {
            return Err(StreamIdError::OverMaxBytes);
        }
        // Control characters have no legitimate place in a decoded URL path
        // and are a log-injection vector (protocol §12.3).
        if path.chars().any(char::is_control) {
            return Err(StreamIdError::ControlCharacter);
        }
        for (segment_index, segment) in path.split('/').enumerate() {
            if segment.is_empty() {
                return Err(StreamIdError::EmptySegment);
            }
            if segment == "." || segment == ".." {
                return Err(StreamIdError::RelativeSegment);
            }
            if segment_index == 0 && segment == ROOT_SEGMENT_RESERVED {
                return Err(StreamIdError::ReservedRootSegment);
            }
        }
        Ok(Self(path.to_owned()))
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Durable spelling: the exact path as a serde string. `Deserialize` is
/// deliberately absent; bytes re-enter through [`FromStr`], the one canonical
/// parser, so no decoder can skip the path rules.
impl serde::Serialize for StreamId {
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        serializer.serialize_str(&self.0)
    }
}

/// Why a string is not a valid [`StreamId`].
///
/// Every variant maps to `400 Bad Request` at the HTTP edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamIdError {
    /// The path exceeds [`STREAM_ID_BYTES_MAX`] bytes.
    OverMaxBytes,
    /// The path contains a control character.
    ControlCharacter,
    /// The path is empty or has an empty segment (leading, trailing, or
    /// doubled `/`).
    EmptySegment,
    /// The path contains a `.` or `..` segment.
    RelativeSegment,
    /// The first segment is the reserved `__ds` control prefix (protocol §6).
    ReservedRootSegment,
}

impl fmt::Display for StreamIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OverMaxBytes => {
                write!(formatter, "stream id exceeds {STREAM_ID_BYTES_MAX} bytes")
            }
            Self::ControlCharacter => formatter.write_str("stream id contains a control character"),
            Self::EmptySegment => formatter.write_str("stream id is empty or has an empty segment"),
            Self::RelativeSegment => {
                formatter.write_str("stream id contains a `.` or `..` segment")
            }
            Self::ReservedRootSegment => {
                formatter.write_str("stream id starts with the reserved `__ds` segment")
            }
        }
    }
}

impl std::error::Error for StreamIdError {}
