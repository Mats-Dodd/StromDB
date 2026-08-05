//! Stream-root-relative URL path that identifies a stream.

use std::fmt;
use std::str::FromStr;

/// Stream-root-relative path identifying one stream.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(String);

/// Upper bound on a stream id, in bytes.
///
/// The protocol states no limit, so this is strom's own bound on work per
/// request (§10) and on the key length any storage layout must carry.
pub const STREAM_ID_BYTES_MAX: usize = 512;

/// §6: a first segment of `__ds` addresses subscription control APIs, never an
/// application stream.
const ROOT_SEGMENT_RESERVED: &str = "__ds";

impl StreamId {
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

impl serde::Serialize for StreamId {
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        serializer.serialize_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StreamIdError {
    #[error("stream id exceeds {STREAM_ID_BYTES_MAX} bytes")]
    OverMaxBytes,
    #[error("stream id contains a control character")]
    ControlCharacter,
    #[error("stream id is empty or has an empty segment")]
    EmptySegment,
    #[error("stream id contains a `.` or `..` segment")]
    RelativeSegment,
    #[error("stream id starts with the reserved `{ROOT_SEGMENT_RESERVED}` segment")]
    ReservedRootSegment,
}
