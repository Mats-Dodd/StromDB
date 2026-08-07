//! Stream-root-relative URL path that identifies a stream.

use std::fmt;
use std::str::FromStr;

/// Stream-root-relative path identifying one stream.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamPath(Box<str>);

/// Upper bound on a stream path, in bytes.
///
/// The protocol states no limit, so this is strom's own bound on work per
/// request (§10) and on the key length any storage layout must carry.
pub const STREAM_PATH_BYTES_MAX: usize = 512;

/// §6: a first segment of `__ds` addresses subscription control APIs, never an
/// application stream.
const ROOT_SEGMENT_RESERVED: &str = "__ds";

impl StreamPath {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Validates and returns one borrowed canonical stream-path spelling.
    ///
    /// # Errors
    ///
    /// Returns [`StreamPathError`] when `path` violates a stream-path invariant.
    pub fn validate(path: &str) -> Result<&str, StreamPathError> {
        if path.len() > STREAM_PATH_BYTES_MAX {
            return Err(StreamPathError::OverMaxBytes);
        }
        if path.chars().any(char::is_control) {
            return Err(StreamPathError::ControlCharacter);
        }
        for (segment_index, segment) in path.split('/').enumerate() {
            if segment.is_empty() {
                return Err(StreamPathError::EmptySegment);
            }
            if segment == "." || segment == ".." {
                return Err(StreamPathError::RelativeSegment);
            }
            if segment_index == 0 && segment == ROOT_SEGMENT_RESERVED {
                return Err(StreamPathError::ReservedRootSegment);
            }
        }
        Ok(path)
    }
}

impl FromStr for StreamPath {
    type Err = StreamPathError;

    fn from_str(path: &str) -> Result<Self, Self::Err> {
        Self::validate(path).map(|canonical| Self(canonical.into()))
    }
}

impl fmt::Display for StreamPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl serde::Serialize for StreamPath {
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        serializer.serialize_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StreamPathError {
    #[error("stream path exceeds {STREAM_PATH_BYTES_MAX} bytes")]
    OverMaxBytes,
    #[error("stream path contains a control character")]
    ControlCharacter,
    #[error("stream path is empty or has an empty segment")]
    EmptySegment,
    #[error("stream path contains a `.` or `..` segment")]
    RelativeSegment,
    #[error("stream path starts with the reserved `{ROOT_SEGMENT_RESERVED}` segment")]
    ReservedRootSegment,
}
