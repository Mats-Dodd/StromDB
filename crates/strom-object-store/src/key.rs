//! Canonical object keys.
//!
//! The adapter moves opaque bytes; it never spells durable keys itself. The
//! typed stores derive canonical keys from domain identities and hand them
//! down as [`ObjectKey`] values. Parsing here enforces only the transport
//! rules every durable key obeys: bounded length, a fixed character set, and
//! slash-separated non-empty segments.

use std::fmt;
use std::str::FromStr;

use crate::bounds::KEY_BYTES_MAX;

/// A canonical, bounded, slash-separated object key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey(String);

impl ObjectKey {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn to_store_path(&self) -> object_store::path::Path {
        object_store::path::Path::parse(&self.0)
            .expect("ObjectKey construction guarantees a valid store path")
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl TryFrom<String> for ObjectKey {
    type Error = ObjectKeyError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        if raw.is_empty() {
            return Err(ObjectKeyError::Empty);
        }
        if raw.len() > KEY_BYTES_MAX {
            return Err(ObjectKeyError::TooLong {
                bytes_actual: raw.len(),
            });
        }
        for segment in raw.split('/') {
            if segment.is_empty() {
                return Err(ObjectKeyError::EmptySegment);
            }
            if segment == "." || segment == ".." {
                return Err(ObjectKeyError::RelativeSegment);
            }
            if let Some(character) = segment
                .chars()
                .find(|character| !is_key_character(*character))
            {
                return Err(ObjectKeyError::ForbiddenCharacter { character });
            }
        }
        Ok(Self(raw))
    }
}

impl TryFrom<&str> for ObjectKey {
    type Error = ObjectKeyError;

    fn try_from(raw: &str) -> Result<Self, Self::Error> {
        Self::try_from(raw.to_owned())
    }
}

impl FromStr for ObjectKey {
    type Err = ObjectKeyError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::try_from(raw)
    }
}

const fn is_key_character(character: char) -> bool {
    character.is_ascii_lowercase()
        || character.is_ascii_digit()
        || matches!(character, '-' | '_' | '.')
}

/// Why a raw string is not a canonical object key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ObjectKeyError {
    #[error("object key is empty")]
    Empty,
    #[error("object key is {bytes_actual} bytes; the bound is {KEY_BYTES_MAX}")]
    TooLong { bytes_actual: usize },
    #[error("object key contains forbidden character {character:?}")]
    ForbiddenCharacter { character: char },
    #[error("object key contains an empty segment")]
    EmptySegment,
    #[error("object key contains a `.` or `..` segment")]
    RelativeSegment,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_key_round_trips_through_parse_and_display() {
        let raw = "partition/p1/seal/v1/18446744073709551614";
        let key: ObjectKey = raw.parse().expect("canonical key parses");
        assert_eq!(
            key.to_string(),
            raw,
            "display must reproduce the parsed spelling"
        );
    }

    #[test]
    fn every_transport_rule_rejects_its_violation() {
        let oversized = "a".repeat(KEY_BYTES_MAX + 1);
        let cases = [
            ("", ObjectKeyError::Empty),
            (
                oversized.as_str(),
                ObjectKeyError::TooLong {
                    bytes_actual: KEY_BYTES_MAX + 1,
                },
            ),
            (
                "partition/P1/seal",
                ObjectKeyError::ForbiddenCharacter { character: 'P' },
            ),
            (
                "partition/p 1",
                ObjectKeyError::ForbiddenCharacter { character: ' ' },
            ),
            ("/partition/p1", ObjectKeyError::EmptySegment),
            ("partition/p1/", ObjectKeyError::EmptySegment),
            ("partition//p1", ObjectKeyError::EmptySegment),
            ("partition/./p1", ObjectKeyError::RelativeSegment),
            ("partition/../p1", ObjectKeyError::RelativeSegment),
        ];
        for (raw, expected) in cases {
            assert_eq!(
                ObjectKey::try_from(raw),
                Err(expected),
                "input {raw:?} must be rejected"
            );
        }
    }

    #[test]
    fn key_at_the_length_bound_parses() {
        let raw = "a".repeat(KEY_BYTES_MAX);
        assert!(
            ObjectKey::try_from(raw.as_str()).is_ok(),
            "a key of exactly {KEY_BYTES_MAX} bytes is legal"
        );
    }
}
