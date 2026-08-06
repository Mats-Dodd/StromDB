//! Expiry configuration fixed at stream creation.

use std::fmt;
use std::num::NonZeroU64;
use std::str::FromStr;

use jiff::Timestamp;

/// How a stream expires. Fixed at creation; TTL and expires-at are mutually exclusive (§5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiryPolicy {
    None,
    SlidingTtl(StreamTtl),
    AbsoluteExpiry(ExpiresAt),
}

impl TryFrom<(Option<StreamTtl>, Option<ExpiresAt>)> for ExpiryPolicy {
    type Error = ExpiryPolicyConflict;

    fn try_from(headers: (Option<StreamTtl>, Option<ExpiresAt>)) -> Result<Self, Self::Error> {
        match headers {
            (None, None) => Ok(Self::None),
            (Some(ttl), None) => Ok(Self::SlidingTtl(ttl)),
            (None, Some(expires_at)) => Ok(Self::AbsoluteExpiry(expires_at)),
            (Some(_), Some(_)) => Err(ExpiryPolicyConflict),
        }
    }
}

impl serde::Serialize for ExpiryPolicy {
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        const ENUM: &str = "ExpiryPolicy";
        match self {
            Self::None => serializer.serialize_unit_variant(ENUM, 0u32, "None"),
            Self::SlidingTtl(ttl) => {
                serializer.serialize_newtype_variant(ENUM, 1u32, "SlidingTtl", ttl)
            }
            Self::AbsoluteExpiry(expires_at) => {
                serializer.serialize_newtype_variant(ENUM, 2u32, "AbsoluteExpiry", expires_at)
            }
        }
    }
}

/// Both `Stream-TTL` and `Stream-Expires-At` were supplied (§5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Stream-TTL and Stream-Expires-At are mutually exclusive")]
pub struct ExpiryPolicyConflict;

/// Sliding idle window in seconds (`Stream-TTL`, §5.1).
///
/// This type is deliberately narrower than the protocol. §5.1 accepts any
/// non-negative decimal integer, so `0` is a well-formed header there. strom
/// rejects it: a zero window expires the stream at the instant it is created,
/// and no client can mean that. `NonZeroU64` removes the case instead of
/// leaving every consumer to re-check it.
///
/// Leading zeros, a leading `+`, and non-decimal spellings are rejected as
/// §5.1 requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamTtl(NonZeroU64);

impl StreamTtl {
    #[must_use]
    pub const fn seconds(self) -> NonZeroU64 {
        self.0
    }
}

impl FromStr for StreamTtl {
    type Err = StreamTtlError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.is_empty() {
            return Err(StreamTtlError::Malformed);
        }
        if input.len() > 1 && input.starts_with('0') {
            return Err(StreamTtlError::Malformed);
        }
        let mut seconds: u64 = 0;
        for character in input.chars() {
            let Some(digit) = character.to_digit(10) else {
                return Err(StreamTtlError::Malformed);
            };
            seconds = seconds
                .checked_mul(10)
                .and_then(|shifted| shifted.checked_add(u64::from(digit)))
                .ok_or(StreamTtlError::OverMax)?;
        }
        NonZeroU64::new(seconds)
            .map(Self)
            .ok_or(StreamTtlError::Zero)
    }
}

impl From<NonZeroU64> for StreamTtl {
    fn from(seconds: NonZeroU64) -> Self {
        Self(seconds)
    }
}

impl fmt::Display for StreamTtl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl serde::Serialize for StreamTtl {
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        serializer.serialize_u64(self.0.get())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StreamTtlError {
    #[error("stream TTL is not a plain decimal integer without leading zeros")]
    Malformed,
    #[error("stream TTL of zero seconds is not accepted")]
    Zero,
    #[error("stream TTL exceeds the representable maximum")]
    OverMax,
}

/// Absolute expiry instant (`Stream-Expires-At`, RFC 3339, §5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExpiresAt(Timestamp);

impl From<ExpiresAt> for i128 {
    fn from(expires_at: ExpiresAt) -> Self {
        expires_at.0.as_nanosecond()
    }
}

impl FromStr for ExpiresAt {
    type Err = ExpiresAtError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        // Reject RFC 9557 time-zone annotations; protocol asks for plain RFC 3339.
        if input.contains('[') {
            return Err(ExpiresAtError);
        }
        input
            .parse::<Timestamp>()
            .map(Self)
            .map_err(|_parse_detail| ExpiresAtError)
    }
}

impl TryFrom<i128> for ExpiresAt {
    type Error = ExpiresAtRangeError;

    fn try_from(unix_nanoseconds: i128) -> Result<Self, Self::Error> {
        Timestamp::from_nanosecond(unix_nanoseconds)
            .map(Self)
            .map_err(|_out_of_range| ExpiresAtRangeError)
    }
}

impl fmt::Display for ExpiresAt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl serde::Serialize for ExpiresAt {
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        serializer.serialize_i128(self.0.as_nanosecond())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("expires-at is not a valid RFC 3339 timestamp")]
pub struct ExpiresAtError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("expires-at nanoseconds lie outside the representable instant range")]
pub struct ExpiresAtRangeError;
