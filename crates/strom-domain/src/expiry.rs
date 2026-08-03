//! Expiry configuration fixed at stream creation.

use std::fmt;
use std::num::NonZeroU64;
use std::str::FromStr;

use jiff::Timestamp;

/// How a stream expires. Fixed at creation (protocol §5.1); no protocol
/// operation mutates it afterwards.
///
/// `Stream-TTL` and `Stream-Expires-At` are mutually exclusive (§5.1). This
/// enum makes the combined state unrepresentable; construct the policy from
/// the two optional headers with `TryFrom`, which rejects the conflict.
///
/// The sliding-TTL countdown *deadline* is hot state — it moves on every read
/// and write — and deliberately does not live here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiryPolicy {
    /// The stream never expires.
    None,
    /// The stream expires after an idle window (`Stream-TTL`).
    SlidingTtl(StreamTtl),
    /// The stream expires at an absolute instant (`Stream-Expires-At`).
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

/// Durable spelling: a serde enum with fixed variant indices — 0 `None`,
/// 1 `SlidingTtl`, 2 `AbsoluteExpiry`. The indices are part of the durable
/// format; this impl is written by hand so a variant reorder cannot silently
/// change them.
impl serde::Serialize for ExpiryPolicy {
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        /// The serde enum name shared by every variant of this impl.
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

/// Both `Stream-TTL` and `Stream-Expires-At` were supplied. Protocol §5.1
/// tells servers to reject this with `400 Bad Request`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpiryPolicyConflict;

impl fmt::Display for ExpiryPolicyConflict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Stream-TTL and Stream-Expires-At are mutually exclusive")
    }
}

impl std::error::Error for ExpiryPolicyConflict {}

/// A sliding idle window in seconds (`Stream-TTL` header, protocol §5.1).
///
/// The protocol grammar is strict: decimal digits only, no leading zeros, no
/// sign, decimal point, or exponent. Two Courant restrictions on top of the
/// grammar: zero is rejected because a zero idle window is dead on arrival
/// and always indicates a client bug, and values must fit in `u64` seconds
/// (about 584 billion years, so the stream will most likely be subsumed by the suns heat death before we run into this limit).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamTtl(NonZeroU64);

impl StreamTtl {
    /// The idle window, in seconds.
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
        // `03600` is explicitly invalid (§5.1); `0` alone passes the grammar
        // and is handled as the zero case below.
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

/// Every nonzero second count is a valid idle window, so widening from
/// [`NonZeroU64`] is infallible. This is the construction path for decoders
/// that already hold a proven-nonzero count.
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

/// Durable spelling: the window as a serde `u64` of seconds.
impl serde::Serialize for StreamTtl {
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        serializer.serialize_u64(self.0.get())
    }
}

/// Why a string is not a valid [`StreamTtl`].
///
/// Every variant maps to `400 Bad Request` at the HTTP edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamTtlError {
    /// Not the strict decimal grammar of protocol §5.1: digits only, no
    /// leading zeros, no sign, decimal point, or exponent.
    Malformed,
    /// Zero seconds: valid protocol grammar, rejected by Courant.
    Zero,
    /// Exceeds the representable maximum of `u64::MAX` seconds.
    OverMax,
}

impl fmt::Display for StreamTtlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => formatter
                .write_str("stream TTL is not a plain decimal integer without leading zeros"),
            Self::Zero => formatter.write_str("stream TTL of zero seconds is not accepted"),
            Self::OverMax => formatter.write_str("stream TTL exceeds the representable maximum"),
        }
    }
}

impl std::error::Error for StreamTtlError {}

/// An absolute expiry instant (`Stream-Expires-At` header, RFC 3339,
/// protocol §5.1).
///
/// Comparison and equality are on the instant, not the source text:
/// `2030-01-01T01:00:00+01:00` equals `2030-01-01T00:00:00Z`. `Display`
/// prints the canonical UTC form. Parsing never reads a clock; whether an
/// instant lies in the past is a policy question for the shell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExpiresAt(Timestamp);

impl FromStr for ExpiresAt {
    type Err = ExpiresAtError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        // Reject RFC 9557 time-zone annotations such as `[Europe/Paris]`.
        // jiff would accept them, but checking that an annotation agrees with
        // its offset needs a time-zone database this crate deliberately
        // excludes, and the protocol asks for plain RFC 3339.
        if input.contains('[') {
            return Err(ExpiresAtError);
        }
        input
            .parse::<Timestamp>()
            .map(Self)
            .map_err(|_parse_detail| ExpiresAtError)
    }
}

/// The instant as a nanosecond count since the Unix epoch. This is the
/// durable representation: an integer has exactly one spelling, where an RFC
/// 3339 string has many. Not every `i128` is an instant, so the conversion
/// is fallible; jiff bounds timestamps to the years -9999 through 9999.
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

/// Durable spelling: the instant as a serde `i128` nanosecond count since
/// the Unix epoch, the exact inverse of [`ExpiresAt::try_from`] on `i128`.
impl serde::Serialize for ExpiresAt {
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        serializer.serialize_i128(self.0.as_nanosecond())
    }
}

/// The string is not a valid RFC 3339 instant. Maps to `400 Bad Request` at
/// the HTTP edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpiresAtError;

/// The nanosecond count lies outside the representable instant range.
/// Reaches callers only from durable bytes, so it signals a corrupt or
/// foreign record, never a client mistake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpiresAtRangeError;

impl fmt::Display for ExpiresAtRangeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("expires-at nanoseconds lie outside the representable instant range")
    }
}

impl std::error::Error for ExpiresAtRangeError {}

impl fmt::Display for ExpiresAtError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("expires-at is not a valid RFC 3339 timestamp")
    }
}

impl std::error::Error for ExpiresAtError {}
