//! The MIME content type fixed at stream creation.

use std::fmt;
use std::str::FromStr;

/// The content type of a stream, fixed at creation (protocol §5.1).
///
/// Held in canonical form: the `type/subtype` essence and the optional
/// `charset` value are ASCII-lowercased with insignificant whitespace
/// removed. Equality on the canonical form is therefore exactly the
/// protocol's configuration match: idempotent `PUT` (`200` versus `409`,
/// §5.1) and append validation (§5.2) both ride on this `Eq`.
///
/// Version 1 accepts `charset` as the only parameter; widening later is
/// compatible, narrowing is not.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamContentType {
    /// Lowercased `type/subtype`, for example `application/json`.
    essence: String,
    /// Lowercased `charset` parameter value, when present.
    charset: Option<String>,
}

/// Upper bound on a content-type string, in bytes.
///
/// A Courant bound; the protocol sets none. Real media types fit in tens of
/// bytes.
pub const CONTENT_TYPE_BYTES_MAX: usize = 256;

impl StreamContentType {
    /// The protocol default `application/octet-stream`, used when stream
    /// creation omits `Content-Type` (protocol §5.1).
    #[must_use]
    pub fn octet_stream() -> Self {
        Self {
            essence: String::from("application/octet-stream"),
            charset: None,
        }
    }

    /// True for `application/json`: JSON-mode message-boundary semantics
    /// apply (protocol §9.1). The charset parameter does not affect this.
    #[must_use]
    pub fn is_json(&self) -> bool {
        self.essence == "application/json"
    }

    /// True for any `text/*` type. SSE carries `text/*` and JSON data events
    /// as UTF-8 text; everything else is base64-encoded (protocol §5.8).
    #[must_use]
    pub fn is_text(&self) -> bool {
        self.essence.starts_with("text/")
    }
}

impl FromStr for StreamContentType {
    type Err = ContentTypeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.len() > CONTENT_TYPE_BYTES_MAX {
            return Err(ContentTypeError::OverMaxBytes);
        }
        let mut parts = input.split(';');
        let essence_raw = parts.next().unwrap_or("").trim();
        let (type_raw, subtype_raw) = essence_raw
            .split_once('/')
            .ok_or(ContentTypeError::Malformed)?;
        if !is_token(type_raw) || !is_token(subtype_raw) {
            return Err(ContentTypeError::Malformed);
        }
        let mut charset = None;
        for parameter_raw in parts {
            let (name_raw, value_raw) = parameter_raw
                .trim()
                .split_once('=')
                .ok_or(ContentTypeError::Malformed)?;
            if !name_raw.trim().eq_ignore_ascii_case("charset") {
                return Err(ContentTypeError::UnsupportedParameter);
            }
            let value = value_raw.trim();
            if !is_token(value) || charset.is_some() {
                return Err(ContentTypeError::Malformed);
            }
            charset = Some(value.to_ascii_lowercase());
        }
        let essence = format!(
            "{}/{}",
            type_raw.to_ascii_lowercase(),
            subtype_raw.to_ascii_lowercase()
        );
        Ok(Self { essence, charset })
    }
}

impl fmt::Display for StreamContentType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.charset {
            Some(charset) => write!(formatter, "{}; charset={}", self.essence, charset),
            None => formatter.write_str(&self.essence),
        }
    }
}

impl serde::Serialize for StreamContentType {
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        serializer.collect_str(self)
    }
}

/// True when `input` is a non-empty HTTP `token` (RFC 9110 §5.6.2).
fn is_token(input: &str) -> bool {
    !input.is_empty() && input.chars().all(is_token_char)
}

/// True for the HTTP `tchar` set (RFC 9110 §5.6.2).
const fn is_token_char(character: char) -> bool {
    matches!(
        character,
        'a'..='z'
            | 'A'..='Z'
            | '0'..='9'
            | '!' | '#' | '$' | '%' | '&' | '\'' | '*' | '+' | '-' | '.'
            | '^' | '_' | '`' | '|' | '~'
    )
}

/// Why a string is not a valid [`StreamContentType`].
///
/// Every variant maps to `400 Bad Request` at the HTTP edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentTypeError {
    /// The string exceeds [`CONTENT_TYPE_BYTES_MAX`] bytes.
    OverMaxBytes,
    /// The string is not `type/subtype` with an optional single
    /// `charset=token` parameter.
    Malformed,
    /// A parameter other than `charset` was supplied.
    UnsupportedParameter,
}

impl fmt::Display for ContentTypeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OverMaxBytes => {
                write!(
                    formatter,
                    "content type exceeds {CONTENT_TYPE_BYTES_MAX} bytes"
                )
            }
            Self::Malformed => formatter.write_str(
                "content type is not `type/subtype` with an optional `charset=token` parameter",
            ),
            Self::UnsupportedParameter => {
                formatter.write_str("content type has a parameter other than `charset`")
            }
        }
    }
}

impl std::error::Error for ContentTypeError {}
