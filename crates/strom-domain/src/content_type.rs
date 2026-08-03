//! MIME content type fixed at stream creation.

use std::fmt;
use std::str::FromStr;

/// Stream content type in canonical form (`type/subtype` plus optional `charset`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamContentType {
    essence: String,
    charset: Option<String>,
}

/// Upper bound on a content-type string, in bytes.
pub const CONTENT_TYPE_BYTES_MAX: usize = 256;

impl StreamContentType {
    /// Default when stream creation omits `Content-Type` (§5.1).
    #[must_use]
    pub fn octet_stream() -> Self {
        Self {
            essence: String::from("application/octet-stream"),
            charset: None,
        }
    }

    /// True for `application/json` (JSON-mode message boundaries, §9.1).
    #[must_use]
    pub fn is_json(&self) -> bool {
        self.essence == "application/json"
    }

    /// True for any `text/*` type (SSE text encoding, §5.8).
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

fn is_token(input: &str) -> bool {
    !input.is_empty() && input.chars().all(is_token_char)
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentTypeError {
    OverMaxBytes,
    Malformed,
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
