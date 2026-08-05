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
///
/// The protocol states no limit, so this is strom's own bound on work per
/// creation request (§10): generous for any real media type, small enough to
/// reject a hostile header.
pub const CONTENT_TYPE_BYTES_MAX: usize = 256;

const ESSENCE_OCTET_STREAM: &str = "application/octet-stream";
const ESSENCE_JSON: &str = "application/json";
const ESSENCE_TEXT_PREFIX: &str = "text/";

const _: () = assert!(
    ESSENCE_OCTET_STREAM.len() <= CONTENT_TYPE_BYTES_MAX,
    "the default content type must be a value this type can also parse back"
);

impl StreamContentType {
    /// Default when stream creation omits `Content-Type` (§5.1).
    #[must_use]
    pub fn octet_stream() -> Self {
        Self {
            essence: String::from(ESSENCE_OCTET_STREAM),
            charset: None,
        }
    }

    /// True for `application/json` (JSON-mode message boundaries, §9.1).
    #[must_use]
    pub fn is_json(&self) -> bool {
        self.essence == ESSENCE_JSON
    }

    /// True for any `text/*` type (SSE text encoding, §5.8).
    #[must_use]
    pub fn is_text(&self) -> bool {
        self.essence.starts_with(ESSENCE_TEXT_PREFIX)
    }
}

impl FromStr for StreamContentType {
    type Err = ContentTypeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.len() > CONTENT_TYPE_BYTES_MAX {
            return Err(ContentTypeError::OverMaxBytes);
        }
        let mut parts = input.split(';');
        let essence_raw = parts
            .next()
            .expect("str::split yields the whole input when the separator is absent")
            .trim();
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
        let parameter_bytes = charset.as_ref().map_or(0, |value| {
            "; charset="
                .len()
                .checked_add(value.len())
                .expect("two substrings from one bounded input cannot overflow usize")
        });
        let canonical_bytes = essence
            .len()
            .checked_add(parameter_bytes)
            .expect("substrings from one bounded input cannot overflow usize");
        if canonical_bytes > CONTENT_TYPE_BYTES_MAX {
            return Err(ContentTypeError::OverMaxBytes);
        }
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
