//! MIME content type fixed at stream creation.

use std::fmt;
use std::str::FromStr;

/// Stream content type in canonical form (`type/subtype` plus optional `charset`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamContentType(MediaType);

/// The two named media types cover almost every stream, so they live inline
/// and never touch the heap. Every other media type is boxed, which keeps the
/// hot struct one tag and one pointer wide. `FromStr` canonicalizes the named
/// charset-free spellings to their unit variants, so the derived equality and
/// hash stay structural.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum MediaType {
    OctetStream,
    Json,
    General(Box<GeneralMediaType>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GeneralMediaType {
    essence: Box<str>,
    charset: Option<Box<str>>,
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

const _: () = assert!(
    size_of::<StreamContentType>() == 16,
    "a content type is a hot ledger-row field and stays one tag and one pointer wide"
);

impl StreamContentType {
    /// Default when stream creation omits `Content-Type` (§5.1).
    #[must_use]
    pub const fn octet_stream() -> Self {
        Self(MediaType::OctetStream)
    }

    /// True for `application/json` (JSON-mode message boundaries, §9.1).
    #[must_use]
    pub fn is_json(&self) -> bool {
        match &self.0 {
            MediaType::Json => true,
            MediaType::OctetStream => false,
            MediaType::General(general) => &*general.essence == ESSENCE_JSON,
        }
    }

    /// True for any `text/*` type (SSE text encoding, §5.8).
    #[must_use]
    pub fn is_text(&self) -> bool {
        match &self.0 {
            MediaType::OctetStream | MediaType::Json => false,
            MediaType::General(general) => general.essence.starts_with(ESSENCE_TEXT_PREFIX),
        }
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
        let parameter_bytes = charset.as_ref().map_or(0, |value| {
            "; charset="
                .len()
                .checked_add(value.len())
                .expect("two substrings from one bounded input cannot overflow usize")
        });
        let canonical_bytes = essence_raw
            .len()
            .checked_add(parameter_bytes)
            .expect("substrings from one bounded input cannot overflow usize");
        if canonical_bytes > CONTENT_TYPE_BYTES_MAX {
            return Err(ContentTypeError::OverMaxBytes);
        }
        if charset.is_none() {
            if essence_raw.eq_ignore_ascii_case(ESSENCE_OCTET_STREAM) {
                return Ok(Self(MediaType::OctetStream));
            }
            if essence_raw.eq_ignore_ascii_case(ESSENCE_JSON) {
                return Ok(Self(MediaType::Json));
            }
        }
        let essence = format!(
            "{}/{}",
            type_raw.to_ascii_lowercase(),
            subtype_raw.to_ascii_lowercase()
        )
        .into_boxed_str();
        Ok(Self(MediaType::General(Box::new(GeneralMediaType {
            essence,
            charset: charset.map(String::into_boxed_str),
        }))))
    }
}

impl fmt::Display for StreamContentType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            MediaType::OctetStream => formatter.write_str(ESSENCE_OCTET_STREAM),
            MediaType::Json => formatter.write_str(ESSENCE_JSON),
            MediaType::General(general) => match &general.charset {
                Some(charset) => {
                    write!(formatter, "{}; charset={}", general.essence, charset)
                }
                None => formatter.write_str(&general.essence),
            },
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ContentTypeError {
    #[error("content type exceeds {CONTENT_TYPE_BYTES_MAX} bytes")]
    OverMaxBytes,
    #[error("content type is not `type/subtype` with an optional `charset=token` parameter")]
    Malformed,
    #[error("content type has a parameter other than `charset`")]
    UnsupportedParameter,
}
