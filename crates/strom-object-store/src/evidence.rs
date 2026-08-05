//! Operation results the engine treats as evidence.

use std::num::NonZeroUsize;

use crate::bounds::LIST_KEYS_MAX;
use crate::bytes::Etag;
use crate::key::ObjectKey;

/// The normalized outcome of one conditional create.
///
/// `Direct` is stronger than byte equality: it means this request received
/// the winning response. The adapter never manufactures it by resending after
/// an ambiguous response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateEvidence {
    /// This request received the winning response.
    Direct,
    /// The exact candidate bytes exist; their author is unknown.
    DurableMatch,
    /// Different bytes occupy the immutable coordinate.
    NotOurs,
    /// The request may still take effect. The caller owns reconciliation.
    Unresolved,
}

/// One whole object observed by a bounded read.
#[derive(Debug, Clone)]
pub struct RawObject {
    body: bytes::Bytes,
    etag: Etag,
}

impl RawObject {
    pub(crate) const fn new(body: bytes::Bytes, etag: Etag) -> Self {
        Self { body, etag }
    }

    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    #[must_use]
    pub const fn etag(&self) -> &Etag {
        &self.etag
    }
}

/// Range bytes whose checksum matched the caller's expectation.
#[derive(Debug, Clone)]
pub struct VerifiedRangeBytes(bytes::Bytes);

impl VerifiedRangeBytes {
    pub(crate) const fn new(body: bytes::Bytes) -> Self {
        Self(body)
    }

    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.0
    }
}

/// One bounded request for one lexicographically ordered list page.
#[derive(Debug, Clone)]
pub struct ListPageRequest {
    pub prefix: ObjectKey,
    /// Exclusive continuation point; keys at or before it are not returned.
    pub start_exclusive: Option<ObjectKey>,
    pub keys_max: KeysBound,
}

/// A caller-imposed ceiling on how many keys one list page may surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeysBound(NonZeroUsize);

impl KeysBound {
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl TryFrom<usize> for KeysBound {
    type Error = KeysBoundError;

    fn try_from(raw: usize) -> Result<Self, Self::Error> {
        let bound = NonZeroUsize::new(raw).ok_or(KeysBoundError::Zero)?;
        if bound.get() > LIST_KEYS_MAX {
            return Err(KeysBoundError::OverListBound {
                keys_actual: bound.get(),
            });
        }
        Ok(Self(bound))
    }
}

/// Why a raw count is not a legal list page bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum KeysBoundError {
    #[error("keys bound is zero")]
    Zero,
    #[error("keys bound is {keys_actual}; the bound is {LIST_KEYS_MAX}")]
    OverListBound { keys_actual: usize },
}

/// One ordered, bounded list page.
#[derive(Debug, Clone)]
pub struct ListPage {
    keys: Vec<ObjectKey>,
    continuation: Option<ObjectKey>,
}

impl ListPage {
    pub(crate) const fn new(keys: Vec<ObjectKey>, continuation: Option<ObjectKey>) -> Self {
        Self { keys, continuation }
    }

    /// Keys in ascending lexicographic order.
    #[must_use]
    pub fn keys(&self) -> &[ObjectKey] {
        &self.keys
    }

    /// The exclusive start of the next page, when more keys exist.
    #[must_use]
    pub const fn continuation(&self) -> Option<&ObjectKey> {
        self.continuation.as_ref()
    }

    #[must_use]
    pub fn into_keys(self) -> Vec<ObjectKey> {
        self.keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_bound_rejects_zero_and_over_page_counts() {
        assert_eq!(
            Err(KeysBoundError::Zero),
            KeysBound::try_from(0),
            "zero bound is illegal"
        );
        assert_eq!(
            Err(KeysBoundError::OverListBound {
                keys_actual: LIST_KEYS_MAX + 1
            }),
            KeysBound::try_from(LIST_KEYS_MAX + 1),
            "a bound above the page limit is illegal"
        );
        assert!(
            KeysBound::try_from(LIST_KEYS_MAX).is_ok(),
            "the page limit itself is legal"
        );
    }
}
