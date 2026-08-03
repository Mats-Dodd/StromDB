//! Stream lifecycle state: open or closed.

/// Whether a stream still accepts appends (protocol §4).
///
/// Streams start `Open`. Closure is durable and monotonic (§4.1): there is no
/// transition out of `Closed`, and this type provides none. `SoftDeleted` is
/// deliberately absent until fork support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamLifecycle {
    /// The stream accepts appends.
    Open,
    /// The stream rejects appends; its data remains readable (§4.1).
    Closed,
}

impl StreamLifecycle {
    /// Close the stream. Idempotent, like the protocol close operation
    /// (§4.1): closing an already-closed stream is success, not an error.
    #[must_use = "close returns the new lifecycle; the old value is consumed"]
    pub const fn close(self) -> Self {
        match self {
            Self::Open | Self::Closed => Self::Closed,
        }
    }

    /// True when the stream no longer accepts appends.
    #[must_use]
    pub const fn is_closed(self) -> bool {
        matches!(self, Self::Closed)
    }
}

/// Durable spelling: a serde enum with fixed variant indices — 0 `Open`,
/// 1 `Closed`. Written by hand so a variant reorder cannot silently change
/// the durable format.
impl serde::Serialize for StreamLifecycle {
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        /// The serde enum name shared by both variants of this impl.
        const ENUM: &str = "StreamLifecycle";
        match self {
            Self::Open => serializer.serialize_unit_variant(ENUM, 0u32, "Open"),
            Self::Closed => serializer.serialize_unit_variant(ENUM, 1u32, "Closed"),
        }
    }
}
