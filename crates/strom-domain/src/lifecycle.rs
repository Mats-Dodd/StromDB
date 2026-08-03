//! Stream lifecycle state: open or closed.

/// Whether a stream still accepts appends (§4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamLifecycle {
    Open,
    Closed,
}

impl StreamLifecycle {
    /// Close the stream. Idempotent (§4.1).
    #[must_use = "close returns the new lifecycle; the old value is consumed"]
    pub const fn close(self) -> Self {
        match self {
            Self::Open | Self::Closed => Self::Closed,
        }
    }

    #[must_use]
    pub const fn is_closed(self) -> bool {
        matches!(self, Self::Closed)
    }
}

impl serde::Serialize for StreamLifecycle {
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        const ENUM: &str = "StreamLifecycle";
        match self {
            Self::Open => serializer.serialize_unit_variant(ENUM, 0u32, "Open"),
            Self::Closed => serializer.serialize_unit_variant(ENUM, 1u32, "Closed"),
        }
    }
}
