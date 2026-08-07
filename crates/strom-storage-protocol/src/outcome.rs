//! Decided typed-store outcomes observed by the writer machine.

/// Decided outcome of establishing the canonical genesis Seal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenesisEstablishment {
    Established,
    LostRace,
    Unresolved,
}

/// Failures of typed store operations, shaped for protocol decisions.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TypedStoreError {
    #[error("retryable store failure: {detail}")]
    Retryable { detail: String },
    #[error("store rejected the request: {detail}")]
    Rejected { detail: String },
    #[error("store durable contradiction: {detail}")]
    Contradiction { detail: String },
}

/// Decided outcome of establishing one WAL candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalEstablishment {
    Durable,
    Occupied,
    UnresolvedAbsent,
}

/// Decided outcome of publishing one authority-bearing Seal candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealPublication {
    Authored,
    NoAuthority,
    Unresolved,
}
