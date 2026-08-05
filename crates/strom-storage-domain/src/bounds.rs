//! Named bounds for storage-domain parsing and encoding.

/// Largest complete Seal frame accepted or produced.
pub const SEAL_ENCODED_BYTES_MAX: usize = 1024 * 1024;

/// Largest complete WAL frame accepted or produced.
pub const WAL_ENCODED_BYTES_MAX: usize = 4 * 1024 * 1024;

/// Most operation facts one WAL run may carry.
pub const WAL_RUN_FACTS_MAX: usize = 4096;

/// Largest frameless ledger record accepted or produced.
pub const LEDGER_RECORD_BYTES_MAX: usize = 1024;
