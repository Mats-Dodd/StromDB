//! Named bounds for storage-domain parsing and encoding.

/// Largest complete Seal frame accepted or produced.
pub const SEAL_ENCODED_BYTES_MAX: usize = 1024 * 1024;

/// Largest complete WAL frame accepted or produced.
pub const WAL_ENCODED_BYTES_MAX: usize = 4 * 1024 * 1024;

/// Most operation facts one WAL run may carry.
pub const WAL_RUN_FACTS_MAX: usize = 4096;

/// Most ranges carried by one V2 tree manifest.
pub const TREE_RANGES_MAX_V2: usize = 1;

/// Most sorted runs carried by one tree manifest.
pub const TREE_RUNS_MAX: usize = 16;

/// Most tables carried by one sorted run.
pub const RUN_TABLES_MAX: usize = 4096;

/// Largest complete SST object accepted or produced.
pub const SST_OBJECT_BYTES_MAX: u64 = 128 * 1024 * 1024;

/// Largest canonical Directory key.
pub const DIRECTORY_KEY_BYTES_MAX: usize = 512;

/// Largest frameless stream record accepted or produced.
pub const STREAM_RECORD_BYTES_MAX: usize = 1024;

/// Most lifetime path occupancies in one V2 partition.
pub const PARTITION_PATH_OCCUPANCIES_MAX_V2: u64 = 10_000_000;

/// Largest merged resident Directory and Ledger logical-byte account.
pub const PARTITION_RESIDENT_LOGICAL_BYTES_MAX_V2: u64 = 16 * 1024 * 1024 * 1024;

/// Most Directory and Ledger table objects selected for V2 bootstrap.
pub const PARTITION_BOOTSTRAP_OBJECTS_MAX_V2: usize = 1024;

/// Largest sum of selected Directory and Ledger table object lengths.
pub const PARTITION_BOOTSTRAP_BYTES_MAX_V2: u64 = 32 * 1024 * 1024 * 1024;

/// Largest inclusive replay-coordinate span from a V2 Seal through its FENCE.
pub const WAL_SUFFIX_COORDINATES_MAX_V2: u64 = 1024;

/// Worst-case resident logical bytes charged for one Directory row.
pub const DIRECTORY_ROW_LOGICAL_BYTES_MAX: u64 = 525;

/// Worst-case resident logical bytes charged for one Ledger value row.
pub const LEDGER_VALUE_ROW_LOGICAL_BYTES_MAX: u64 = 1_035;

/// Resident logical bytes charged for a Ledger delete before newest-wins merge.
pub const LEDGER_DELETE_ROW_LOGICAL_BYTES: u64 = 9;

const _: () = assert!(
    DIRECTORY_KEY_BYTES_MAX == strom_domain::STREAM_ID_BYTES_MAX,
    "Directory keys and protocol stream identifiers share one path bound"
);

const _: () = assert!(
    DIRECTORY_KEY_BYTES_MAX == 512
        && STREAM_RECORD_BYTES_MAX == 1_024
        && DIRECTORY_ROW_LOGICAL_BYTES_MAX == 13 + 512
        && LEDGER_VALUE_ROW_LOGICAL_BYTES_MAX == 11 + 1_024
        && LEDGER_DELETE_ROW_LOGICAL_BYTES == 9,
    "the V1 worst-row account must track every configured field bound"
);

const _: () = assert!(
    PARTITION_PATH_OCCUPANCIES_MAX_V2
        * (DIRECTORY_ROW_LOGICAL_BYTES_MAX + LEDGER_VALUE_ROW_LOGICAL_BYTES_MAX)
        < PARTITION_RESIDENT_LOGICAL_BYTES_MAX_V2,
    "ten million worst-case live rows must fit the V2 resident logical-byte bound"
);
