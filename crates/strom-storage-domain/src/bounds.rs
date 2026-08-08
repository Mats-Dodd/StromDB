//! Named bounds for storage-domain parsing and encoding.

/// Largest complete Seal archive accepted or produced.
pub const SEAL_ENCODED_BYTES_MAX: usize = 1024 * 1024;

/// Largest complete WAL archive accepted or produced.
pub const WAL_ENCODED_BYTES_MAX: usize = 4 * 1024 * 1024;

/// Most operation facts one WAL run may carry.
pub const WAL_RUN_FACTS_MAX: usize = 4096;

/// Worst-case fixed archive framing for one WAL RUN.
pub const WAL_RUN_FIXED_ENCODED_BYTES_MAX: usize = 128;

/// Worst-case encoded contribution of one fact excluding variable strings.
pub const WAL_FACT_ENCODED_FIXED_BYTES_MAX: usize = 128;

/// Worst-case complete estimated contribution of one fact to a WAL RUN.
pub const WAL_FACT_ENCODED_BYTES_ESTIMATE_MAX: usize = WAL_FACT_ENCODED_FIXED_BYTES_MAX
    + strom_domain::STREAM_PATH_BYTES_MAX
    + strom_domain::CONTENT_TYPE_BYTES_MAX;

/// Most commands retained in the writer's pending durability barrier.
pub const WRITER_PENDING_COMMANDS_MAX: usize = 4096;

const _: () = assert!(
    WRITER_PENDING_COMMANDS_MAX <= WAL_RUN_FACTS_MAX,
    "every pending barrier must remain inside the WAL fact-count bound"
);

const _: () = assert!(
    WAL_RUN_FIXED_ENCODED_BYTES_MAX
        + WRITER_PENDING_COMMANDS_MAX * WAL_FACT_ENCODED_BYTES_ESTIMATE_MAX
        <= WAL_ENCODED_BYTES_MAX,
    "the pending-command and field bounds must fit one encoded WAL object"
);

/// Most sorted runs carried by one tree manifest.
pub const TREE_RUNS_MAX: usize = 16;

/// Most tables carried by one sorted run.
pub const RUN_TABLES_MAX: usize = 4096;

/// Largest complete SST object accepted or produced.
pub const SST_OBJECT_BYTES_MAX: u64 = 128 * 1024 * 1024;

/// Largest estimated SST emitted by one checkpoint table.
pub const SST_TABLE_TARGET_BYTES: u64 = 64 * 1024 * 1024;

/// Worst-case fixed archive framing and table identity bytes.
pub const SST_ARCHIVE_FIXED_BYTES_MAX: u64 = 256;

/// Worst-case encoded contribution of one Directory row, including alignment.
pub const DIRECTORY_ROW_ENCODED_BYTES_MAX: u64 = 1_024;

/// Worst-case encoded contribution of one Ledger value row, including alignment.
pub const LEDGER_VALUE_ROW_ENCODED_BYTES_MAX: u64 = 2_048;

/// Worst-case encoded contribution of one Ledger delete row, including alignment.
pub const LEDGER_DELETE_ROW_ENCODED_BYTES_MAX: u64 = 64;

/// Same SST object bound for rkyv gates that take `usize`.
pub(crate) const SST_OBJECT_BYTES_MAX_USIZE: usize = 128 * 1024 * 1024;

const _: () = assert!(
    SST_OBJECT_BYTES_MAX == 128 * 1024 * 1024 && SST_OBJECT_BYTES_MAX_USIZE == 128 * 1024 * 1024,
    "the rkyv byte gate and durable SST bound must agree"
);

const _: () = assert!(
    SST_TABLE_TARGET_BYTES < SST_OBJECT_BYTES_MAX,
    "the checkpoint table target must leave room below the hard SST bound"
);

const _: () = assert!(
    SST_ARCHIVE_FIXED_BYTES_MAX
        + PARTITION_PATH_OCCUPANCIES_MAX_V2
            * (DIRECTORY_ROW_ENCODED_BYTES_MAX + LEDGER_VALUE_ROW_ENCODED_BYTES_MAX)
        <= PARTITION_BOOTSTRAP_BYTES_MAX_V2,
    "a complete maximum-capacity Directory and Ledger base must fit the V2 bootstrap byte bound"
);

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

/// Suffix span that starts a checkpoint while one is not already in flight.
pub const WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER: u64 = 512;

/// Most commands waiting to be considered by the single writer.
pub const WRITER_INGRESS_COMMANDS_MAX: usize = 1024;

const _: () = assert!(
    WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER * 2 == WAL_SUFFIX_COORDINATES_MAX_V2,
    "the checkpoint trigger is half the V2 suffix coordinate budget"
);

/// Worst-case resident logical bytes charged for one Directory row.
pub const DIRECTORY_ROW_LOGICAL_BYTES_MAX: u64 = 525;

/// Worst-case resident logical bytes charged for one Ledger value row.
pub const LEDGER_VALUE_ROW_LOGICAL_BYTES_MAX: u64 = 1_035;

/// Resident logical bytes charged for a Ledger delete before newest-wins merge.
pub const LEDGER_DELETE_ROW_LOGICAL_BYTES: u64 = 9;

const _: () = assert!(
    PARTITION_PATH_OCCUPANCIES_MAX_V2
        * (DIRECTORY_ROW_LOGICAL_BYTES_MAX + LEDGER_VALUE_ROW_LOGICAL_BYTES_MAX)
        < PARTITION_RESIDENT_LOGICAL_BYTES_MAX_V2,
    "ten million worst-case live rows must fit the V2 resident logical-byte bound"
);
