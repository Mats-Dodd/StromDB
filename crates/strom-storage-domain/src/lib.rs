//! Pure storage vocabulary and durable codecs for `StromDB`.

mod archive;
mod bounds;
mod coordinate;
mod directory;
mod ledger;
mod owner;
mod partition;
mod seal;
mod spelling;
mod sst;
#[cfg(feature = "proptest")]
pub mod strategy;
mod table;
mod wal;

pub use archive::{DecodeError, EncodeError};
pub use bounds::{
    DIRECTORY_KEY_BYTES_MAX, DIRECTORY_ROW_ENCODED_BYTES_MAX, DIRECTORY_ROW_LOGICAL_BYTES_MAX,
    LEDGER_DELETE_ROW_ENCODED_BYTES_MAX, LEDGER_DELETE_ROW_LOGICAL_BYTES,
    LEDGER_VALUE_ROW_ENCODED_BYTES_MAX, LEDGER_VALUE_ROW_LOGICAL_BYTES_MAX,
    PARTITION_BOOTSTRAP_BYTES_MAX_V2, PARTITION_BOOTSTRAP_OBJECTS_MAX_V2,
    PARTITION_PATH_OCCUPANCIES_MAX_V2, PARTITION_RESIDENT_LOGICAL_BYTES_MAX_V2, RUN_TABLES_MAX,
    SEAL_ENCODED_BYTES_MAX, SST_ARCHIVE_FIXED_BYTES_MAX, SST_OBJECT_BYTES_MAX,
    SST_TABLE_TARGET_BYTES, TREE_RUNS_MAX, WAL_ENCODED_BYTES_MAX, WAL_RUN_FACTS_MAX,
    WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER, WAL_SUFFIX_COORDINATES_MAX_V2, WRITER_INGRESS_COMMANDS_MAX,
};
pub use coordinate::{BatchId, CoordinateExhausted, SealGeneration, StreamUid, ZeroCoordinate};
pub use directory::{DirectoryEntry, DirectoryKey, DirectoryKeyError};
pub use ledger::{LedgerCell, StreamRecord};
pub use owner::OwnerToken;
pub use partition::{PartitionId, PartitionIdError};
pub use seal::{
    Seal, SealError, SortedRun, TableRef, TreeVersion, WalReplayPoint, decode_seal, encode_seal,
};
pub use spelling::{KeySpellingError, SealKey, SealNamespace, TableKey, WalKey, WalNamespace};
pub use sst::{
    SstDecodeError, SstEncodeError, decode_directory_sst, decode_ledger_sst, encode_directory_sst,
    encode_ledger_sst,
};
pub use table::{AttemptId, FreshIdentity, StoreKind, TableIdentityError, TableObjectId};
pub use wal::{OperationFact, WalBody, WalFacts, WalFactsError, WalObject, decode_wal, encode_wal};
