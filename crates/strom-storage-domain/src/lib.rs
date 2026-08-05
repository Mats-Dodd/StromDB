//! Pure storage vocabulary and durable codecs for `StromDB`.

mod bounds;
mod coordinate;
mod envelope;
mod ledger;
mod owner;
mod partition;
mod seal;
mod spelling;
mod sst;
#[cfg(feature = "proptest")]
pub mod strategy;
mod stream_uid;
mod wal;
mod wire;

pub use bounds::{
    DIRECTORY_KEY_BYTES_MAX, DIRECTORY_ROW_LOGICAL_BYTES_MAX, LEDGER_DELETE_ROW_LOGICAL_BYTES,
    LEDGER_VALUE_ROW_LOGICAL_BYTES_MAX, PARTITION_BOOTSTRAP_BYTES_MAX_V2,
    PARTITION_BOOTSTRAP_OBJECTS_MAX_V2, PARTITION_PATH_OCCUPANCIES_MAX_V2,
    PARTITION_RESIDENT_LOGICAL_BYTES_MAX_V2, RUN_TABLES_MAX, SEAL_ENCODED_BYTES_MAX,
    SST_OBJECT_BYTES_MAX, STREAM_RECORD_BYTES_MAX, TREE_RANGES_MAX_V2, TREE_RUNS_MAX,
    WAL_ENCODED_BYTES_MAX, WAL_RUN_FACTS_MAX, WAL_SUFFIX_COORDINATES_MAX_V2,
};
pub use coordinate::{CoordinateExhausted, ZeroCoordinate};
pub use envelope::{DecodeError, EncodeError};
pub use ledger::{
    DirectoryEntry, DirectoryKey, DirectoryKeyError, LedgerCell, StreamRecord,
    decode_stream_record, encode_stream_record,
};
pub use owner::OwnerToken;
pub use partition::{PartitionId, PartitionIdError};
pub use seal::{
    AttemptId, FreshIdentity, KeyBound, RangeVersion, Seal, SealError, SealFormat, SealGeneration,
    SealIdentity, SortedRun, StoreKind, TableObjectId, TableRef, TreeVersion, WalReplayPoint,
    decode_seal, encode_seal,
};
pub use spelling::{KeySpellingError, SealKey, TableKey, WalKey};
pub use sst::{
    RowCodec, RowField, SST_HEADER_BYTES, SstDecodeError, SstEncodeError, SstHeader,
    decode_directory_sst, decode_ledger_sst, decode_sst_header, encode_directory_sst,
    encode_ledger_sst, encode_sst_header,
};
pub use stream_uid::StreamUid;
pub use wal::{
    BatchId, BoundedNonEmptyVec, BoundedNonEmptyVecError, OperationFact, WalFence, WalIdentity,
    WalObject, WalRun, decode_wal, encode_wal,
};
