//! Pure storage vocabulary and durable codecs for `StromDB`.

mod bounds;
mod coordinate;
mod envelope;
mod ledger;
mod owner;
mod partition;
mod seal;
mod spelling;
#[cfg(feature = "proptest")]
pub mod strategy;
mod stream_uid;
mod wal;
mod wire;

pub use bounds::{
    LEDGER_RECORD_BYTES_MAX, SEAL_ENCODED_BYTES_MAX, WAL_ENCODED_BYTES_MAX, WAL_RUN_FACTS_MAX,
};
pub use coordinate::{CoordinateExhausted, ZeroCoordinate};
pub use envelope::{DecodeError, EncodeError};
pub use ledger::{
    LedgerKey, LedgerRecord, PathTombstone, StreamRecord, decode_ledger_record,
    encode_ledger_record,
};
pub use owner::OwnerToken;
pub use partition::{PartitionId, PartitionIdError};
pub use seal::{
    Seal, SealFormat, SealGeneration, SealIdentity, TreeVersion, WalReplayPoint, decode_seal,
    encode_seal,
};
pub use spelling::{KeySpellingError, SealKey, WalKey};
pub use stream_uid::StreamUid;
pub use wal::{
    BatchId, BoundedNonEmptyVec, BoundedNonEmptyVecError, OperationFact, WalFence, WalIdentity,
    WalObject, WalRun, decode_wal, encode_wal,
};
