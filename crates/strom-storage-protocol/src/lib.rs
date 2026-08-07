//! Pure writer correctness protocol and resident forest for `StromDB`.

mod forest;
mod outcome;
mod writer;

pub use forest::{Applied, FoldContradiction, Forest, ForestContradiction, ForestDelta};
pub use outcome::{SealPublication, TypedStoreError, WalEstablishment};
pub use writer::{
    AdmissionRefusal, AuthoredClaim, CheckpointInput, CheckpointTicket, CollectionInput,
    CommandEnvelope, Completion, CreateStream, EffectKey, PreparationOutcome, PreparedCheckpoint,
    WRITER_OUTPUTS_PER_STEP_MAX, WriterAction, WriterEffect, WriterEvent, WriterExit,
    WriterMachine, WriterOutput, WriterStep,
};
