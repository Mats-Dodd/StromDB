//! Pure bootstrap and writer correctness protocol and resident forest for `StromDB`.

mod bootstrap;
mod forest;
mod outcome;
mod writer;

pub use bootstrap::{
    BootstrapEffect, BootstrapEvent, BootstrapExit, BootstrapMachine, BootstrapStep,
};
pub use forest::{Applied, FoldContradiction, Forest, ForestContradiction, ForestDelta};
pub use outcome::{GenesisEstablishment, SealPublication, TypedStoreError, WalEstablishment};
pub use writer::{
    AdmissionRefusal, CheckpointInput, CheckpointTicket, CollectionInput, CommandEnvelope,
    Completion, CreateStream, EffectKey, PreparationOutcome, PreparedCheckpoint,
    WRITER_OUTPUTS_PER_STEP_MAX, WriterAction, WriterEffect, WriterEvent, WriterExit,
    WriterMachine, WriterOutput, WriterRecovery, WriterStep,
};
