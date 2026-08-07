//! Deterministic failures and operation gates for tests above the adapter seam.

mod fault_store;
mod gate;

pub use fault_store::{
    BackendFailure, Fault, FaultStore, FaultStoreConfigError, FaultStoreVerificationError,
    Operation, Selection, Target,
};
pub use gate::Gate;
