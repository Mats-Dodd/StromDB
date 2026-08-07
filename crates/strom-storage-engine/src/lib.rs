//! The `StromDB` storage engine: effect interpreters and typed stores. The
//! `strom-db` crate is the public embeddable interface.

mod bootstrap;
mod checkpoint;
mod engine;
mod store;
mod writer;

pub use engine::{CloseOutcome, Engine, OpenError, StreamError};
pub use strom_storage_domain::{PartitionId, SealGeneration};
