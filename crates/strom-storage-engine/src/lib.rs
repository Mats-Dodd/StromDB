//! The `StromDB` storage engine: writer, bootstrap, admission, forest, and
//! typed stores. The `strom-db` crate is the public embeddable interface.

mod admission;
mod bootstrap;
mod checkpoint;
mod collection;
mod engine;
mod forest;
mod store;
mod writer;

pub use engine::{CloseOutcome, Engine, OpenError, StreamError};
pub use strom_storage_domain::PartitionId;

pub(crate) use forest::{Applied, FoldContradiction, Forest};

#[cfg(test)]
fn test_entropy() -> strom_common::Entropy {
    const TEST_SEED: u64 = 42;
    strom_common::Entropy::from_seed(strom_common::Seed::from(TEST_SEED))
}
