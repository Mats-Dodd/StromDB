//! The `StromDB` storage engine: writer, bootstrap, admission, forest, and
//! typed stores. The `stromdb` crate is the public embeddable interface.

mod admission;
mod bootstrap;
mod checkpoint;
mod engine;
mod forest;
mod store;
mod writer;

pub use admission::{AdmissionRefusal, StreamCommand, StreamReply};
pub use bootstrap::BootstrapExit;
pub use engine::{CommandError, Engine, PublishedView};
pub use writer::WriterExit;

pub(crate) use forest::{Applied, FoldContradiction, Forest};

#[cfg(test)]
fn test_entropy() -> strom_common::Entropy {
    const TEST_SEED: u64 = 42;
    strom_common::Entropy::from_seed(strom_common::Seed::from(TEST_SEED))
}
