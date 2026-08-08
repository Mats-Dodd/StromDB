//! Deterministic capability seams shared by `StromDB` crates (stromstyle §4).

pub mod monotonic_clock;
pub mod randomness;

pub use monotonic_clock::{
    ManualMonotonicClock, MonotonicClock, MonotonicInstant, OsMonotonicClock,
};
pub use randomness::{Entropy, Generator, Seed};
