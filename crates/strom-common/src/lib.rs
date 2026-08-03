//! Deterministic capability seams shared by `StromDB` crates (stromstyle §4).

pub mod randomness;
pub mod wall_clock;

pub use randomness::{Entropy, Seed};
pub use wall_clock::{Clock, ManualClock, OsClock, Timestamp};
