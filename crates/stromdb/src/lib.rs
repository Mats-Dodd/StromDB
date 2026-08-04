//! The `StromDB` database engine.

/// Returns the `StromDB` crate version.
#[must_use]
pub const fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
