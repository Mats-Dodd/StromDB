//! The `StromDB` database engine.

/// Returns the `StromDB` crate version.
#[must_use]
pub const fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    #[test]
    fn reports_package_version() {
        assert_eq!(super::version(), env!("CARGO_PKG_VERSION"));
    }
}
