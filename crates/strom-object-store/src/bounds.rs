//! Named limits for every adapter operation (stromstyle §8: bounds are the design).

/// Longest accepted object key in bytes. Spec anchor: the S3 key length limit.
pub const KEY_BYTES_MAX: usize = 1024;

/// Most keys one list page may surface. Spec anchor: the S3 `MaxKeys` page limit.
pub const LIST_KEYS_MAX: usize = 1000;

/// Largest accepted object body in bytes. Spec anchor: the S3 single-request
/// `PutObject` limit of 5 GiB.
pub const PUT_BYTES_MAX: u64 = 5 * 1024 * 1024 * 1024;
