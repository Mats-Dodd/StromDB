//! Process configuration for the `strom-server` binary.

use std::net::SocketAddr;
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use object_store::prefix::PrefixStore;

// Path is relative to this crate's manifest directory (`crates/strom-server`).
// Isolated so secretspec_derive's serde/`std::env::var` expansion can carry an
// `#[expect]` (attributes on the macro invocation itself are ignored).
#[expect(
    clippy::unsafe_derive_deserialize,
    clippy::disallowed_methods,
    reason = "secretspec_derive expands to serde derives and SECRETSPEC_* env reads at the process boundary"
)]
mod generated_secrets {
    secretspec_derive::declare_secrets!("../../secretspec.toml");
}

/// Command-line and environment configuration for one server process.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "strom-server",
    about = "Durable Streams HTTP server over StromDB"
)]
pub struct ServerConfig {
    /// Socket address to bind.
    #[arg(long, env = "STROM_BIND", default_value = "127.0.0.1:4437")]
    pub bind: SocketAddr,

    /// Object-store backend.
    #[arg(long, env = "STROM_STORE", default_value = "memory")]
    pub store: StoreBackend,

    /// S3 bucket name (required when `--store s3`).
    #[arg(long, env = "STROM_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    /// Key prefix inside the bucket (required when `--store s3`).
    #[arg(long, env = "STROM_S3_PREFIX")]
    pub s3_prefix: Option<String>,

    /// S3 API endpoint (required when `--store s3`; use a `MinIO` URL locally).
    #[arg(long, env = "STROM_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    /// S3 region (required when `--store s3`).
    #[arg(long, env = "STROM_S3_REGION")]
    pub s3_region: Option<String>,
}

/// Which object-store adapter the process opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StoreBackend {
    Memory,
    S3,
}

/// Why process configuration could not build a store.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("`--store s3` requires --s3-bucket, --s3-prefix, --s3-endpoint, and --s3-region")]
    MissingS3Options,
    #[error("failed to build the S3 object store: {detail}")]
    S3Build { detail: String },
}

impl ServerConfig {
    /// Build the injected object store for this configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when S3 options are incomplete or the S3 client
    /// cannot be constructed.
    pub fn build_store(&self) -> Result<Arc<dyn ObjectStore>, ConfigError> {
        match self.store {
            StoreBackend::Memory => Ok(Arc::new(InMemory::new())),
            StoreBackend::S3 => self.build_s3_store(),
        }
    }

    fn build_s3_store(&self) -> Result<Arc<dyn ObjectStore>, ConfigError> {
        let (Some(bucket), Some(prefix), Some(endpoint), Some(region)) = (
            self.s3_bucket.as_deref(),
            self.s3_prefix.as_deref(),
            self.s3_endpoint.as_deref(),
            self.s3_region.as_deref(),
        ) else {
            return Err(ConfigError::MissingS3Options);
        };

        // Honors SECRETSPEC_PROVIDER / SECRETSPEC_PROFILE when unset on the builder.
        // Credentials stay in `loaded`; they are never exported into process env.
        let loaded = generated_secrets::SecretSpec::builder()
            .load()
            .map_err(|error| ConfigError::S3Build {
                detail: error.to_string(),
            })?;

        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region)
            .with_endpoint(endpoint)
            .with_access_key_id(&loaded.secrets.aws_access_key_id)
            .with_secret_access_key(&loaded.secrets.aws_secret_access_key);
        if endpoint.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
        let s3 = builder.build().map_err(|error| ConfigError::S3Build {
            detail: error.to_string(),
        })?;
        Ok(Arc::new(PrefixStore::new(s3, prefix.to_owned())))
    }
}
