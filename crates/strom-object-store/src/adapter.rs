//! The one concrete adapter over the `object_store` backends.
//!
//! All foreign vocabulary is translated at this boundary (stromstyle §9); no
//! `object_store` type crosses the public seam.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    ClientOptions, GetOptions, GetRange, ObjectStore, ObjectStoreExt as _, PutMode, PutOptions,
    PutPayload, RetryConfig,
};

use crate::bytes::{ByteBound, ByteRange, Checksum, Etag, FrozenBytes};
use crate::error::{S3ConfigError, StoreContradiction, StoreError};
use crate::evidence::{CreateEvidence, ListPage, ListPageRequest, RawObject, VerifiedRangeBytes};
use crate::key::ObjectKey;

/// Explicit S3 client configuration (stromstyle §6: options are spelled out).
#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    /// Custom endpoint for S3-compatible stores such as `MinIO`.
    pub endpoint: Option<String>,
    /// Permit plain HTTP; only for local test endpoints.
    pub allow_http: bool,
    /// Explicit credentials; `None` falls back to the process environment.
    pub credentials: Option<S3Credentials>,
    pub request_timeout: Duration,
}

/// Static S3 credentials.
#[derive(Clone)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl fmt::Debug for S3Credentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .finish()
    }
}

/// The raw object-store adapter beneath the typed Seal, WAL, and content stores.
#[derive(Debug, Clone)]
pub struct ObjectStoreAdapter {
    store: Arc<dyn ObjectStore>,
}

impl ObjectStoreAdapter {
    /// An S3 backend with retries disabled.
    ///
    /// Retries stay off because a transparently resent create can win on the
    /// wire and then observe its own bytes as an occupant, which would turn a
    /// `Direct` win into weaker evidence. Callers own every retry decision.
    ///
    /// # Errors
    ///
    /// Returns [`S3ConfigError`] when the client cannot be built from the
    /// given configuration.
    pub fn s3(config: S3Config) -> Result<Self, S3ConfigError> {
        let retry = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let client = ClientOptions::new()
            .with_timeout(config.request_timeout)
            .with_allow_http(config.allow_http);
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(config.bucket)
            .with_region(config.region)
            .with_retry(retry)
            .with_client_options(client);
        if let Some(endpoint) = config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(credentials) = config.credentials {
            builder = builder
                .with_access_key_id(credentials.access_key_id)
                .with_secret_access_key(credentials.secret_access_key);
        }
        let store = builder.build().map_err(|source| S3ConfigError {
            detail: source.to_string(),
        })?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    /// A deterministic in-memory backend for tests above this seam.
    #[must_use]
    pub fn in_memory() -> Self {
        Self {
            store: Arc::new(InMemory::new()),
        }
    }

    /// Send one create-if-absent exactly once and normalize the result.
    ///
    /// An occupied coordinate is reconciled here with one bounded read and a
    /// byte compare against the frozen candidate. An ambiguous transport
    /// result returns [`CreateEvidence::Unresolved`]; the caller owns any
    /// further reconciliation and this adapter never resends.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Rejected`] when the backend refuses the request
    /// definitively. Every other outcome, including ambiguity, is evidence.
    pub async fn create_if_absent(
        &self,
        key: &ObjectKey,
        candidate: FrozenBytes,
    ) -> Result<CreateEvidence, StoreError> {
        let location = key.to_store_path();
        let payload = PutPayload::from(candidate.clone_body());
        let outcome = self
            .store
            .put_opts(&location, payload, PutOptions::from(PutMode::Create))
            .await;
        match outcome {
            Ok(_) => Ok(CreateEvidence::Direct),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(self.compare_occupant(&location, &candidate).await),
            Err(
                refusal @ (object_store::Error::PermissionDenied { .. }
                | object_store::Error::Unauthenticated { .. }),
            ) => Err(StoreError::Rejected {
                detail: refusal.to_string(),
            }),
            // The request may have reached the store; only evidence, never a
            // resend, may resolve it.
            Err(_) => Ok(CreateEvidence::Unresolved),
        }
    }

    /// Read one whole object, refusing bodies above the caller's bound.
    ///
    /// # Errors
    ///
    /// A body above `bytes_max` is a [`StoreError::Contradiction`]; transport
    /// trouble is [`StoreError::Retryable`]. Absence is `Ok(None)`.
    pub async fn read(
        &self,
        key: &ObjectKey,
        bytes_max: ByteBound,
    ) -> Result<Option<RawObject>, StoreError> {
        let location = key.to_store_path();
        match self.store.get_opts(&location, GetOptions::default()).await {
            Ok(observation) => {
                if observation.meta.size > bytes_max.get() {
                    return Err(StoreError::Contradiction(
                        StoreContradiction::OversizedObject {
                            key: key.clone(),
                            bytes_max: bytes_max.get(),
                            bytes_actual: observation.meta.size,
                        },
                    ));
                }
                let etag = require_etag(observation.meta.e_tag.clone())?;
                let body = observation
                    .bytes()
                    .await
                    .map_err(|source| map_operation_error(&source))?;
                Ok(Some(RawObject::new(body, etag)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(source) => Err(map_operation_error(&source)),
        }
    }

    /// Read one authenticated byte range and verify its checksum.
    ///
    /// # Errors
    ///
    /// A short read or a checksum mismatch is a [`StoreError::Contradiction`];
    /// transport trouble is [`StoreError::Retryable`]. Absence is `Ok(None)`.
    #[expect(
        clippy::missing_panics_doc,
        reason = "usize always fits in u64 on supported targets"
    )]
    pub async fn read_range(
        &self,
        key: &ObjectKey,
        range: ByteRange,
        expected: Checksum,
    ) -> Result<Option<VerifiedRangeBytes>, StoreError> {
        let location = key.to_store_path();
        let options = GetOptions {
            range: Some(GetRange::Bounded(range.start()..range.end_exclusive())),
            ..GetOptions::default()
        };
        match self.store.get_opts(&location, options).await {
            Ok(observation) => {
                let body = observation
                    .bytes()
                    .await
                    .map_err(|source| map_operation_error(&source))?;
                let bytes_actual =
                    u64::try_from(body.len()).expect("usize fits in u64 on supported platforms");
                if bytes_actual != range.length() {
                    return Err(StoreError::Contradiction(StoreContradiction::ShortRange {
                        key: key.clone(),
                        bytes_expected: range.length(),
                        bytes_actual,
                    }));
                }
                let actual = Checksum::compute(&body);
                if actual != expected {
                    return Err(StoreError::Contradiction(
                        StoreContradiction::ChecksumMismatch {
                            key: key.clone(),
                            expected,
                            actual,
                        },
                    ));
                }
                Ok(Some(VerifiedRangeBytes::new(body)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(source) => Err(map_operation_error(&source)),
        }
    }

    /// List one bounded page of keys in ascending lexicographic order.
    ///
    /// The trait beneath does not promise order, but both selected backends
    /// list lexicographically; a violation is surfaced as a contradiction
    /// rather than silently repaired, because pagination proofs depend on it.
    ///
    /// # Errors
    ///
    /// A noncanonical or out-of-order listed key is a
    /// [`StoreError::Contradiction`]; transport trouble is
    /// [`StoreError::Retryable`].
    pub async fn list_page(&self, request: ListPageRequest) -> Result<ListPage, StoreError> {
        let prefix = request.prefix.to_store_path();
        let mut listing = match &request.start_exclusive {
            Some(offset) => self
                .store
                .list_with_offset(Some(&prefix), &offset.to_store_path()),
            None => self.store.list(Some(&prefix)),
        };
        let keys_max = request.keys_max.get();
        let mut keys: Vec<ObjectKey> = Vec::with_capacity(keys_max);
        let mut truncated = false;
        while let Some(entry) = listing.next().await {
            let meta = entry.map_err(|source| map_operation_error(&source))?;
            let listed = parse_listed_key(&meta.location)?;
            let previous = keys.last().or(request.start_exclusive.as_ref());
            if let Some(previous) = previous
                && *previous >= listed
            {
                return Err(StoreError::Contradiction(
                    StoreContradiction::UnorderedList {
                        previous: previous.clone(),
                        listed,
                    },
                ));
            }
            if keys.len() == keys_max {
                truncated = true;
                break;
            }
            keys.push(listed);
        }
        let continuation = if truncated {
            keys.last().cloned()
        } else {
            None
        };
        Ok(ListPage::new(keys, continuation))
    }

    /// Delete one content object; absence already satisfies the contract.
    ///
    /// # Errors
    ///
    /// Transport trouble is [`StoreError::Retryable`]; a definitive backend
    /// refusal is [`StoreError::Rejected`].
    pub async fn delete_idempotent(&self, key: &ObjectKey) -> Result<(), StoreError> {
        let location = key.to_store_path();
        match self.store.delete(&location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(source) => Err(map_operation_error(&source)),
        }
    }

    /// One bounded read plus byte compare against an occupied coordinate.
    async fn compare_occupant(&self, location: &Path, candidate: &FrozenBytes) -> CreateEvidence {
        let candidate_size =
            u64::try_from(candidate.len()).expect("usize fits in u64 on supported platforms");
        match self.store.get_opts(location, GetOptions::default()).await {
            Ok(observation) => {
                if observation.meta.size != candidate_size {
                    return CreateEvidence::NotOurs;
                }
                match observation.bytes().await {
                    Ok(occupant) if occupant.as_ref() == candidate.as_slice() => {
                        CreateEvidence::DurableMatch
                    }
                    Ok(_) => CreateEvidence::NotOurs,
                    Err(_) => CreateEvidence::Unresolved,
                }
            }
            // The occupant that fenced the create is already gone or
            // unreadable; no evidence either way remains.
            Err(_) => CreateEvidence::Unresolved,
        }
    }
}

fn require_etag(observed: Option<String>) -> Result<Etag, StoreError> {
    let raw = observed.ok_or_else(|| StoreError::Rejected {
        detail: "backend returned no etag for an observed object".to_owned(),
    })?;
    Etag::try_from(raw).map_err(|empty| StoreError::Rejected {
        detail: empty.to_string(),
    })
}

fn parse_listed_key(location: &Path) -> Result<ObjectKey, StoreError> {
    ObjectKey::try_from(location.as_ref()).map_err(|source| {
        StoreError::Contradiction(StoreContradiction::ForeignKey {
            listed: location.as_ref().to_owned(),
            detail: source.to_string(),
        })
    })
}

fn map_operation_error(source: &object_store::Error) -> StoreError {
    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "foreign non_exhaustive enum; unknown variants default to the retryable class"
    )]
    match source {
        object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. }
        | object_store::Error::NotSupported { .. }
        | object_store::Error::NotImplemented { .. }
        | object_store::Error::InvalidPath { .. }
        | object_store::Error::UnknownConfigurationKey { .. } => StoreError::Rejected {
            detail: source.to_string(),
        },
        _ => StoreError::Retryable {
            detail: source.to_string(),
        },
    }
}
