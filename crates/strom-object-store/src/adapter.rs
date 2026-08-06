//! The one concrete adapter over an injected `object_store` backend.
//!
//! The backend trait object enters through [`ObjectStoreAdapter::new`]. All
//! other foreign vocabulary is translated at this boundary (stromstyle §9);
//! no other `object_store` type crosses the public seam.

use std::sync::Arc;

use futures::StreamExt as _;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore, ObjectStoreExt as _, PutMode, PutOptions, PutPayload};

use crate::bytes::{ByteBound, Etag, FrozenBytes};
use crate::error::{StoreContradiction, StoreError};
use crate::evidence::{CreateEvidence, ListPage, ListPageRequest, RawObject};
use crate::key::ObjectKey;

/// The raw object-store adapter beneath the typed Seal, WAL, and content stores.
#[derive(Debug, Clone)]
pub struct ObjectStoreAdapter {
    store: Arc<dyn ObjectStore>,
}

impl ObjectStoreAdapter {
    /// Wrap one injected `object_store` backend.
    ///
    /// The backend must not transparently resend a create after an ambiguous
    /// result. A resent create can win on the wire and then observe its own
    /// bytes as an occupant, which would turn a `Direct` win into weaker
    /// evidence. Callers own every retry decision, so configure the injected
    /// store with transport retries disabled.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
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
                consume_bounded_object(key, bytes_max, observation)
                    .await
                    .map(Some)
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

async fn consume_bounded_object(
    key: &ObjectKey,
    bytes_max: ByteBound,
    observation: object_store::GetResult,
) -> Result<RawObject, StoreError> {
    let etag = require_etag(observation.meta.e_tag.clone())?;
    let mut body = bytes::BytesMut::new();
    let mut stream = observation.into_stream();
    let mut bytes_observed = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|source| map_operation_error(&source))?;
        let chunk_bytes = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        let bytes_remaining = bytes_max
            .get()
            .checked_sub(bytes_observed)
            .expect("the running byte count never exceeds the bound");
        if chunk_bytes > bytes_remaining {
            return Err(StoreError::Contradiction(
                StoreContradiction::OversizedObject {
                    key: key.clone(),
                    bytes_max: bytes_max.get(),
                    bytes_actual: bytes_max.get().saturating_add(1),
                },
            ));
        }
        bytes_observed = bytes_observed
            .checked_add(chunk_bytes)
            .expect("an accepted chunk fits inside the u64 byte bound");
        body.extend_from_slice(&chunk);
    }
    Ok(RawObject::new(body.freeze(), etag))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn body_bound_overrules_dishonest_in_bound_metadata() {
        let store = InMemory::new();
        let location = Path::from("dishonest");
        store
            .put(&location, PutPayload::from_static(b"six bytes"))
            .await
            .expect("test body stores");
        let mut observation = store.get(&location).await.expect("test body reads");
        observation.meta.size = 1;
        let key: ObjectKey = "dishonest".parse().expect("test key is canonical");
        let bound = ByteBound::try_from(5).expect("test bound is nonzero");

        let outcome = consume_bounded_object(&key, bound, observation).await;

        assert!(
            matches!(
                outcome,
                Err(StoreError::Contradiction(
                    StoreContradiction::OversizedObject {
                        bytes_max: 5,
                        bytes_actual: 6,
                        ..
                    }
                ))
            ),
            "the stream stops at the first byte beyond the bound, got {outcome:?}"
        );
    }
}
