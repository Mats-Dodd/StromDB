//! Typed Seal store: create-once candidates, newest-generation LIST, exact GET.

use std::str::FromStr as _;

use strom_object_store::{
    ByteBound, CreateEvidence, FrozenBytes, ListPageRequest, ObjectStoreAdapter,
};
use strom_storage_domain::{
    EncodeError, SEAL_ENCODED_BYTES_MAX, Seal, SealGeneration, SealKey, SealNamespace, decode_seal,
    encode_seal,
};

use super::{StoreErrorClass, map_store_error, newest_keys_bound, object_key};

/// One Seal candidate, encoded exactly once. Key and body agree by construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EncodedSeal {
    generation: SealGeneration,
    bytes: FrozenBytes,
}

impl EncodedSeal {
    /// Encode `seal` once and freeze the exact bytes that will be sent.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError`] when serialization fails or the archive exceeds
    /// [`SEAL_ENCODED_BYTES_MAX`].
    pub(crate) fn new(seal: &Seal) -> Result<Self, EncodeError> {
        let bytes = encode_seal(seal)?;
        Ok(Self::from_encoded(seal.generation(), bytes))
    }

    fn from_encoded(generation: SealGeneration, bytes: Vec<u8>) -> Self {
        let frozen = FrozenBytes::try_from(bytes)
            .expect("encode_seal yields a non-empty body within PUT_BYTES_MAX");
        Self {
            generation,
            bytes: frozen,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) const fn generation(&self) -> SealGeneration {
        self.generation
    }

    /// Exact frozen bytes of this candidate. After an ambiguous create, reconcile
    /// with one bounded GET compared against these bytes—never re-encode.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}

/// Failures of Seal store operations, shaped for the writer state machine.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SealStoreError {
    /// Transport trouble; a bounded retry of the same idempotent request is legal.
    #[error("retryable Seal store failure: {detail}")]
    Retryable { detail: String },
    /// The backend refused the request definitively; retrying cannot help.
    #[error("Seal store rejected the request: {detail}")]
    Rejected { detail: String },
    /// Durable bytes violate the storage model. The caller fails closed.
    #[error("Seal store durable contradiction: {detail}")]
    Contradiction { detail: String },
}

impl SealStoreError {
    fn from_store(error: strom_object_store::StoreError) -> Self {
        let (class, detail) = map_store_error(error);
        match class {
            StoreErrorClass::Retryable => Self::Retryable { detail },
            StoreErrorClass::Rejected => Self::Rejected { detail },
            StoreErrorClass::Contradiction => Self::Contradiction { detail },
        }
    }

    fn contradiction(detail: impl Into<String>) -> Self {
        Self::Contradiction {
            detail: detail.into(),
        }
    }
}

/// Typed Seal namespace over the raw object-store adapter.
#[derive(Debug, Clone)]
pub(crate) struct SealStore {
    adapter: ObjectStoreAdapter,
}

impl SealStore {
    #[must_use]
    pub(crate) const fn new(adapter: ObjectStoreAdapter) -> Self {
        Self { adapter }
    }

    /// Send the candidate exactly once; the evidence passes through unchanged.
    ///
    /// An authority-bearing create is send-once: the caller must not call
    /// create again for the same candidate. After
    /// [`CreateEvidence::Unresolved`], the caller reconciles with a bounded
    /// exact GET compared against [`EncodedSeal::as_slice`].
    ///
    /// # Errors
    ///
    /// Returns [`SealStoreError`] when the adapter reports a retryable,
    /// rejected, or contradictory outcome.
    pub(crate) async fn create_seal(
        &self,
        candidate: &EncodedSeal,
    ) -> Result<CreateEvidence, SealStoreError> {
        let key = object_key(SealKey::from(candidate.generation));
        self.adapter
            .create_if_absent(&key, candidate.bytes.clone())
            .await
            .map_err(SealStoreError::from_store)
    }

    /// Newest generation via one ascending list page with `keys_max = 1`.
    ///
    /// # Errors
    ///
    /// Returns [`SealStoreError`] on adapter failure. A listed key that does
    /// not parse as this partition's Seal key is a contradiction.
    pub(crate) async fn newest_generation(&self) -> Result<Option<SealGeneration>, SealStoreError> {
        let page = self
            .adapter
            .list_page(ListPageRequest {
                prefix: object_key(SealNamespace),
                start_exclusive: None,
                keys_max: newest_keys_bound(),
            })
            .await
            .map_err(SealStoreError::from_store)?;
        let Some(listed) = page.keys().first() else {
            return Ok(None);
        };
        let key = SealKey::from_str(listed.as_str()).map_err(|source| {
            SealStoreError::contradiction(format!(
                "listed key {listed} under the Seal namespace is not a Seal key: {source}"
            ))
        })?;
        Ok(Some(key.generation()))
    }

    /// Bounded read then checked decode against the exact identity.
    ///
    /// # Errors
    ///
    /// Returns [`SealStoreError`] on adapter failure. A present body that fails
    /// decode is a contradiction, never absence.
    pub(crate) async fn read_seal(
        &self,
        generation: SealGeneration,
    ) -> Result<Option<Seal>, SealStoreError> {
        let key = object_key(SealKey::from(generation));
        let bound = seal_read_bound();
        let Some(observed) = self
            .adapter
            .read(&key, bound)
            .await
            .map_err(SealStoreError::from_store)?
        else {
            return Ok(None);
        };
        decode_seal(generation, observed.body())
            .map(Some)
            .map_err(|source| {
                SealStoreError::contradiction(format!(
                    "Seal body at {key} failed checked decode for {generation:?}: {source}"
                ))
            })
    }
}

fn seal_read_bound() -> ByteBound {
    let bytes = u64::try_from(SEAL_ENCODED_BYTES_MAX).expect("SEAL_ENCODED_BYTES_MAX fits in u64");
    ByteBound::try_from(bytes).expect("SEAL_ENCODED_BYTES_MAX is nonzero")
}

#[cfg(test)]
mod tests {
    use super::*;
    use strom_object_store::ObjectKey;
    use strom_storage_domain::{PartitionId, TreeVersion, WalReplayPoint};

    #[tokio::test]
    async fn created_seal_reads_back_equal_at_its_exact_identity() {
        let store = SealStore::new(ObjectStoreAdapter::in_memory());
        let seal = seal_at(SealGeneration::genesis());
        let candidate = EncodedSeal::new(&seal).expect("genesis seal encodes");
        let evidence = store.create_seal(&candidate).await.expect("create runs");
        assert_eq!(
            CreateEvidence::Direct,
            evidence,
            "an unoccupied Seal coordinate grants Direct"
        );

        let observed = store
            .read_seal(candidate.generation())
            .await
            .expect("read runs")
            .expect("created Seal is present");
        assert_eq!(observed, seal, "read-after-create recovers the Seal");
        assert_eq!(
            observed.generation(),
            candidate.generation(),
            "decoded identity matches the durable location"
        );
    }

    #[tokio::test]
    async fn newest_generation_returns_the_greatest_after_creates_in_any_order() {
        let generation_one = SealGeneration::genesis();
        let generation_two = generation_one.successor().expect("generation two exists");

        for order in [
            [generation_one, generation_two],
            [generation_two, generation_one],
        ] {
            let store = SealStore::new(ObjectStoreAdapter::in_memory());
            for generation in order {
                let candidate = EncodedSeal::new(&seal_at(generation)).expect("seal encodes");
                store.create_seal(&candidate).await.expect("create runs");
            }
            let newest = store.newest_generation().await.expect("list runs");
            assert_eq!(
                newest,
                Some(generation_two),
                "MaxKeys=1 under reverse ordinals surfaces generation two regardless of create order"
            );
        }
    }

    #[tokio::test]
    async fn garbage_bytes_at_a_valid_seal_key_are_a_typed_contradiction() {
        let adapter = ObjectStoreAdapter::in_memory();
        let store = SealStore::new(adapter.clone());
        let identity = SealGeneration::genesis();
        let key = object_key(SealKey::from(identity));
        let garbage =
            FrozenBytes::try_from(b"garbage-seal-body".to_vec()).expect("garbage body freezes");
        adapter
            .create_if_absent(&key, garbage)
            .await
            .expect("plant runs");

        let outcome = store.read_seal(identity).await;
        assert!(
            matches!(outcome, Err(SealStoreError::Contradiction { .. })),
            "decode failure at an owned Seal key is Contradiction, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn garbage_key_under_the_seal_namespace_prefix_is_a_typed_contradiction_for_newest_generation()
     {
        let adapter = ObjectStoreAdapter::in_memory();
        let store = SealStore::new(adapter.clone());
        // `-` sorts before digits, so MaxKeys=1 surfaces this before any Seal key.
        let garbage_key = ObjectKey::try_from(format!("{SealNamespace}/-garbage"))
            .expect("garbage key is a legal ObjectKey");
        let body = FrozenBytes::try_from(b"foreign".to_vec()).expect("body freezes");
        adapter
            .create_if_absent(&garbage_key, body)
            .await
            .expect("plant runs");

        let outcome = store.newest_generation().await;
        assert!(
            matches!(outcome, Err(SealStoreError::Contradiction { .. })),
            "a foreign key under the Seal namespace is Contradiction, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn body_encoded_for_identity_a_planted_at_identity_b_is_a_typed_contradiction_on_read_seal()
     {
        let adapter = ObjectStoreAdapter::in_memory();
        let store = SealStore::new(adapter.clone());
        let generation_a = SealGeneration::genesis();
        let generation_b = generation_a.successor().expect("generation two exists");
        let candidate_a = EncodedSeal::new(&seal_at(generation_a)).expect("seal a encodes");
        let identity_b = generation_b;
        let planted = FrozenBytes::try_from(candidate_a.as_slice().to_vec())
            .expect("encoded seal body freezes");
        adapter
            .create_if_absent(&object_key(SealKey::from(identity_b)), planted)
            .await
            .expect("plant runs");

        let outcome = store.read_seal(identity_b).await;
        assert!(
            matches!(outcome, Err(SealStoreError::Contradiction { .. })),
            "IdentityMismatch at the read identity is Contradiction, got {outcome:?}"
        );
    }

    fn seal_at(generation: SealGeneration) -> Seal {
        Seal::new(
            partition(),
            generation,
            WalReplayPoint::Genesis,
            TreeVersion::empty(),
            TreeVersion::empty(),
        )
        .expect("empty genesis trees are valid")
    }

    fn partition() -> PartitionId {
        "00112233-4455-6677-8899-aabbccddeeff"
            .parse()
            .expect("test partition is canonical")
    }
}
