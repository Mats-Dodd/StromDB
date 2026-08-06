//! Typed WAL store: create-once candidates, newest surviving batch, exact GET,
//! and collector-authorized RUN delete.

use std::str::FromStr as _;

use strom_object_store::{
    ByteBound, CreateEvidence, Etag, FrozenBytes, ListPageRequest, ObjectStoreAdapter,
};
use strom_storage_domain::{
    BatchId, EncodeError, PartitionId, WAL_ENCODED_BYTES_MAX, WalBody, WalKey, WalNamespace,
    WalObject, decode_wal, encode_wal,
};

use super::{StoreErrorClass, map_store_error, newest_keys_bound, object_key};

/// One WAL candidate, encoded exactly once. Key and body agree by construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedWal {
    batch: BatchId,
    bytes: FrozenBytes,
}

impl EncodedWal {
    /// Encode `object` once and freeze the exact bytes that will be sent.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError`] when serialization fails or the archive exceeds
    /// [`WAL_ENCODED_BYTES_MAX`].
    pub fn new(object: &WalObject) -> Result<Self, EncodeError> {
        let bytes = encode_wal(object)?;
        Ok(Self::from_encoded(object.batch(), bytes))
    }

    fn from_encoded(batch: BatchId, bytes: Vec<u8>) -> Self {
        let frozen = FrozenBytes::try_from(bytes)
            .expect("encode_wal yields a non-empty body within PUT_BYTES_MAX");
        Self {
            batch,
            bytes: frozen,
        }
    }

    #[must_use]
    pub const fn batch(&self) -> BatchId {
        self.batch
    }

    /// Exact frozen bytes of this candidate. After an ambiguous create, reconcile
    /// with one bounded GET compared against these bytes—never re-encode.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}

/// One decoded WAL object plus the exact validator that observed it.
#[derive(Debug, PartialEq, Eq)]
pub struct ObservedWal {
    object: WalObject,
    validator: Etag,
    bytes: FrozenBytes,
}

impl ObservedWal {
    #[must_use]
    pub const fn object(&self) -> &WalObject {
        &self.object
    }

    #[must_use]
    pub const fn validator(&self) -> &Etag {
        &self.validator
    }

    #[must_use]
    pub const fn batch(&self) -> BatchId {
        self.object.batch()
    }

    #[must_use]
    pub const fn body(&self) -> &WalBody {
        self.object.body()
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    /// Consume this observation into a collector delete proof for a RUN.
    ///
    /// # Errors
    ///
    /// Returns [`WalDeleteRefusal`] when the observed body is a FENCE.
    pub fn into_run_delete(self) -> Result<AuthorizedWalRunDelete, WalDeleteRefusal> {
        match self.object.body() {
            WalBody::Run(_) => Ok(AuthorizedWalRunDelete {
                batch: self.object.batch(),
                validator: self.validator,
            }),
            WalBody::Fence => Err(WalDeleteRefusal::Fence {
                batch: self.object.batch(),
            }),
        }
    }
}

/// Why an observation cannot become a collector delete proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WalDeleteRefusal {
    /// FENCE objects are permanent; collectors never delete them.
    #[error("WAL fence cannot become a delete proof")]
    Fence { batch: BatchId },
}

/// Proof that one decoded, collectible RUN was observed at its exact bytes.
///
/// Construction accepts only a RUN observation. A FENCE or a raw key cannot
/// construct this type, so a collector cannot delete either by mistake.
///
/// GAP: the storage contract asks for an exact-validator conditional delete
/// (S3 `DeleteObject` with `If-Match`). `object_store` 0.14 does not expose
/// delete preconditions, so `delete_run` sends an unconditional delete. The
/// validator is carried here so the seam does not change when the primitive
/// arrives. The correctness protocol does not depend on the validator today:
/// WAL coordinates are create-only and the fence protocol stops a legal
/// writer from re-occupying a collected coordinate. The validator is
/// defense in depth against a non-conforming actor in the namespace.
#[derive(Debug, PartialEq, Eq)]
pub struct AuthorizedWalRunDelete {
    batch: BatchId,
    validator: Etag,
}

impl AuthorizedWalRunDelete {
    #[must_use]
    pub const fn batch(&self) -> BatchId {
        self.batch
    }

    #[must_use]
    pub const fn validator(&self) -> &Etag {
        &self.validator
    }
}

/// Failures of WAL store operations, shaped for the writer state machine.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WalStoreError {
    /// Transport trouble; a bounded retry of the same idempotent request is legal.
    #[error("retryable WAL store failure: {detail}")]
    Retryable { detail: String },
    /// The backend refused the request definitively; retrying cannot help.
    #[error("WAL store rejected the request: {detail}")]
    Rejected { detail: String },
    /// Durable bytes violate the storage model. The caller fails closed.
    #[error("WAL store durable contradiction: {detail}")]
    Contradiction { detail: String },
}

impl WalStoreError {
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

/// Typed WAL namespace over the raw object-store adapter.
#[derive(Debug, Clone)]
pub struct WalStore {
    adapter: ObjectStoreAdapter,
}

impl WalStore {
    #[must_use]
    pub const fn new(adapter: ObjectStoreAdapter) -> Self {
        Self { adapter }
    }

    /// Send the candidate exactly once; the evidence passes through unchanged.
    ///
    /// An authority-bearing create is send-once: the caller must not call
    /// create again for the same candidate. After
    /// [`CreateEvidence::Unresolved`], the caller reconciles with a bounded
    /// exact GET compared against [`EncodedWal::as_slice`].
    ///
    /// # Errors
    ///
    /// Returns [`WalStoreError`] when the adapter reports a retryable,
    /// rejected, or contradictory outcome.
    pub async fn create_wal(
        &self,
        candidate: &EncodedWal,
    ) -> Result<CreateEvidence, WalStoreError> {
        let key = object_key(WalKey::from(candidate.batch));
        self.adapter
            .create_if_absent(&key, candidate.bytes.clone())
            .await
            .map_err(WalStoreError::from_store)
    }

    /// Newest surviving batch via one ascending list page with `keys_max = 1`.
    ///
    /// # Errors
    ///
    /// Returns [`WalStoreError`] on adapter failure. A listed key that does
    /// not parse as this partition's WAL key is a contradiction.
    pub async fn newest_surviving_batch(&self) -> Result<Option<BatchId>, WalStoreError> {
        let page = self
            .adapter
            .list_page(ListPageRequest {
                prefix: object_key(WalNamespace),
                start_exclusive: None,
                keys_max: newest_keys_bound(),
            })
            .await
            .map_err(WalStoreError::from_store)?;
        let Some(listed) = page.keys().first() else {
            return Ok(None);
        };
        let key = WalKey::from_str(listed.as_str()).map_err(|source| {
            WalStoreError::contradiction(format!(
                "listed key {listed} under the WAL namespace is not a WAL key: {source}"
            ))
        })?;
        Ok(Some(key.batch()))
    }

    /// Bounded read then checked decode against the exact identity.
    ///
    /// # Errors
    ///
    /// Returns [`WalStoreError`] on adapter failure. A present body that fails
    /// decode is a contradiction, never absence.
    pub async fn read_wal(
        &self,
        partition: PartitionId,
        batch: BatchId,
    ) -> Result<Option<ObservedWal>, WalStoreError> {
        let key = object_key(WalKey::from(batch));
        let bound = wal_read_bound();
        let Some(observed) = self
            .adapter
            .read(&key, bound)
            .await
            .map_err(WalStoreError::from_store)?
        else {
            return Ok(None);
        };
        let object = decode_wal(partition, batch, observed.body()).map_err(|source| {
            WalStoreError::contradiction(format!(
                "WAL body at {key} failed checked decode for {partition}/{batch:?}: {source}"
            ))
        })?;
        let bytes = FrozenBytes::try_from(observed.body().to_vec()).map_err(|source| {
            WalStoreError::contradiction(format!(
                "decoded WAL body at {key} cannot be retained for exact reconciliation: {source}"
            ))
        })?;
        Ok(Some(ObservedWal {
            object,
            validator: observed.etag().clone(),
            bytes,
        }))
    }

    /// Delete one observed RUN. Absence already satisfies the contract.
    ///
    /// GAP: the storage contract asks for an exact-validator conditional delete
    /// (S3 `DeleteObject` with `If-Match`). `object_store` 0.14 does not expose
    /// delete preconditions, so `delete_run` sends an unconditional delete. The
    /// validator is carried on [`AuthorizedWalRunDelete`] so the seam does not
    /// change when the primitive arrives. The correctness protocol does not
    /// depend on the validator today: WAL coordinates are create-only and the
    /// fence protocol stops a legal writer from re-occupying a collected
    /// coordinate. The validator is defense in depth against a non-conforming
    /// actor in the namespace.
    ///
    /// # Errors
    ///
    /// Returns [`WalStoreError`] when the adapter reports a retryable,
    /// rejected, or contradictory outcome.
    pub async fn delete_run(&self, proof: AuthorizedWalRunDelete) -> Result<(), WalStoreError> {
        let AuthorizedWalRunDelete { batch, validator } = proof;
        // Retained until If-Match delete is available; see AuthorizedWalRunDelete.
        drop(validator);
        let key = object_key(WalKey::from(batch));
        self.adapter
            .delete_idempotent(&key)
            .await
            .map_err(WalStoreError::from_store)
    }
}

fn wal_read_bound() -> ByteBound {
    let bytes = u64::try_from(WAL_ENCODED_BYTES_MAX).expect("WAL_ENCODED_BYTES_MAX fits in u64");
    ByteBound::try_from(bytes).expect("WAL_ENCODED_BYTES_MAX is nonzero")
}

#[cfg(test)]
mod tests {
    use strom_object_store::ObjectKey;
    use strom_storage_domain::{
        DirectoryKey, OperationFact, OwnerToken, SealGeneration, StreamUid, WalFacts,
    };

    use super::*;

    #[tokio::test]
    async fn created_wal_reads_back_equal_with_a_stable_validator() {
        let store = WalStore::new(ObjectStoreAdapter::in_memory());
        let object = run_at(batch(1));
        let candidate = EncodedWal::new(&object).expect("run encodes");
        let evidence = store.create_wal(&candidate).await.expect("create runs");
        assert_eq!(
            CreateEvidence::Direct,
            evidence,
            "an unoccupied WAL coordinate grants Direct"
        );

        let first = store
            .read_wal(partition(), candidate.batch())
            .await
            .expect("read runs")
            .expect("created WAL is present");
        assert_eq!(
            first.object(),
            &object,
            "read-after-create recovers the WAL object"
        );
        let second = store
            .read_wal(partition(), candidate.batch())
            .await
            .expect("re-read runs")
            .expect("created WAL is still present");
        assert_eq!(
            first.validator(),
            second.validator(),
            "the observation carries a stable etag across reads"
        );
    }

    #[tokio::test]
    async fn newest_surviving_batch_returns_the_greatest_after_creates_in_any_order() {
        let one = batch(1);
        let two = batch(2);
        for order in [[one, two], [two, one]] {
            let store = WalStore::new(ObjectStoreAdapter::in_memory());
            for batch_id in order {
                let candidate = EncodedWal::new(&run_at(batch_id)).expect("run encodes");
                store.create_wal(&candidate).await.expect("create runs");
            }
            let newest = store.newest_surviving_batch().await.expect("list runs");
            assert_eq!(
                newest,
                Some(two),
                "MaxKeys=1 under reverse ordinals surfaces batch two regardless of create order"
            );
        }
    }

    #[tokio::test]
    async fn second_create_reports_durable_match_or_not_ours_by_bytes() {
        let store = WalStore::new(ObjectStoreAdapter::in_memory());
        let object = run_at(batch(1));
        let candidate = EncodedWal::new(&object).expect("run encodes");
        store
            .create_wal(&candidate)
            .await
            .expect("first create runs");

        let same = store
            .create_wal(&candidate)
            .await
            .expect("second create runs");
        assert_eq!(
            CreateEvidence::DurableMatch,
            same,
            "identical bytes prove existence, never authorship"
        );

        let adapter = ObjectStoreAdapter::in_memory();
        let contested = WalStore::new(adapter.clone());
        let key = object_key(WalKey::from(object.batch()));
        let foreign = FrozenBytes::try_from(b"not-a-wal".to_vec()).expect("foreign body freezes");
        adapter
            .create_if_absent(&key, foreign)
            .await
            .expect("foreign create runs");
        let different = contested
            .create_wal(&candidate)
            .await
            .expect("contested create runs");
        assert_eq!(
            CreateEvidence::NotOurs,
            different,
            "a different occupant fences the caller"
        );
    }

    #[tokio::test]
    async fn absence_is_none_for_read_and_newest_surviving_batch() {
        let store = WalStore::new(ObjectStoreAdapter::in_memory());
        let batch = batch(1);
        let read = store.read_wal(partition(), batch).await.expect("read runs");
        assert!(read.is_none(), "absence is Ok(None), not an error");
        let newest = store.newest_surviving_batch().await.expect("list runs");
        assert!(
            newest.is_none(),
            "an empty WAL namespace has no newest surviving batch"
        );
    }

    #[tokio::test]
    async fn garbage_bytes_at_a_valid_wal_key_are_a_typed_contradiction() {
        let adapter = ObjectStoreAdapter::in_memory();
        let store = WalStore::new(adapter.clone());
        let batch = batch(1);
        let key = object_key(WalKey::from(batch));
        let garbage =
            FrozenBytes::try_from(b"garbage-wal-body".to_vec()).expect("garbage body freezes");
        adapter
            .create_if_absent(&key, garbage)
            .await
            .expect("plant runs");

        let outcome = store.read_wal(partition(), batch).await;
        assert!(
            matches!(outcome, Err(WalStoreError::Contradiction { .. })),
            "decode failure at an owned WAL key is Contradiction, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn fence_observation_refuses_to_become_a_delete_proof() {
        let store = WalStore::new(ObjectStoreAdapter::in_memory());
        let object = fence_at(batch(1));
        let candidate = EncodedWal::new(&object).expect("fence encodes");
        store.create_wal(&candidate).await.expect("create runs");

        let observed = store
            .read_wal(partition(), candidate.batch())
            .await
            .expect("read runs")
            .expect("fence is present");
        assert!(
            matches!(observed.body(), WalBody::Fence),
            "a fence body decodes as WalBody::Fence"
        );
        assert_eq!(
            observed.into_run_delete(),
            Err(WalDeleteRefusal::Fence {
                batch: object.batch()
            }),
            "a fence observation cannot construct a delete proof"
        );
    }

    #[tokio::test]
    async fn run_delete_removes_the_object_and_is_idempotent_with_a_second_proof() {
        let store = WalStore::new(ObjectStoreAdapter::in_memory());
        let object = run_at(batch(1));
        let candidate = EncodedWal::new(&object).expect("run encodes");
        store.create_wal(&candidate).await.expect("create runs");

        // Two observations before delete: AuthorizedWalRunDelete is consumed,
        // so a second delete needs its own proof rather than cloning the first.
        let first_proof = store
            .read_wal(partition(), candidate.batch())
            .await
            .expect("read runs")
            .expect("run is present")
            .into_run_delete()
            .expect("a run observation yields a delete proof");
        let second_proof = store
            .read_wal(partition(), candidate.batch())
            .await
            .expect("re-read runs")
            .expect("run is still present")
            .into_run_delete()
            .expect("a second run observation yields a second proof");

        store
            .delete_run(first_proof)
            .await
            .expect("delete of a present run runs");
        let after = store
            .read_wal(partition(), candidate.batch())
            .await
            .expect("read after delete runs");
        assert!(after.is_none(), "a deleted RUN is absent");

        store
            .delete_run(second_proof)
            .await
            .expect("delete of an already-absent coordinate is idempotent");
    }

    #[tokio::test]
    async fn deleting_the_newest_run_leaves_older_surviving_coordinates() {
        let store = WalStore::new(ObjectStoreAdapter::in_memory());
        let older = run_at(batch(1));
        let newer = run_at(batch(2));
        store
            .create_wal(&EncodedWal::new(&older).expect("older encodes"))
            .await
            .expect("create older runs");
        store
            .create_wal(&EncodedWal::new(&newer).expect("newer encodes"))
            .await
            .expect("create newer runs");

        let proof = store
            .read_wal(partition(), newer.batch())
            .await
            .expect("read runs")
            .expect("newer run is present")
            .into_run_delete()
            .expect("newer run yields a delete proof");
        store.delete_run(proof).await.expect("delete newer runs");

        let newest = store.newest_surviving_batch().await.expect("list runs");
        assert_eq!(
            newest,
            Some(batch(1)),
            "deleting batch two leaves batch one as the newest survivor"
        );
    }

    #[tokio::test]
    async fn garbage_key_under_the_wal_namespace_prefix_is_a_typed_contradiction_for_newest_surviving_batch()
     {
        let adapter = ObjectStoreAdapter::in_memory();
        let store = WalStore::new(adapter.clone());
        // `-` sorts before digits, so MaxKeys=1 surfaces this before any WAL key.
        let garbage_key = ObjectKey::try_from(format!("{WalNamespace}/-garbage"))
            .expect("garbage key is a legal ObjectKey");
        let body = FrozenBytes::try_from(b"foreign".to_vec()).expect("body freezes");
        adapter
            .create_if_absent(&garbage_key, body)
            .await
            .expect("plant runs");

        let outcome = store.newest_surviving_batch().await;
        assert!(
            matches!(outcome, Err(WalStoreError::Contradiction { .. })),
            "a foreign key under the WAL namespace is Contradiction, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn body_encoded_for_identity_a_planted_at_identity_b_is_a_typed_contradiction_on_read_wal()
     {
        let adapter = ObjectStoreAdapter::in_memory();
        let store = WalStore::new(adapter.clone());
        let batch_a = batch(1);
        let batch_b = batch(2);
        let candidate_a = EncodedWal::new(&run_at(batch_a)).expect("wal a encodes");
        let planted = FrozenBytes::try_from(candidate_a.as_slice().to_vec())
            .expect("encoded wal body freezes");
        adapter
            .create_if_absent(&object_key(WalKey::from(batch_b)), planted)
            .await
            .expect("plant runs");

        let outcome = store.read_wal(partition(), batch_b).await;
        assert!(
            matches!(outcome, Err(WalStoreError::Contradiction { .. })),
            "IdentityMismatch at the read identity is Contradiction, got {outcome:?}"
        );
    }

    fn run_at(batch_id: BatchId) -> WalObject {
        let facts = WalFacts::try_from(vec![deleted_fact()]).expect("one fact is a legal run");
        WalObject::new(partition(), batch_id, owner(), WalBody::Run(facts))
    }

    fn fence_at(batch_id: BatchId) -> WalObject {
        WalObject::new(partition(), batch_id, owner(), WalBody::Fence)
    }

    fn deleted_fact() -> OperationFact {
        let path = DirectoryKey::try_from(Box::<[u8]>::from(b"events/abc".as_slice()))
            .expect("test stream path is canonical");
        let uid = StreamUid::try_from(1).expect("test uid is nonzero");
        OperationFact::StreamDeleted { path, uid }
    }

    fn batch(raw: u64) -> BatchId {
        BatchId::try_from(raw).expect("test batch is nonzero")
    }

    fn partition() -> PartitionId {
        "00112233-4455-6677-8899-aabbccddeeff"
            .parse()
            .expect("test partition is canonical")
    }

    fn owner() -> OwnerToken {
        OwnerToken::from(SealGeneration::genesis())
    }
}
