//! Typed WAL store: create-once candidates, newest surviving batch, exact GET,
//! and collector-authorized RUN delete.

use std::str::FromStr as _;

use strom_object_store::{
    ByteBound, CreateEvidence, Etag, ListPageRequest, ObjectStoreAdapter, PutBody,
};
use strom_storage_domain::{
    BatchId, EncodedWal, PartitionId, WAL_ENCODED_BYTES_MAX, WalBody, WalKey, WalNamespace,
    WalObject, decode_wal,
};

use super::{
    TypedStoreError, WalEstablishment, newest_keys_bound, object_key, typed_store_contradiction,
    typed_store_error,
};

/// One decoded WAL object plus the exact validator that observed it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ObservedWal {
    object: WalObject,
    validator: Etag,
    bytes: PutBody,
}

impl ObservedWal {
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn object(&self) -> &WalObject {
        &self.object
    }

    #[must_use]
    pub(crate) fn into_object(self) -> WalObject {
        self.object
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) const fn validator(&self) -> &Etag {
        &self.validator
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) const fn body(&self) -> &WalBody {
        self.object.body()
    }

    #[must_use]
    fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    /// Consume this observation into a collector delete proof for a RUN.
    ///
    /// # Errors
    ///
    /// Returns [`WalDeleteRefusal`] when the observed body is a FENCE.
    pub(crate) fn into_run_delete(self) -> Result<AuthorizedWalRunDelete, WalDeleteRefusal> {
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
pub(crate) enum WalDeleteRefusal {
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
pub(crate) struct AuthorizedWalRunDelete {
    batch: BatchId,
    validator: Etag,
}

/// Typed WAL namespace over the raw object-store adapter.
#[derive(Debug, Clone)]
pub(crate) struct WalStore {
    adapter: ObjectStoreAdapter,
}

impl WalStore {
    #[must_use]
    pub(crate) const fn new(adapter: ObjectStoreAdapter) -> Self {
        Self { adapter }
    }

    /// Send the candidate exactly once and reconcile one ambiguous response
    /// with at most one bounded exact GET of the frozen candidate coordinate.
    ///
    /// # Errors
    ///
    /// Returns [`TypedStoreError`] when the adapter reports a retryable,
    /// rejected, or contradictory outcome.
    pub(crate) async fn establish_wal(
        &self,
        candidate: &EncodedWal,
    ) -> Result<WalEstablishment, TypedStoreError> {
        match self.create_wal(candidate).await? {
            CreateEvidence::Direct | CreateEvidence::DurableMatch => Ok(WalEstablishment::Durable),
            CreateEvidence::NotOurs => Ok(WalEstablishment::Occupied),
            CreateEvidence::Unresolved => {
                match self
                    .read_wal(candidate.partition(), candidate.batch())
                    .await?
                {
                    Some(observed) if observed.as_slice() == candidate.bytes().as_ref() => {
                        Ok(WalEstablishment::Durable)
                    }
                    Some(_foreign) => Ok(WalEstablishment::Occupied),
                    None => Ok(WalEstablishment::UnresolvedAbsent),
                }
            }
        }
    }

    async fn create_wal(&self, candidate: &EncodedWal) -> Result<CreateEvidence, TypedStoreError> {
        let key = object_key(WalKey::from(candidate.batch()));
        self.adapter
            .create_if_absent(
                &key,
                PutBody::try_from(candidate.bytes().clone())
                    .expect("an encoded WAL fits the adapter PUT bound"),
            )
            .await
            .map_err(typed_store_error)
    }

    /// Newest surviving batch via one ascending list page with `keys_max = 1`.
    ///
    /// # Errors
    ///
    /// Returns [`TypedStoreError`] on adapter failure. A listed key that does
    /// not parse as this partition's WAL key is a contradiction.
    pub(crate) async fn newest_surviving_batch(&self) -> Result<Option<BatchId>, TypedStoreError> {
        let page = self
            .adapter
            .list_page(ListPageRequest {
                prefix: object_key(WalNamespace),
                start_exclusive: None,
                keys_max: newest_keys_bound(),
            })
            .await
            .map_err(typed_store_error)?;
        let Some(listed) = page.keys().first() else {
            return Ok(None);
        };
        let key = WalKey::from_str(listed.as_str()).map_err(|source| {
            typed_store_contradiction(format!(
                "listed key {listed} under the WAL namespace is not a WAL key: {source}"
            ))
        })?;
        Ok(Some(key.batch()))
    }

    /// Bounded read then checked decode against the exact identity.
    ///
    /// # Errors
    ///
    /// Returns [`TypedStoreError`] on adapter failure. A present body that fails
    /// decode is a contradiction, never absence.
    pub(crate) async fn read_wal(
        &self,
        partition: PartitionId,
        batch: BatchId,
    ) -> Result<Option<ObservedWal>, TypedStoreError> {
        let key = object_key(WalKey::from(batch));
        let bound = wal_read_bound();
        let Some(observed) = self
            .adapter
            .read(&key, bound)
            .await
            .map_err(typed_store_error)?
        else {
            return Ok(None);
        };
        let object = decode_wal(partition, batch, observed.body()).map_err(|source| {
            typed_store_contradiction(format!(
                "WAL body at {key} failed checked decode for {partition}/{batch:?}: {source}"
            ))
        })?;
        let (body, validator) = observed.into_parts();
        let bytes = PutBody::try_from(body).map_err(|source| {
            typed_store_contradiction(format!(
                "decoded WAL body at {key} cannot be retained for exact reconciliation: {source}"
            ))
        })?;
        Ok(Some(ObservedWal {
            object,
            validator,
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
    /// Returns [`TypedStoreError`] when the adapter reports a retryable,
    /// rejected, or contradictory outcome.
    pub(crate) async fn delete_run(
        &self,
        proof: AuthorizedWalRunDelete,
    ) -> Result<(), TypedStoreError> {
        let AuthorizedWalRunDelete { batch, validator } = proof;
        // Retained until If-Match delete is available; see AuthorizedWalRunDelete.
        drop(validator);
        let key = object_key(WalKey::from(batch));
        self.adapter
            .delete_idempotent(&key)
            .await
            .map_err(typed_store_error)
    }
}

fn wal_read_bound() -> ByteBound {
    let bytes = u64::try_from(WAL_ENCODED_BYTES_MAX).expect("WAL_ENCODED_BYTES_MAX fits in u64");
    ByteBound::try_from(bytes).expect("WAL_ENCODED_BYTES_MAX is nonzero")
}

#[cfg(test)]
mod tests {
    use object_store::path::Path;
    use object_store::{ObjectStoreExt as _, PutPayload};
    use strom_domain::StreamPath;
    use strom_object_store::ObjectKey;
    use strom_object_store::test_support::{
        BackendFailure, Fault, FaultStore, Operation, Selection, Target,
    };
    use strom_storage_domain::{OperationFact, OwnerToken, SealGeneration, StreamUid, WalFacts};

    use super::*;

    #[tokio::test]
    async fn wal_establishment_decides_direct_match_and_occupied_evidence() {
        let store = WalStore::new(ObjectStoreAdapter::in_memory());
        let candidate = EncodedWal::new(&run_at(batch(1))).expect("run encodes");
        assert_eq!(
            WalEstablishment::Durable,
            store
                .establish_wal(&candidate)
                .await
                .expect("direct create is decided")
        );
        assert_eq!(
            WalEstablishment::Durable,
            store
                .establish_wal(&candidate)
                .await
                .expect("matching occupant is decided")
        );
        let foreign = EncodedWal::new(&fence_at(batch(1))).expect("fence encodes");
        assert_eq!(
            WalEstablishment::Occupied,
            store
                .establish_wal(&foreign)
                .await
                .expect("foreign occupant is decided")
        );
    }

    #[tokio::test]
    async fn unresolved_wal_establishment_performs_one_exact_reconciliation()
    -> Result<(), Box<dyn std::error::Error>> {
        let candidate = EncodedWal::new(&run_at(batch(1)))?;
        let key = object_key(WalKey::from(candidate.batch()));

        let matching_fault = FaultStore::new().inject(Fault::CreateThenLoseResponse {
            target: Target::Key(key.clone()),
        })?;
        let matching = WalStore::new(ObjectStoreAdapter::new(matching_fault.backend()));
        assert_eq!(
            WalEstablishment::Durable,
            matching.establish_wal(&candidate).await?
        );
        matching_fault.assert_called_once(Operation::Create, &key)?;
        matching_fault.assert_called_once(Operation::Read, &key)?;
        matching_fault.verify()?;

        let foreign_fault = FaultStore::new().inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(key.clone())),
            failure: BackendFailure::Transport,
        })?;
        let foreign_backend = foreign_fault.backend();
        let foreign = EncodedWal::new(&fence_at(candidate.batch()))?;
        foreign_backend
            .put(
                &Path::from(key.as_str()),
                PutPayload::from(foreign.bytes().to_vec()),
            )
            .await?;
        let foreign_store = WalStore::new(ObjectStoreAdapter::new(foreign_backend));
        assert_eq!(
            WalEstablishment::Occupied,
            foreign_store.establish_wal(&candidate).await?
        );
        foreign_fault.assert_called_once(Operation::Create, &key)?;
        foreign_fault.assert_called_once(Operation::Read, &key)?;
        foreign_fault.verify()?;

        let absent_fault = FaultStore::new().inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(key.clone())),
            failure: BackendFailure::Transport,
        })?;
        let absent = WalStore::new(ObjectStoreAdapter::new(absent_fault.backend()));
        assert_eq!(
            WalEstablishment::UnresolvedAbsent,
            absent.establish_wal(&candidate).await?
        );
        absent_fault.assert_called_once(Operation::Create, &key)?;
        absent_fault.assert_called_once(Operation::Read, &key)?;
        absent_fault.verify()?;
        Ok(())
    }

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
    async fn garbage_bytes_at_a_valid_wal_key_are_a_typed_contradiction() {
        let adapter = ObjectStoreAdapter::in_memory();
        let store = WalStore::new(adapter.clone());
        let batch = batch(1);
        let key = object_key(WalKey::from(batch));
        let garbage =
            PutBody::try_from(b"garbage-wal-body".to_vec()).expect("garbage body freezes");
        adapter
            .create_if_absent(&key, garbage)
            .await
            .expect("plant runs");

        let outcome = store.read_wal(partition(), batch).await;
        assert!(
            matches!(outcome, Err(TypedStoreError::Contradiction { .. })),
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
    async fn garbage_key_under_the_wal_namespace_prefix_is_a_typed_contradiction_for_newest_surviving_batch()
     {
        let adapter = ObjectStoreAdapter::in_memory();
        let store = WalStore::new(adapter.clone());
        // `-` sorts before digits, so MaxKeys=1 surfaces this before any WAL key.
        let garbage_key = ObjectKey::try_from(format!("{WalNamespace}/-garbage"))
            .expect("garbage key is a legal ObjectKey");
        let body = PutBody::try_from(b"foreign".to_vec()).expect("body freezes");
        adapter
            .create_if_absent(&garbage_key, body)
            .await
            .expect("plant runs");

        let outcome = store.newest_surviving_batch().await;
        assert!(
            matches!(outcome, Err(TypedStoreError::Contradiction { .. })),
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
        let planted =
            PutBody::try_from(candidate_a.bytes().clone()).expect("encoded wal body freezes");
        adapter
            .create_if_absent(&object_key(WalKey::from(batch_b)), planted)
            .await
            .expect("plant runs");

        let outcome = store.read_wal(partition(), batch_b).await;
        assert!(
            matches!(outcome, Err(TypedStoreError::Contradiction { .. })),
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
        let path = "events/abc"
            .parse::<StreamPath>()
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
