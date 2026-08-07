//! Typed Seal store: create-once candidates, newest-generation LIST, exact GET.

use std::str::FromStr as _;

use strom_object_store::{ByteBound, CreateEvidence, ListPageRequest, ObjectStoreAdapter, PutBody};
use strom_storage_domain::{
    EncodedAuthoritySeal, EncodedGenesisSeal, SEAL_ENCODED_BYTES_MAX, Seal, SealGeneration,
    SealKey, SealNamespace, decode_seal,
};

use super::{
    SealPublication, TypedStoreError, newest_keys_bound, object_key, typed_store_contradiction,
    typed_store_error,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GenesisEstablishment {
    Established,
    LostRace,
    Unresolved,
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

    pub(crate) async fn establish_genesis(
        &self,
        candidate: &EncodedGenesisSeal,
    ) -> Result<GenesisEstablishment, TypedStoreError> {
        match self
            .create_seal(candidate.generation(), candidate.bytes())
            .await?
        {
            CreateEvidence::Direct | CreateEvidence::DurableMatch => {
                Ok(GenesisEstablishment::Established)
            }
            CreateEvidence::NotOurs => Ok(GenesisEstablishment::LostRace),
            CreateEvidence::Unresolved => Ok(GenesisEstablishment::Unresolved),
        }
    }

    pub(crate) async fn publish_authority(
        &self,
        candidate: &EncodedAuthoritySeal,
    ) -> Result<SealPublication, TypedStoreError> {
        match self
            .create_seal(candidate.generation(), candidate.bytes())
            .await?
        {
            CreateEvidence::Direct => Ok(SealPublication::Authored),
            CreateEvidence::DurableMatch | CreateEvidence::NotOurs => {
                Ok(SealPublication::NoAuthority)
            }
            CreateEvidence::Unresolved => Ok(SealPublication::Unresolved),
        }
    }

    async fn create_seal(
        &self,
        generation: SealGeneration,
        bytes: &bytes::Bytes,
    ) -> Result<CreateEvidence, TypedStoreError> {
        let key = object_key(SealKey::from(generation));
        self.adapter
            .create_if_absent(
                &key,
                PutBody::try_from(bytes.clone())
                    .expect("an encoded Seal fits the adapter PUT bound"),
            )
            .await
            .map_err(typed_store_error)
    }

    /// Newest generation via one ascending list page with `keys_max = 1`.
    ///
    /// # Errors
    ///
    /// Returns [`TypedStoreError`] on adapter failure. A listed key that does
    /// not parse as this partition's Seal key is a contradiction.
    pub(crate) async fn newest_generation(
        &self,
    ) -> Result<Option<SealGeneration>, TypedStoreError> {
        let page = self
            .adapter
            .list_page(ListPageRequest {
                prefix: object_key(SealNamespace),
                start_exclusive: None,
                keys_max: newest_keys_bound(),
            })
            .await
            .map_err(typed_store_error)?;
        let Some(listed) = page.keys().first() else {
            return Ok(None);
        };
        let key = SealKey::from_str(listed.as_str()).map_err(|source| {
            typed_store_contradiction(format!(
                "listed key {listed} under the Seal namespace is not a Seal key: {source}"
            ))
        })?;
        Ok(Some(key.generation()))
    }

    /// Bounded read then checked decode against the exact identity.
    ///
    /// # Errors
    ///
    /// Returns [`TypedStoreError`] on adapter failure. A present body that fails
    /// decode is a contradiction, never absence.
    pub(crate) async fn read_seal(
        &self,
        generation: SealGeneration,
    ) -> Result<Option<Seal>, TypedStoreError> {
        let key = object_key(SealKey::from(generation));
        let bound = seal_read_bound();
        let Some(observed) = self
            .adapter
            .read(&key, bound)
            .await
            .map_err(typed_store_error)?
        else {
            return Ok(None);
        };
        decode_seal(generation, observed.body())
            .map(Some)
            .map_err(|source| {
                typed_store_contradiction(format!(
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
    use strom_object_store::test_support::{
        BackendFailure, Fault, FaultStore, Operation, Selection, Target,
    };
    use strom_storage_domain::{PartitionId, TreeVersion, WalReplayPoint, encode_seal};

    #[tokio::test]
    async fn genesis_establishment_decides_every_evidence_class()
    -> Result<(), Box<dyn std::error::Error>> {
        let genesis = seal_at(SealGeneration::genesis());
        let candidate = EncodedGenesisSeal::try_from(&genesis)?;
        let store = SealStore::new(ObjectStoreAdapter::in_memory());
        assert_eq!(
            GenesisEstablishment::Established,
            store.establish_genesis(&candidate).await?
        );
        assert_eq!(
            GenesisEstablishment::Established,
            store.establish_genesis(&candidate).await?
        );

        let foreign_partition = "11112222-3333-4444-8888-9999aaaabbbb".parse()?;
        let foreign = Seal::new(
            foreign_partition,
            SealGeneration::genesis(),
            WalReplayPoint::Genesis,
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;
        let occupied_store = SealStore::new(ObjectStoreAdapter::in_memory());
        occupied_store
            .establish_genesis(&EncodedGenesisSeal::try_from(&foreign)?)
            .await?;
        assert_eq!(
            GenesisEstablishment::LostRace,
            occupied_store.establish_genesis(&candidate).await?
        );

        let key = object_key(SealKey::from(SealGeneration::genesis()));
        let fault_store = FaultStore::new()
            .inject(Fault::FailBefore {
                selection: Selection::create(Target::Key(key.clone())),
                failure: BackendFailure::Transport,
            })?
            .inject(Fault::FailBefore {
                selection: Selection::read(Target::Key(key.clone())),
                failure: BackendFailure::Transport,
            })?;
        let ambiguous = SealStore::new(ObjectStoreAdapter::new(fault_store.backend()));
        assert_eq!(
            GenesisEstablishment::Unresolved,
            ambiguous.establish_genesis(&candidate).await?
        );
        assert_create_without_reconcile(&fault_store, &key)?;
        Ok(())
    }

    #[tokio::test]
    async fn authority_publication_requires_direct_authorship()
    -> Result<(), Box<dyn std::error::Error>> {
        let generation = SealGeneration::genesis().successor()?;
        let candidate = EncodedAuthoritySeal::try_from(&seal_at(generation))?;
        let store = SealStore::new(ObjectStoreAdapter::in_memory());
        assert_eq!(
            SealPublication::Authored,
            store.publish_authority(&candidate).await?
        );
        assert_eq!(
            SealPublication::NoAuthority,
            store.publish_authority(&candidate).await?
        );

        let foreign_partition = "11112222-3333-4444-8888-9999aaaabbbb".parse()?;
        let foreign = Seal::new(
            foreign_partition,
            generation,
            WalReplayPoint::Genesis,
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;
        let occupied_store = SealStore::new(ObjectStoreAdapter::in_memory());
        occupied_store
            .publish_authority(&EncodedAuthoritySeal::try_from(&foreign)?)
            .await?;
        assert_eq!(
            SealPublication::NoAuthority,
            occupied_store.publish_authority(&candidate).await?
        );

        let key = object_key(SealKey::from(generation));
        let fault_store = FaultStore::new()
            .inject(Fault::FailBefore {
                selection: Selection::create(Target::Key(key.clone())),
                failure: BackendFailure::Transport,
            })?
            .inject(Fault::FailBefore {
                selection: Selection::read(Target::Key(key.clone())),
                failure: BackendFailure::Transport,
            })?;
        let ambiguous = SealStore::new(ObjectStoreAdapter::new(fault_store.backend()));
        assert_eq!(
            SealPublication::Unresolved,
            ambiguous.publish_authority(&candidate).await?
        );
        assert_create_without_reconcile(&fault_store, &key)?;
        Ok(())
    }

    #[tokio::test]
    async fn created_seal_reads_back_equal_at_its_exact_identity() {
        let store = SealStore::new(ObjectStoreAdapter::in_memory());
        let seal = seal_at(SealGeneration::genesis());
        let candidate = EncodedGenesisSeal::try_from(&seal).expect("genesis seal encodes");
        let evidence = store
            .create_seal(candidate.generation(), candidate.bytes())
            .await
            .expect("create runs");
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
                let seal = seal_at(generation);
                let bytes = bytes::Bytes::from(encode_seal(&seal).expect("seal encodes"));
                store
                    .create_seal(generation, &bytes)
                    .await
                    .expect("create runs");
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
            PutBody::try_from(b"garbage-seal-body".to_vec()).expect("garbage body freezes");
        adapter
            .create_if_absent(&key, garbage)
            .await
            .expect("plant runs");

        let outcome = store.read_seal(identity).await;
        assert!(
            matches!(outcome, Err(TypedStoreError::Contradiction { .. })),
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
        let body = PutBody::try_from(b"foreign".to_vec()).expect("body freezes");
        adapter
            .create_if_absent(&garbage_key, body)
            .await
            .expect("plant runs");

        let outcome = store.newest_generation().await;
        assert!(
            matches!(outcome, Err(TypedStoreError::Contradiction { .. })),
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
        let candidate_a =
            EncodedGenesisSeal::try_from(&seal_at(generation_a)).expect("seal a encodes");
        let identity_b = generation_b;
        let planted =
            PutBody::try_from(candidate_a.bytes().clone()).expect("encoded seal body freezes");
        adapter
            .create_if_absent(&object_key(SealKey::from(identity_b)), planted)
            .await
            .expect("plant runs");

        let outcome = store.read_seal(identity_b).await;
        assert!(
            matches!(outcome, Err(TypedStoreError::Contradiction { .. })),
            "IdentityMismatch at the read identity is Contradiction, got {outcome:?}"
        );
    }

    fn assert_create_without_reconcile(
        store: &FaultStore,
        key: &ObjectKey,
    ) -> Result<(), Box<dyn std::error::Error>> {
        store.assert_called_once(Operation::Create, key)?;
        let diagnostic = store
            .verify()
            .expect_err("the read trap remains unused when Seal publication does not reconcile");
        let detail = diagnostic.to_string();
        assert!(
            detail.contains("unused fault") && detail.contains(&format!("Read {key}")),
            "only the explicit read trap remains unused: {detail}"
        );
        Ok(())
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
