//! Best-effort collection after an advancing Seal publication.

use strom_object_store::ObjectStoreAdapter;
use strom_storage_domain::{BatchId, PartitionId, Seal};

use crate::store::{TableStore, WalStore, targeted_table_deletes};

pub(crate) async fn collect_advance(adapter: ObjectStoreAdapter, source: Seal, successor: Seal) {
    let (partition, first, cut) = plan_wal_collection(&source, &successor);
    let table_deletes = targeted_table_deletes(&source, &successor);
    let wal_store = WalStore::new(adapter.clone());
    let mut batch = first;
    loop {
        match wal_store.read_wal(partition, batch).await {
            Ok(Some(observed)) => match observed.into_run_delete() {
                Ok(proof) => {
                    if wal_store.delete_run(proof).await.is_err() {
                        return;
                    }
                }
                Err(_fence) => {}
            },
            Ok(None) => {}
            Err(_) => return,
        }
        if batch == cut {
            break;
        }
        batch = batch
            .successor()
            .expect("a collection coordinate below its cut has a successor");
    }

    let table_store = TableStore::new(adapter);
    for proof in table_deletes {
        if table_store.delete_table(proof).await.is_err() {
            return;
        }
    }
}

fn plan_wal_collection(source: &Seal, successor: &Seal) -> (PartitionId, BatchId, BatchId) {
    assert_eq!(
        source.partition(),
        successor.partition(),
        "one advancing Seal transition keeps the partition identity"
    );
    assert_eq!(
        source
            .generation()
            .successor()
            .expect("an advancing successor proves the source generation is not exhausted"),
        successor.generation(),
        "collection requires an exact Seal successor pair"
    );
    let cut = successor
        .replay()
        .batch()
        .expect("an advancing successor Seal has a WAL cut");
    let first = match source.replay().batch() {
        None => BatchId::try_from(1).expect("batch one is a legal WAL coordinate"),
        Some(previous) => {
            assert!(
                previous < cut,
                "an advancing successor Seal has a strictly greater WAL cut"
            );
            previous
                .successor()
                .expect("a WAL cut below its successor cut has a successor coordinate")
        }
    };
    (source.partition(), first, cut)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt as _};
    use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};
    use strom_object_store::test_support::{
        BackendFailure, Fault, FaultStore, Operation, Selection, Target,
    };
    use strom_object_store::{CreateEvidence, FrozenBytes, ObjectKey};
    use strom_storage_domain::{
        AttemptId, DirectoryKey, FreshIdentity, OperationFact, OwnerToken, SealGeneration, SealKey,
        SortedRun, StoreKind, StreamUid, TableKey, TableObjectId, TableRef, TreeVersion, WalBody,
        WalFacts, WalKey, WalObject, WalReplayPoint,
    };

    use super::*;
    use crate::store::{EncodedSeal, EncodedWal, SealStore, WalStore};

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    const COVERED_RUN_BATCHES: [u64; 2] = [1, 4];
    const FENCE_BATCH: u64 = 2;
    const CUT_BATCH: u64 = 4;
    const AFTER_CUT_BATCH: u64 = 5;
    const TABLE_MARKER: &[u8] = b"collection table";

    #[tokio::test]
    async fn complete_collection_deletes_only_covered_runs_and_dropped_source_tables() -> TestResult
    {
        let fixture = CollectionFixture::plant(CollectionState::new()?, None).await?;

        collect_advance(
            fixture.adapter.clone(),
            fixture.source.clone(),
            fixture.successor.clone(),
        )
        .await;

        fixture.assert_complete().await?;
        for batch in COVERED_RUN_BATCHES {
            fixture.store.assert_called_once(
                Operation::Delete,
                &object_key(WalKey::from(BatchId::try_from(batch)?)),
            )?;
        }
        fixture.store.verify()?;
        Ok(())
    }

    #[tokio::test]
    async fn every_delete_failure_is_leak_only_and_repeated_collection_finishes() -> TestResult {
        let state = CollectionState::new()?;
        for target in state.dead.clone() {
            check_delete_failure(
                state.clone(),
                target.clone(),
                Fault::FailBefore {
                    selection: Selection::delete(Target::Key(target.clone())),
                    failure: BackendFailure::Transport,
                },
                ObjectState::Present,
            )
            .await?;
            check_delete_failure(
                state.clone(),
                target.clone(),
                Fault::DeleteThenLoseResponse {
                    target: Target::Key(target.clone()),
                },
                ObjectState::Absent,
            )
            .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn invalid_seal_transitions_are_rejected_before_any_delete() -> TestResult {
        let fixture = CollectionFixture::plant(CollectionState::new()?, None).await?;
        for transition in [
            InvalidTransition::CrossPartition,
            InvalidTransition::SkippedGeneration,
            InvalidTransition::NonAdvancingCut,
            InvalidTransition::GenesisSuccessor,
        ] {
            let (source, successor) = transition.seals(&fixture.source, &fixture.successor)?;

            let outcome =
                tokio::spawn(collect_advance(fixture.adapter.clone(), source, successor)).await;

            assert!(
                outcome
                    .expect_err("an invalid collection transition panics")
                    .is_panic(),
                "an invalid collection transition fails as an in-process invariant: {transition:?}"
            );
            fixture.assert_unchanged().await?;
        }
        Ok(())
    }

    async fn check_delete_failure(
        state: CollectionState,
        target: ObjectKey,
        fault: Fault,
        target_state: ObjectState,
    ) -> TestResult {
        assert!(
            state.dead.contains(&target),
            "a collection fault targets an object authorized for deletion"
        );
        let fixture = CollectionFixture::plant(state, Some(fault)).await?;
        collect_advance(
            fixture.adapter.clone(),
            fixture.source.clone(),
            fixture.successor.clone(),
        )
        .await;

        fixture.assert_preserved().await?;
        fixture.assert_state(&target, target_state).await?;

        collect_advance(
            ObjectStoreAdapter::new(Arc::clone(&fixture.backend)),
            fixture.source.clone(),
            fixture.successor.clone(),
        )
        .await;

        fixture.assert_complete().await?;
        fixture.store.verify()?;
        Ok(())
    }

    struct CollectionFixture {
        store: FaultStore,
        backend: Arc<dyn ObjectStore>,
        adapter: ObjectStoreAdapter,
        source: Seal,
        successor: Seal,
        dead: Vec<ObjectKey>,
        preserved: Vec<ObjectKey>,
    }

    impl CollectionFixture {
        async fn plant(state: CollectionState, fault: Option<Fault>) -> TestResult<Self> {
            let store = match fault {
                Some(fault) => FaultStore::new().inject(fault)?,
                None => FaultStore::new(),
            };
            let backend = store.backend();
            let adapter = ObjectStoreAdapter::new(Arc::clone(&backend));
            let wal_store = WalStore::new(adapter.clone());
            for wal in state
                .covered_runs
                .iter()
                .chain([&state.fence, &state.after_cut])
            {
                assert_eq!(CreateEvidence::Direct, wal_store.create_wal(wal).await?);
            }
            for table in &state.tables {
                plant_table(&adapter, table).await?;
            }
            let seal_store = SealStore::new(adapter.clone());
            for seal in [&state.source, &state.successor] {
                assert_eq!(
                    CreateEvidence::Direct,
                    seal_store.create_seal(&EncodedSeal::new(seal)?).await?
                );
            }

            Ok(Self {
                store,
                backend,
                adapter,
                source: state.source,
                successor: state.successor,
                dead: state.dead,
                preserved: state.preserved,
            })
        }

        async fn assert_complete(&self) -> TestResult {
            for key in &self.dead {
                self.assert_state(key, ObjectState::Absent).await?;
            }
            self.assert_preserved().await
        }

        async fn assert_unchanged(&self) -> TestResult {
            for key in &self.dead {
                self.assert_state(key, ObjectState::Present).await?;
            }
            self.assert_preserved().await
        }

        async fn assert_preserved(&self) -> TestResult {
            for key in &self.preserved {
                self.assert_state(key, ObjectState::Present).await?;
            }
            Ok(())
        }

        async fn assert_state(&self, key: &ObjectKey, expected: ObjectState) -> TestResult {
            let observed = match self.backend.head(&Path::from(key.as_str())).await {
                Ok(_metadata) => ObjectState::Present,
                Err(object_store::Error::NotFound { .. }) => ObjectState::Absent,
                Err(error) => return Err(error.into()),
            };
            assert_eq!(expected, observed, "durable object state differs for {key}");
            Ok(())
        }
    }

    #[derive(Clone)]
    struct CollectionState {
        source: Seal,
        successor: Seal,
        covered_runs: Vec<EncodedWal>,
        fence: EncodedWal,
        after_cut: EncodedWal,
        tables: Vec<TableRef>,
        dead: Vec<ObjectKey>,
        preserved: Vec<ObjectKey>,
    }

    impl CollectionState {
        fn new() -> TestResult<Self> {
            let partition = partition();
            let source_generation = SealGeneration::genesis().successor()?;
            let successor_generation = source_generation.successor()?;
            let dropped_directory = table_ref(
                source_generation,
                SealGeneration::genesis(),
                0,
                StoreKind::Directory,
            )?;
            let carried_directory = table_ref(
                source_generation,
                SealGeneration::genesis(),
                1,
                StoreKind::Directory,
            )?;
            let dropped_ledger = table_ref(
                source_generation,
                SealGeneration::genesis(),
                0,
                StoreKind::Ledger,
            )?;
            let carried_ledger = table_ref(
                source_generation,
                SealGeneration::genesis(),
                1,
                StoreKind::Ledger,
            )?;
            let fresh_directory = table_ref(
                successor_generation,
                source_generation,
                0,
                StoreKind::Directory,
            )?;
            let fresh_ledger = table_ref(
                successor_generation,
                source_generation,
                0,
                StoreKind::Ledger,
            )?;
            let unrelated = table_ref(
                successor_generation,
                source_generation,
                1,
                StoreKind::Directory,
            )?;
            let source = Seal::new(
                partition,
                source_generation,
                WalReplayPoint::Genesis,
                tree([dropped_directory, carried_directory])?,
                tree([dropped_ledger, carried_ledger])?,
            )?;
            let owner = OwnerToken::from(source_generation);
            let successor = Seal::new(
                partition,
                successor_generation,
                WalReplayPoint::Through {
                    batch: BatchId::try_from(CUT_BATCH)?,
                    owner,
                },
                tree([fresh_directory, carried_directory])?,
                tree([fresh_ledger, carried_ledger])?,
            )?;
            let covered_runs = COVERED_RUN_BATCHES
                .into_iter()
                .map(|batch| encoded_run(partition, batch, owner))
                .collect::<TestResult<Vec<_>>>()?;
            let fence = EncodedWal::new(&WalObject::new(
                partition,
                BatchId::try_from(FENCE_BATCH)?,
                owner,
                WalBody::Fence,
            ))?;
            let after_cut = encoded_run(partition, AFTER_CUT_BATCH, owner)?;
            let tables = vec![
                dropped_directory,
                carried_directory,
                dropped_ledger,
                carried_ledger,
                fresh_directory,
                fresh_ledger,
                unrelated,
            ];
            let dead = covered_runs
                .iter()
                .map(|wal| object_key(WalKey::from(wal.batch())))
                .chain([
                    object_key(TableKey::new(dropped_directory.object())),
                    object_key(TableKey::new(dropped_ledger.object())),
                ])
                .collect();
            let preserved = vec![
                object_key(WalKey::from(fence.batch())),
                object_key(WalKey::from(after_cut.batch())),
                object_key(TableKey::new(carried_directory.object())),
                object_key(TableKey::new(carried_ledger.object())),
                object_key(TableKey::new(fresh_directory.object())),
                object_key(TableKey::new(fresh_ledger.object())),
                object_key(TableKey::new(unrelated.object())),
                object_key(SealKey::from(source_generation)),
                object_key(SealKey::from(successor_generation)),
            ];
            Ok(Self {
                source,
                successor,
                covered_runs,
                fence,
                after_cut,
                tables,
                dead,
                preserved,
            })
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ObjectState {
        Present,
        Absent,
    }

    #[derive(Debug, Clone, Copy)]
    enum InvalidTransition {
        CrossPartition,
        SkippedGeneration,
        NonAdvancingCut,
        GenesisSuccessor,
    }

    impl InvalidTransition {
        fn seals(self, source: &Seal, successor: &Seal) -> TestResult<(Seal, Seal)> {
            let (source, successor) = match self {
                Self::CrossPartition => {
                    let partition = "11112222-3333-4444-8888-9999aaaabbbb".parse()?;
                    let invalid = Seal::new(
                        partition,
                        successor.generation(),
                        successor.replay(),
                        successor.directory().clone(),
                        successor.ledger().clone(),
                    )?;
                    (source.clone(), invalid)
                }
                Self::SkippedGeneration => {
                    let generation = successor.generation().successor()?;
                    let invalid = Seal::new(
                        successor.partition(),
                        generation,
                        successor.replay(),
                        successor.directory().clone(),
                        successor.ledger().clone(),
                    )?;
                    (source.clone(), invalid)
                }
                Self::NonAdvancingCut => {
                    let cut = BatchId::try_from(CUT_BATCH)?;
                    let invalid_source = Seal::new(
                        source.partition(),
                        source.generation(),
                        WalReplayPoint::Through {
                            batch: cut,
                            owner: OwnerToken::from(SealGeneration::genesis()),
                        },
                        source.directory().clone(),
                        source.ledger().clone(),
                    )?;
                    (invalid_source, successor.clone())
                }
                Self::GenesisSuccessor => {
                    let invalid = Seal::new(
                        successor.partition(),
                        successor.generation(),
                        WalReplayPoint::Genesis,
                        successor.directory().clone(),
                        successor.ledger().clone(),
                    )?;
                    (source.clone(), invalid)
                }
            };
            Ok((source, successor))
        }
    }

    fn encoded_run(
        partition: PartitionId,
        batch: u64,
        owner: OwnerToken,
    ) -> TestResult<EncodedWal> {
        let fact = OperationFact::StreamCreated {
            path: directory_key(&format!("collection/run-{batch}"))?,
            uid: StreamUid::try_from(batch)?,
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Open,
        };
        Ok(EncodedWal::new(&WalObject::new(
            partition,
            BatchId::try_from(batch)?,
            owner,
            WalBody::Run(WalFacts::try_from(vec![fact])?),
        ))?)
    }

    async fn plant_table(adapter: &ObjectStoreAdapter, table: &TableRef) -> TestResult {
        let key = object_key(TableKey::new(table.object()));
        let body = FrozenBytes::try_from(TABLE_MARKER.to_vec())?;
        assert_eq!(
            CreateEvidence::Direct,
            adapter.create_if_absent(&key, body).await?
        );
        Ok(())
    }

    fn table_ref(
        generation: SealGeneration,
        owner: SealGeneration,
        ordinal: u32,
        store: StoreKind,
    ) -> TestResult<TableRef> {
        let fresh = FreshIdentity::new(generation, AttemptId::new(owner, 0), ordinal)?;
        let object_bytes = u64::try_from(TABLE_MARKER.len())
            .ok()
            .and_then(NonZeroU64::new)
            .expect("the table marker has a nonzero length representable by u64");
        Ok(TableRef::new(
            TableObjectId::new(fresh, store),
            object_bytes,
        )?)
    }

    fn tree(tables: impl IntoIterator<Item = TableRef>) -> TestResult<TreeVersion> {
        Ok(TreeVersion::try_from(vec![SortedRun::try_from(
            tables.into_iter().collect::<Vec<_>>(),
        )?])?)
    }

    fn partition() -> PartitionId {
        "00112233-4455-6677-8899-aabbccddeeff"
            .parse()
            .expect("test partition is canonical")
    }

    fn directory_key(raw: &str) -> TestResult<DirectoryKey> {
        Ok(DirectoryKey::try_from(Box::<[u8]>::from(raw.as_bytes()))?)
    }

    fn object_key(spelling: impl std::fmt::Display) -> ObjectKey {
        ObjectKey::try_from(spelling.to_string())
            .expect("canonical storage-domain spelling is a valid object key")
    }
}
