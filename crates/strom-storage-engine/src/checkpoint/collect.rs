//! Best-effort collection after an advancing Seal publication.

use strom_storage_domain::BatchId;
use strom_storage_protocol::CollectionInput;

use crate::store::{AuthorizedTableDelete, TableStore, WalStore};

pub(super) async fn collect(wal_store: WalStore, table_store: TableStore, input: CollectionInput) {
    let partition = input.source().partition();
    let cut = input.cut();
    let first = input.source().replay().batch().map_or_else(
        || BatchId::try_from(1).expect("batch one is a legal WAL coordinate"),
        |previous| {
            previous
                .successor()
                .expect("a source cut below its prepared successor cut has a successor")
        },
    );
    let table_deletes = AuthorizedTableDelete::dropped_by(&input);
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

    for proof in table_deletes {
        if table_store.delete_table(proof).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt as _};
    use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle, StreamPath};
    use strom_object_store::test_support::{
        BackendFailure, Fault, FaultStore, Operation, Selection, Target,
    };
    use strom_object_store::{CreateEvidence, ObjectKey, ObjectStoreAdapter, PutBody};
    use strom_storage_domain::{
        AttemptId, DecodedTable, EncodedAuthoritySeal, FreshIdentity, OperationFact, OwnerToken,
        PartitionId, Seal, SealGeneration, SealKey, SortedRun, StoreKind, StreamRecord, StreamUid,
        TableKey, TableObjectId, TableRef, TreeVersion, WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER,
        WalBody, WalFacts, WalKey, WalObject, WalReplayPoint, encode_seal, encode_wal,
    };
    use strom_storage_protocol::{
        BootstrapEffect, BootstrapEvent, BootstrapMachine, BootstrapStep, CollectionInput,
        PreparationOutcome, PreparedCheckpoint, SealPublication, WalEstablishment, WriterEffect,
        WriterEvent, WriterMachine, WriterOutput,
    };

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    const COVERED_RUN_BATCHES: [u64; 2] = [1, CUT_BATCH];
    const FENCE_BATCH: u64 = 2;
    const CUT_BATCH: u64 = WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER;
    const AFTER_CUT_BATCH: u64 = CUT_BATCH + 1;
    const TABLE_MARKER: &[u8] = b"collection table";

    #[tokio::test]
    async fn complete_collection_deletes_only_covered_runs_and_dropped_source_tables() -> TestResult
    {
        let fixture = CollectionFixture::plant(CollectionState::new()?, None).await?;

        run_collection(fixture.adapter.clone(), &fixture.state).await?;

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
        run_collection(fixture.adapter.clone(), &fixture.state).await?;

        fixture.assert_preserved().await?;
        fixture.assert_state(&target, target_state).await?;

        run_collection(
            ObjectStoreAdapter::new(Arc::clone(&fixture.backend)),
            &fixture.state,
        )
        .await?;

        fixture.assert_complete().await?;
        fixture.store.verify()?;
        Ok(())
    }

    async fn run_collection(adapter: ObjectStoreAdapter, state: &CollectionState) -> TestResult {
        collect(
            WalStore::new(adapter.clone()),
            TableStore::new(adapter),
            mint_collection_input(state)?,
        )
        .await;
        Ok(())
    }

    struct CollectionFixture {
        store: FaultStore,
        backend: Arc<dyn ObjectStore>,
        adapter: ObjectStoreAdapter,
        state: CollectionState,
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
            for wal in state
                .covered_runs
                .iter()
                .chain([&state.fence, &state.after_cut])
            {
                plant_wal(&adapter, wal).await?;
            }
            for table in &state.tables {
                plant_table(&adapter, table).await?;
            }
            for seal in [&state.source, &state.successor] {
                plant_seal(&adapter, seal).await?;
            }

            Ok(Self {
                store,
                backend,
                adapter,
                state: state.clone(),
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
        head: Seal,
        source: Seal,
        successor: Seal,
        covered_runs: Vec<WalObject>,
        fence: WalObject,
        after_cut: WalObject,
        tables: Vec<TableRef>,
        dead: Vec<ObjectKey>,
        preserved: Vec<ObjectKey>,
    }

    impl CollectionState {
        fn new() -> TestResult<Self> {
            let partition = partition();
            let head_generation = SealGeneration::try_from(CUT_BATCH)?;
            let source_generation = head_generation.successor()?;
            let successor_generation = source_generation.successor()?;
            let dropped_directory = table_ref(
                head_generation,
                SealGeneration::genesis(),
                0,
                StoreKind::Directory,
            )?;
            let carried_directory = table_ref(
                head_generation,
                SealGeneration::genesis(),
                1,
                StoreKind::Directory,
            )?;
            let dropped_ledger = table_ref(
                head_generation,
                SealGeneration::genesis(),
                0,
                StoreKind::Ledger,
            )?;
            let carried_ledger = table_ref(
                head_generation,
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
            let head = Seal::new(
                partition,
                head_generation,
                WalReplayPoint::Genesis,
                tree([dropped_directory, carried_directory])?,
                tree([dropped_ledger, carried_ledger])?,
            )?;
            let source = head.claim_successor()?;
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
                .map(|batch| wal_run(partition, batch, owner))
                .collect::<TestResult<Vec<_>>>()?;
            let fence = WalObject::new(
                partition,
                BatchId::try_from(FENCE_BATCH)?,
                owner,
                WalBody::Fence,
            );
            let after_cut = wal_run(partition, AFTER_CUT_BATCH, owner)?;
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
                head,
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

    fn mint_collection_input(state: &CollectionState) -> TestResult<CollectionInput> {
        let mut bootstrap = BootstrapMachine::new();
        let step = bootstrap.handle(BootstrapEvent::Started {
            genesis_partition: state.head.partition(),
        });
        assert!(matches!(
            step,
            BootstrapStep::Effect(BootstrapEffect::ObserveHead)
        ));
        let step = bootstrap.handle(BootstrapEvent::HeadObserved(Some(state.head.generation())));
        assert!(matches!(
            step,
            BootstrapStep::Effect(BootstrapEffect::ReadSeal { .. })
        ));
        let step = bootstrap.handle(BootstrapEvent::SealRead(Some(state.head.clone())));
        assert!(matches!(
            step,
            BootstrapStep::Effect(BootstrapEffect::PublishClaim(_))
        ));
        let mut step = bootstrap.handle(BootstrapEvent::ClaimPublished(SealPublication::Authored));
        loop {
            let BootstrapStep::Effect(BootstrapEffect::ReadTable { table, .. }) = step else {
                break;
            };
            let ordinal = table.object().fresh().ordinal();
            let uid = StreamUid::try_from(u64::from(ordinal) + 1)?;
            let decoded = match table.object().store() {
                StoreKind::Directory => DecodedTable::Directory(vec![(
                    stream_path(&format!("collection/base-{ordinal}"))?,
                    strom_storage_domain::DirectoryEntry::Live(uid),
                )]),
                StoreKind::Ledger => DecodedTable::Ledger(vec![(
                    uid,
                    strom_storage_domain::LedgerCell::Value(StreamRecord::new(
                        StreamContentType::octet_stream(),
                        ExpiryPolicy::None,
                        StreamLifecycle::Open,
                        BatchId::try_from(1)?,
                    )),
                )]),
                StoreKind::Tally | StoreKind::Annals => {
                    return Err("collection fixture selected an unsupported table".into());
                }
            };
            step = bootstrap.handle(BootstrapEvent::TableRead { table, decoded });
        }
        assert!(matches!(
            step,
            BootstrapStep::Effect(BootstrapEffect::ObserveWalTail)
        ));
        let tail = BatchId::try_from(CUT_BATCH - 1)?;
        let step = bootstrap.handle(BootstrapEvent::WalTailObserved(Some(tail)));
        assert!(matches!(
            step,
            BootstrapStep::Effect(BootstrapEffect::ReadWal { batch, .. }) if batch == tail
        ));
        let step = bootstrap.handle(BootstrapEvent::WalRead(Some(WalObject::new(
            state.head.partition(),
            tail,
            OwnerToken::from(SealGeneration::genesis()),
            WalBody::Fence,
        ))));
        assert!(matches!(
            step,
            BootstrapStep::Effect(BootstrapEffect::EstablishFence(_))
        ));
        let mut step =
            bootstrap.handle(BootstrapEvent::FenceEstablished(WalEstablishment::Durable));
        for raw_batch in 1..=CUT_BATCH {
            let batch = BatchId::try_from(raw_batch)?;
            assert!(
                matches!(
                    step,
                    BootstrapStep::Effect(BootstrapEffect::ReadWal { batch: requested, .. })
                        if requested == batch
                ),
                "bootstrap expected replay batch {raw_batch}, got {step:?}"
            );
            step = bootstrap.handle(BootstrapEvent::WalRead(Some(WalObject::new(
                state.head.partition(),
                batch,
                if raw_batch == CUT_BATCH {
                    OwnerToken::from(state.source.generation())
                } else {
                    OwnerToken::from(SealGeneration::try_from(raw_batch)?)
                },
                WalBody::Fence,
            ))));
        }
        assert!(matches!(
            step,
            BootstrapStep::Effect(BootstrapEffect::ObserveHead)
        ));
        let BootstrapStep::Complete(recovery) = bootstrap.handle(BootstrapEvent::HeadObserved(
            Some(state.source.generation()),
        )) else {
            return Err("collection fixture did not complete bootstrap".into());
        };

        let mut writer = WriterMachine::from_recovery(recovery);
        let (outputs, exit) = writer.handle(WriterEvent::Started).into_parts();
        assert_eq!(None, exit);
        let mut preparations = outputs.into_iter().filter_map(|output| match output {
            WriterOutput::Effect(WriterEffect::PrepareCheckpoint(input)) => Some(input),
            WriterOutput::Effect(
                WriterEffect::EstablishWal(_)
                | WriterEffect::PublishAuthority { .. }
                | WriterEffect::Collect(_),
            )
            | WriterOutput::Action(_) => None,
        });
        let input = preparations
            .next()
            .ok_or("collection fixture did not issue checkpoint preparation")?;
        assert!(preparations.next().is_none());
        let ticket = input.ticket();
        let (_ticket, source, _base, snapshot) = input.into_parts();
        assert_eq!(&source, &state.source);
        let candidate = EncodedAuthoritySeal::try_from(&state.successor)?;
        let prepared =
            PreparedCheckpoint::new(ticket, source, state.successor.clone(), snapshot, candidate);
        let (outputs, exit) = writer
            .handle(WriterEvent::CheckpointPrepared {
                ticket,
                outcome: PreparationOutcome::Prepared(Box::new(prepared)),
            })
            .into_parts();
        assert_eq!(None, exit);
        assert!(outputs.iter().any(|output| matches!(
            output,
            WriterOutput::Effect(WriterEffect::PublishAuthority { ticket: issued, .. })
                if *issued == ticket
        )));
        let (outputs, exit) = writer
            .handle(WriterEvent::SealPublished {
                ticket,
                result: Ok(SealPublication::Authored),
            })
            .into_parts();
        assert_eq!(None, exit);
        outputs
            .into_iter()
            .find_map(|output| match output {
                WriterOutput::Effect(WriterEffect::Collect(input)) => Some(input),
                WriterOutput::Effect(
                    WriterEffect::EstablishWal(_)
                    | WriterEffect::PrepareCheckpoint(_)
                    | WriterEffect::PublishAuthority { .. },
                )
                | WriterOutput::Action(_) => None,
            })
            .ok_or_else(|| "authored publication did not mint collection authority".into())
    }

    fn wal_run(partition: PartitionId, batch: u64, owner: OwnerToken) -> TestResult<WalObject> {
        let fact = OperationFact::StreamCreated {
            path: stream_path(&format!("collection/run-{batch}"))?,
            uid: StreamUid::try_from(batch)?,
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Open,
        };
        Ok(WalObject::new(
            partition,
            BatchId::try_from(batch)?,
            owner,
            WalBody::Run(WalFacts::try_from(vec![fact])?),
        ))
    }

    async fn plant_wal(adapter: &ObjectStoreAdapter, wal: &WalObject) -> TestResult {
        let key = object_key(WalKey::from(wal.batch()));
        let body = PutBody::try_from(encode_wal(wal)?)?;
        assert_eq!(
            CreateEvidence::Direct,
            adapter.create_if_absent(&key, body).await?
        );
        Ok(())
    }

    async fn plant_seal(adapter: &ObjectStoreAdapter, seal: &Seal) -> TestResult {
        let key = object_key(SealKey::from(seal.generation()));
        let body = PutBody::try_from(encode_seal(seal)?)?;
        assert_eq!(
            CreateEvidence::Direct,
            adapter.create_if_absent(&key, body).await?
        );
        Ok(())
    }

    async fn plant_table(adapter: &ObjectStoreAdapter, table: &TableRef) -> TestResult {
        let key = object_key(TableKey::new(table.object()));
        let body = PutBody::try_from(TABLE_MARKER.to_vec())?;
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

    fn stream_path(raw: &str) -> TestResult<StreamPath> {
        Ok(raw.parse()?)
    }

    fn object_key(spelling: impl std::fmt::Display) -> ObjectKey {
        ObjectKey::try_from(spelling.to_string())
            .expect("canonical storage-domain spelling is a valid object key")
    }
}
