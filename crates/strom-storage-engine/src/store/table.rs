//! Typed SST store for checkpoint children and Seal-selected tables.

use std::collections::BTreeSet;
use strom_object_store::{ByteBound, CreateEvidence, ObjectStoreAdapter, PutBody};
use strom_storage_domain::{
    DecodedTable, EncodedTable, PartitionId, StoreKind, TableKey, TableObjectId, TableRef,
    decode_directory_sst, decode_ledger_sst,
};
use strom_storage_protocol::CollectionInput;

use super::{TypedStoreError, object_key, typed_store_contradiction, typed_store_error};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateTableEvidence {
    Match,
    Foreign,
    Absent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TableEstablishment {
    Established,
    Abandoned,
    Contradiction { detail: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AuthorizedTableDelete {
    object: TableObjectId,
}

impl AuthorizedTableDelete {
    pub(crate) fn dropped_by(input: &CollectionInput) -> Vec<Self> {
        let successor_objects: BTreeSet<TableObjectId> = input
            .successor()
            .tables()
            .map(|table| table.object())
            .collect();
        input
            .source()
            .tables()
            .map(|table| table.object())
            .filter(|object| !successor_objects.contains(object))
            .map(|object| Self { object })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TableStore {
    adapter: ObjectStoreAdapter,
}

impl TableStore {
    #[must_use]
    pub(crate) const fn new(adapter: ObjectStoreAdapter) -> Self {
        Self { adapter }
    }

    pub(crate) async fn establish_table(&self, candidate: &EncodedTable) -> TableEstablishment {
        match self.create_table(candidate).await {
            Ok(CreateEvidence::Direct | CreateEvidence::DurableMatch) => {
                TableEstablishment::Established
            }
            Ok(CreateEvidence::NotOurs) => TableEstablishment::Contradiction {
                detail: "foreign bytes occupy a fresh checkpoint table identity".into(),
            },
            Ok(CreateEvidence::Unresolved) => match self.reconcile_table(candidate).await {
                Ok(CandidateTableEvidence::Match) => TableEstablishment::Established,
                Ok(CandidateTableEvidence::Foreign) => TableEstablishment::Contradiction {
                    detail: "an unresolved fresh checkpoint table contains foreign bytes".into(),
                },
                Ok(CandidateTableEvidence::Absent)
                | Err(TypedStoreError::Retryable { .. } | TypedStoreError::Rejected { .. }) => {
                    TableEstablishment::Abandoned
                }
                Err(TypedStoreError::Contradiction { detail }) => {
                    TableEstablishment::Contradiction { detail }
                }
            },
            Err(TypedStoreError::Retryable { .. } | TypedStoreError::Rejected { .. }) => {
                TableEstablishment::Abandoned
            }
            Err(TypedStoreError::Contradiction { detail }) => {
                TableEstablishment::Contradiction { detail }
            }
        }
    }

    async fn create_table(
        &self,
        candidate: &EncodedTable,
    ) -> Result<CreateEvidence, TypedStoreError> {
        let key = object_key(candidate.key());
        self.adapter
            .create_if_absent(
                &key,
                PutBody::try_from(candidate.bytes().clone())
                    .expect("an encoded SST fits the adapter PUT bound"),
            )
            .await
            .map_err(typed_store_error)
    }

    async fn reconcile_table(
        &self,
        candidate: &EncodedTable,
    ) -> Result<CandidateTableEvidence, TypedStoreError> {
        let key = object_key(candidate.key());
        let bound = ByteBound::try_from(candidate.table().object_bytes().get())
            .expect("a TableRef carries a nonzero in-bound object length");
        let observed = self
            .adapter
            .read(&key, bound)
            .await
            .map_err(typed_store_error)?;
        Ok(match observed {
            Some(observed) if observed.body() == candidate.bytes().as_ref() => {
                CandidateTableEvidence::Match
            }
            Some(_foreign) => CandidateTableEvidence::Foreign,
            None => CandidateTableEvidence::Absent,
        })
    }

    pub(crate) async fn delete_table(
        &self,
        proof: AuthorizedTableDelete,
    ) -> Result<(), TypedStoreError> {
        let key = object_key(TableKey::new(proof.object));
        self.adapter
            .delete_idempotent(&key)
            .await
            .map_err(typed_store_error)
    }

    pub(crate) async fn read_table(
        &self,
        partition: PartitionId,
        table: &TableRef,
    ) -> Result<DecodedTable, TypedStoreError> {
        let key = TableKey::new(table.object());
        let object_key = object_key(key);
        let bound = ByteBound::try_from(table.object_bytes().get())
            .expect("a TableRef carries a nonzero in-bound object length");
        let Some(observed) = self
            .adapter
            .read(&object_key, bound)
            .await
            .map_err(typed_store_error)?
        else {
            return Err(typed_store_contradiction(format!(
                "Seal-selected table {key} is absent"
            )));
        };
        let bytes_actual = u64::try_from(observed.body().len()).unwrap_or(u64::MAX);
        if bytes_actual != table.object_bytes().get() {
            return Err(typed_store_contradiction(format!(
                "table {key} has {bytes_actual} bytes; its Seal records {}",
                table.object_bytes()
            )));
        }

        match table.object().store() {
            StoreKind::Directory => decode_directory_sst(partition, &key, observed.body())
                .map(DecodedTable::Directory)
                .map_err(|source| {
                    typed_store_contradiction(format!(
                        "Directory table {key} failed checked decode: {source}"
                    ))
                }),
            StoreKind::Ledger => decode_ledger_sst(partition, &key, observed.body())
                .map(DecodedTable::Ledger)
                .map_err(|source| {
                    typed_store_contradiction(format!(
                        "Ledger table {key} failed checked decode: {source}"
                    ))
                }),
            StoreKind::Tally | StoreKind::Annals => Err(typed_store_contradiction(format!(
                "table {key} names a store with no resident decoder"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use object_store::path::Path;
    use object_store::{ObjectStoreExt as _, PutPayload};
    use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle, StreamPath};
    use strom_object_store::test_support::{BackendFailure, Fault, FaultStore, Selection, Target};
    use strom_object_store::{CreateEvidence, PutBody};
    use strom_storage_domain::{
        AttemptId, DecodedTable, DirectoryEntry, EncodedTable, FreshIdentity, LedgerCell,
        SealGeneration, StreamRecord, StreamUid, TableObjectId, encode_directory_sst,
        encode_ledger_sst,
    };

    use super::*;

    #[tokio::test]
    async fn table_establishment_decides_direct_match_and_foreign_evidence() {
        let adapter = ObjectStoreAdapter::in_memory();
        let store = TableStore::new(adapter);
        let candidate = candidate_table(&[1]);
        assert_eq!(
            TableEstablishment::Established,
            store.establish_table(&candidate).await,
            "a direct content create establishes the table"
        );
        assert_eq!(
            TableEstablishment::Established,
            store.establish_table(&candidate).await,
            "matching durable content establishes the table"
        );
        let foreign = candidate_table(&[2]);
        assert!(
            matches!(
                store.establish_table(&foreign).await,
                TableEstablishment::Contradiction { .. }
            ),
            "foreign bytes at a fresh table identity are contradictory"
        );
    }

    #[tokio::test]
    async fn unresolved_table_establishment_reconciles_match_foreign_and_absence()
    -> Result<(), Box<dyn std::error::Error>> {
        let candidate = candidate_table(&[1]);
        let key = object_key(candidate.key());
        let matching_fault = FaultStore::new().inject(Fault::CreateThenLoseResponse {
            target: Target::Key(key.clone()),
        })?;
        let matching = TableStore::new(ObjectStoreAdapter::new(matching_fault.backend()));
        assert_eq!(
            TableEstablishment::Established,
            matching.establish_table(&candidate).await
        );
        matching_fault.verify()?;

        let foreign_fault = FaultStore::new().inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(key.clone())),
            failure: BackendFailure::Transport,
        })?;
        let foreign_backend = foreign_fault.backend();
        foreign_backend
            .put(&Path::from(key.as_str()), PutPayload::from_static(&[2]))
            .await?;
        let foreign = TableStore::new(ObjectStoreAdapter::new(foreign_backend));
        assert!(matches!(
            foreign.establish_table(&candidate).await,
            TableEstablishment::Contradiction { .. }
        ));
        foreign_fault.verify()?;

        let absent_fault = FaultStore::new().inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(key)),
            failure: BackendFailure::Transport,
        })?;
        let absent = TableStore::new(ObjectStoreAdapter::new(absent_fault.backend()));
        assert_eq!(
            TableEstablishment::Abandoned,
            absent.establish_table(&candidate).await
        );
        absent_fault.verify()?;
        Ok(())
    }

    #[tokio::test]
    async fn table_store_failures_map_to_abandon_or_contradiction()
    -> Result<(), Box<dyn std::error::Error>> {
        let candidate = candidate_table(&[1]);
        let key = object_key(candidate.key());
        for failure in [
            BackendFailure::PermissionDenied,
            BackendFailure::Unauthenticated,
        ] {
            let fault_store = FaultStore::new().inject(Fault::FailBefore {
                selection: Selection::create(Target::Key(key.clone())),
                failure,
            })?;
            let store = TableStore::new(ObjectStoreAdapter::new(fault_store.backend()));
            assert_eq!(
                TableEstablishment::Abandoned,
                store.establish_table(&candidate).await
            );
            fault_store.verify()?;
        }

        let retryable_fault = FaultStore::new()
            .inject(Fault::FailBefore {
                selection: Selection::create(Target::Key(key.clone())),
                failure: BackendFailure::Transport,
            })?
            .inject(Fault::FailBefore {
                selection: Selection::read(Target::Key(key.clone())),
                failure: BackendFailure::Transport,
            })?;
        let retryable = TableStore::new(ObjectStoreAdapter::new(retryable_fault.backend()));
        assert_eq!(
            TableEstablishment::Abandoned,
            retryable.establish_table(&candidate).await
        );
        retryable_fault.verify()?;

        let contradiction_fault = FaultStore::new().inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(key.clone())),
            failure: BackendFailure::Transport,
        })?;
        let backend = contradiction_fault.backend();
        backend
            .put(&Path::from(key.as_str()), PutPayload::from_static(&[1, 2]))
            .await?;
        let store = TableStore::new(ObjectStoreAdapter::new(backend));
        assert!(matches!(
            store.establish_table(&candidate).await,
            TableEstablishment::Contradiction { .. }
        ));
        contradiction_fault.verify()?;
        Ok(())
    }

    #[tokio::test]
    async fn exact_length_directory_table_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let adapter = ObjectStoreAdapter::in_memory();
        let store = TableStore::new(adapter.clone());
        let key = table_key(StoreKind::Directory)?;
        let rows = vec![(
            stream_path("events/a")?,
            DirectoryEntry::Live(StreamUid::try_from(1)?),
        )];
        let bytes = encode_directory_sst(partition(), &key, &rows)?;
        let table = table_ref(key, bytes.len())?;
        plant(&adapter, key, bytes).await?;

        assert_eq!(
            store.read_table(partition(), &table).await?,
            DecodedTable::Directory(rows),
            "the typed store selects the Directory decoder and exact identity"
        );
        Ok(())
    }

    #[tokio::test]
    async fn exact_length_ledger_table_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let adapter = ObjectStoreAdapter::in_memory();
        let store = TableStore::new(adapter.clone());
        let key = table_key(StoreKind::Ledger)?;
        let rows = vec![(
            StreamUid::try_from(1)?,
            LedgerCell::Value(StreamRecord::new(
                StreamContentType::octet_stream(),
                ExpiryPolicy::None,
                StreamLifecycle::Open,
                strom_storage_domain::BatchId::try_from(1)?,
            )),
        )];
        let bytes = encode_ledger_sst(partition(), &key, &rows)?;
        let table = table_ref(key, bytes.len())?;
        plant(&adapter, key, bytes).await?;

        assert_eq!(
            DecodedTable::Ledger(rows),
            store.read_table(partition(), &table).await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn wrong_store_wrong_identity_and_garbage_are_contradictions()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory_table_key = table_key_at(StoreKind::Directory, 0)?;
        let ledger_key = table_key_at(StoreKind::Ledger, 1)?;
        let ledger_rows = vec![(StreamUid::try_from(1)?, LedgerCell::Delete)];
        let ledger_bytes = encode_ledger_sst(partition(), &ledger_key, &ledger_rows)?;
        let wrong_store = ObjectStoreAdapter::in_memory();
        plant(&wrong_store, directory_table_key, ledger_bytes.clone()).await?;
        assert!(matches!(
            TableStore::new(wrong_store)
                .read_table(
                    partition(),
                    &table_ref(directory_table_key, ledger_bytes.len())?
                )
                .await,
            Err(TypedStoreError::Contradiction { .. })
        ));

        let encoded_key = table_key_at(StoreKind::Directory, 2)?;
        let planted_key = table_key_at(StoreKind::Directory, 3)?;
        let rows = vec![(
            stream_path("events/a")?,
            DirectoryEntry::Live(StreamUid::try_from(1)?),
        )];
        let wrong_identity_bytes = encode_directory_sst(partition(), &encoded_key, &rows)?;
        let wrong_identity = ObjectStoreAdapter::in_memory();
        plant(&wrong_identity, planted_key, wrong_identity_bytes.clone()).await?;
        assert!(matches!(
            TableStore::new(wrong_identity)
                .read_table(
                    partition(),
                    &table_ref(planted_key, wrong_identity_bytes.len())?
                )
                .await,
            Err(TypedStoreError::Contradiction { .. })
        ));

        let garbage_key = table_key_at(StoreKind::Directory, 4)?;
        let garbage = vec![0x5a; 64];
        let garbage_adapter = ObjectStoreAdapter::in_memory();
        plant(&garbage_adapter, garbage_key, garbage.clone()).await?;
        assert!(matches!(
            TableStore::new(garbage_adapter)
                .read_table(partition(), &table_ref(garbage_key, garbage.len())?)
                .await,
            Err(TypedStoreError::Contradiction { .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn absence_and_both_length_disagreements_are_contradictions()
    -> Result<(), Box<dyn std::error::Error>> {
        let key = table_key(StoreKind::Directory)?;
        let rows = vec![(
            stream_path("events/a")?,
            DirectoryEntry::Live(StreamUid::try_from(1)?),
        )];
        let bytes = encode_directory_sst(partition(), &key, &rows)?;

        let absent_store = TableStore::new(ObjectStoreAdapter::in_memory());
        let exact = table_ref(key, bytes.len())?;
        assert!(matches!(
            absent_store.read_table(partition(), &exact).await,
            Err(TypedStoreError::Contradiction { .. })
        ));

        let short_adapter = ObjectStoreAdapter::in_memory();
        plant(&short_adapter, key, bytes.clone()).await?;
        let recorded_longer = table_ref(
            key,
            bytes.len().checked_add(1).expect("fixture length fits"),
        )?;
        assert!(matches!(
            TableStore::new(short_adapter)
                .read_table(partition(), &recorded_longer)
                .await,
            Err(TypedStoreError::Contradiction { .. })
        ));

        let long_adapter = ObjectStoreAdapter::in_memory();
        plant(&long_adapter, key, bytes).await?;
        let recorded_shorter = table_ref(
            key,
            exact
                .object_bytes()
                .get()
                .checked_sub(1)
                .and_then(|length| usize::try_from(length).ok())
                .expect("encoded fixture has more than one byte"),
        )?;
        assert!(matches!(
            TableStore::new(long_adapter)
                .read_table(partition(), &recorded_shorter)
                .await,
            Err(TypedStoreError::Contradiction { .. })
        ));
        Ok(())
    }

    async fn plant(
        adapter: &ObjectStoreAdapter,
        key: TableKey,
        bytes: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let evidence = adapter
            .create_if_absent(&object_key(key), PutBody::try_from(bytes)?)
            .await?;
        assert_eq!(CreateEvidence::Direct, evidence);
        Ok(())
    }

    fn table_ref(key: TableKey, bytes: usize) -> Result<TableRef, Box<dyn std::error::Error>> {
        let bytes = u64::try_from(bytes)?;
        Ok(TableRef::new(
            key.object(),
            NonZeroU64::new(bytes).expect("encoded SSTs are nonempty"),
        )?)
    }

    fn candidate_table(bytes: &[u8]) -> EncodedTable {
        let marker = bytes
            .first()
            .copied()
            .expect("the fixture marker is nonempty");
        let path = stream_path(&format!("events/{marker}"))
            .expect("the fixture path is a valid Directory key");
        let rows = [(
            path,
            DirectoryEntry::Live(StreamUid::try_from(1).expect("one is nonzero")),
        )];
        EncodedTable::encode_directory(
            partition(),
            table_key(StoreKind::Directory).expect("test table identity is valid"),
            &rows,
        )
        .expect("the fixture table encodes")
    }

    fn stream_path(raw: &str) -> Result<StreamPath, Box<dyn std::error::Error>> {
        Ok(raw.parse()?)
    }

    fn partition() -> PartitionId {
        "00112233-4455-6677-8899-aabbccddeeff"
            .parse()
            .expect("test partition is canonical")
    }

    fn table_key(store: StoreKind) -> Result<TableKey, Box<dyn std::error::Error>> {
        table_key_at(store, 0)
    }

    fn table_key_at(
        store: StoreKind,
        ordinal: u32,
    ) -> Result<TableKey, Box<dyn std::error::Error>> {
        let owner = SealGeneration::genesis();
        let birth = owner.successor()?;
        let fresh = FreshIdentity::new(birth, AttemptId::new(owner, 7), ordinal)?;
        Ok(TableKey::new(TableObjectId::new(fresh, store)))
    }
}
