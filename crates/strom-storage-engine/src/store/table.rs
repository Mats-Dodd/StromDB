//! Typed SST store for checkpoint children and Seal-selected tables.

use std::collections::BTreeSet;
use std::num::NonZeroU64;

use strom_object_store::{ByteBound, CreateEvidence, FrozenBytes, ObjectStoreAdapter};
use strom_storage_domain::{
    DirectoryEntry, DirectoryKey, LedgerCell, PartitionId, Seal, StoreKind, StreamUid, TableKey,
    TableObjectId, TableRef, decode_directory_sst, decode_ledger_sst,
};

use super::{TypedStoreError, object_key};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TableRows {
    Directory(Vec<(DirectoryKey, DirectoryEntry)>),
    Ledger(Vec<(StreamUid, LedgerCell)>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EncodedTable {
    key: TableKey,
    table: TableRef,
    bytes: FrozenBytes,
}

impl EncodedTable {
    pub(crate) fn new(key: TableKey, bytes: Vec<u8>) -> Self {
        let object_bytes = u64::try_from(bytes.len())
            .ok()
            .and_then(NonZeroU64::new)
            .expect("an encoded SST has a nonzero length representable by u64");
        let table = TableRef::new(key.object(), object_bytes)
            .expect("the SST encoder enforces the hard object bound");
        let bytes = FrozenBytes::try_from(bytes)
            .expect("an encoded SST is nonempty and fits the adapter PUT bound");
        Self { key, table, bytes }
    }

    #[must_use]
    pub(crate) const fn table(&self) -> TableRef {
        self.table
    }

    #[must_use]
    pub(crate) fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CandidateTableEvidence {
    Match,
    Foreign,
    Absent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AuthorizedTableDelete {
    object: TableObjectId,
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

    pub(crate) async fn create_table(
        &self,
        candidate: &EncodedTable,
    ) -> Result<CreateEvidence, TypedStoreError> {
        let key = object_key(candidate.key);
        self.adapter
            .create_if_absent(&key, candidate.bytes.clone())
            .await
            .map_err(TypedStoreError::from_store)
    }

    pub(crate) async fn reconcile_table(
        &self,
        candidate: &EncodedTable,
    ) -> Result<CandidateTableEvidence, TypedStoreError> {
        let key = object_key(candidate.key);
        let bound = ByteBound::try_from(candidate.table.object_bytes().get())
            .expect("a TableRef carries a nonzero in-bound object length");
        let observed = self
            .adapter
            .read(&key, bound)
            .await
            .map_err(TypedStoreError::from_store)?;
        Ok(match observed {
            Some(observed) if observed.body() == candidate.as_slice() => {
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
            .map_err(TypedStoreError::from_store)
    }

    pub(crate) async fn read_table(
        &self,
        partition: PartitionId,
        table: &TableRef,
    ) -> Result<TableRows, TypedStoreError> {
        let key = TableKey::new(table.object());
        let object_key = object_key(key);
        let bound = ByteBound::try_from(table.object_bytes().get())
            .expect("a TableRef carries a nonzero in-bound object length");
        let Some(observed) = self
            .adapter
            .read(&object_key, bound)
            .await
            .map_err(TypedStoreError::from_store)?
        else {
            return Err(TypedStoreError::contradiction(format!(
                "Seal-selected table {key} is absent"
            )));
        };
        let bytes_actual = u64::try_from(observed.body().len()).unwrap_or(u64::MAX);
        if bytes_actual != table.object_bytes().get() {
            return Err(TypedStoreError::contradiction(format!(
                "table {key} has {bytes_actual} bytes; its Seal records {}",
                table.object_bytes()
            )));
        }

        match table.object().store() {
            StoreKind::Directory => decode_directory_sst(partition, &key, observed.body())
                .map(TableRows::Directory)
                .map_err(|source| {
                    TypedStoreError::contradiction(format!(
                        "Directory table {key} failed checked decode: {source}"
                    ))
                }),
            StoreKind::Ledger => decode_ledger_sst(partition, &key, observed.body())
                .map(TableRows::Ledger)
                .map_err(|source| {
                    TypedStoreError::contradiction(format!(
                        "Ledger table {key} failed checked decode: {source}"
                    ))
                }),
            StoreKind::Tally | StoreKind::Annals => Err(TypedStoreError::contradiction(format!(
                "table {key} names a store with no resident decoder"
            ))),
        }
    }
}

pub(crate) fn targeted_table_deletes(
    source: &Seal,
    successor: &Seal,
) -> Vec<AuthorizedTableDelete> {
    assert_eq!(
        source.partition(),
        successor.partition(),
        "one advance keeps the partition identity"
    );
    assert!(
        source.generation() < successor.generation(),
        "targeted collection compares a source with its advancing successor"
    );
    assert_eq!(
        source
            .generation()
            .successor()
            .expect("an observed successor proves the source generation is not exhausted"),
        successor.generation(),
        "targeted collection requires an exact Seal successor pair"
    );
    let successor_objects: BTreeSet<TableObjectId> = seal_tables(successor).collect();
    seal_tables(source)
        .filter(|object| !successor_objects.contains(object))
        .map(|object| AuthorizedTableDelete { object })
        .collect()
}

fn seal_tables(seal: &Seal) -> impl Iterator<Item = TableObjectId> + '_ {
    [seal.directory(), seal.ledger()]
        .into_iter()
        .flat_map(strom_storage_domain::TreeVersion::runs)
        .flat_map(strom_storage_domain::SortedRun::tables)
        .map(|table| table.object())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};
    use strom_object_store::{CreateEvidence, FrozenBytes};
    use strom_storage_domain::{
        AttemptId, FreshIdentity, SealGeneration, StreamRecord, StreamUid, TableObjectId,
        encode_directory_sst, encode_ledger_sst,
    };

    use super::*;

    #[tokio::test]
    async fn exact_length_directory_table_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let adapter = ObjectStoreAdapter::in_memory();
        let store = TableStore::new(adapter.clone());
        let key = table_key(StoreKind::Directory)?;
        let rows = vec![(
            directory_key("events/a")?,
            DirectoryEntry::Live(StreamUid::try_from(1)?),
        )];
        let bytes = encode_directory_sst(partition(), &key, &rows)?;
        let table = table_ref(key, bytes.len())?;
        plant(&adapter, key, bytes).await?;

        assert_eq!(
            store.read_table(partition(), &table).await?,
            TableRows::Directory(rows),
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
            TableRows::Ledger(rows),
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
            directory_key("events/a")?,
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
            directory_key("events/a")?,
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
            .create_if_absent(&object_key(key), FrozenBytes::try_from(bytes)?)
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

    fn directory_key(raw: &str) -> Result<DirectoryKey, Box<dyn std::error::Error>> {
        Ok(DirectoryKey::try_from(Box::<[u8]>::from(raw.as_bytes()))?)
    }

    fn partition() -> PartitionId {
        "00112233-4455-6677-8899-aabbccddeeff"
            .parse()
            .expect("test partition is canonical")
    }
}
