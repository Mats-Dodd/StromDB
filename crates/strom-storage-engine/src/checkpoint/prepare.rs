//! Pure advancing-checkpoint preparation.

use strom_domain::StreamPath;
use strom_storage_domain::{
    AttemptId, BatchId, DIRECTORY_ROW_ENCODED_BYTES_MAX, DirectoryEntry, FreshIdentity,
    LEDGER_DELETE_ROW_ENCODED_BYTES_MAX, LEDGER_VALUE_ROW_ENCODED_BYTES_MAX, LedgerCell,
    OwnerToken, PARTITION_BOOTSTRAP_BYTES_MAX_V2, PARTITION_BOOTSTRAP_OBJECTS_MAX_V2, PartitionId,
    SST_ARCHIVE_FIXED_BYTES_MAX, SST_TABLE_TARGET_BYTES, Seal, SealGeneration, SortedRun,
    StoreKind, StreamUid, TREE_RUNS_MAX, TableKey, TableObjectId, TableRef, TreeVersion,
    WalReplayPoint,
};
use strom_storage_domain::{EncodedAuthoritySeal, EncodedTable};
use strom_storage_protocol::{CheckpointInput, Forest, ForestDelta, PreparedCheckpoint};

#[derive(Debug, thiserror::Error)]
#[error("{detail}")]
pub(super) struct CheckpointContradiction {
    detail: String,
}

impl From<String> for CheckpointContradiction {
    fn from(detail: String) -> Self {
        Self { detail }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ChunkAccounting {
    tables: usize,
    bytes: u64,
}

pub(super) fn prepare_checkpoint(
    input: CheckpointInput,
    emit: &mut impl FnMut(EncodedTable) -> bool,
) -> Result<Option<Box<PreparedCheckpoint>>, CheckpointContradiction> {
    let (ticket, source, base, snapshot) = input.into_parts();
    let cut = ticket.cut();
    let attempt = ticket.attempt();
    let partition = source.partition();
    let owner_claim = attempt.owner_claim();
    let previous_cut = source.replay().batch();
    if previous_cut.is_some_and(|previous| cut <= previous) {
        return Err("checkpoint cut does not advance its source Seal"
            .to_owned()
            .into());
    }
    let generation = source
        .generation()
        .successor()
        .map_err(|error| error.to_string())?;

    let delta = snapshot.delta_since(&base);
    let (cells, carried) = plan_checkpoint(&source, &snapshot, delta);
    let mut ordinal = 0u32;
    let ForestDelta {
        directory: directory_rows,
        ledger: ledger_rows,
    } = cells;
    let Some(directory) = build_directory_tree(
        partition,
        generation,
        attempt,
        &mut ordinal,
        directory_rows,
        carried.map(Seal::directory),
        emit,
    ) else {
        return Ok(None);
    };
    let Some(ledger) = build_ledger_tree(
        partition,
        generation,
        attempt,
        &mut ordinal,
        ledger_rows,
        carried.map(Seal::ledger),
        emit,
    ) else {
        return Ok(None);
    };
    let (successor, encoded_seal) = assemble_checkpoint_seal(
        partition,
        generation,
        cut,
        OwnerToken::from(owner_claim),
        directory,
        ledger,
    );
    Ok(Some(Box::new(PreparedCheckpoint::new(
        ticket,
        source,
        successor,
        snapshot,
        encoded_seal,
    ))))
}

fn plan_checkpoint<'source>(
    source: &'source Seal,
    snapshot: &Forest,
    delta: ForestDelta,
) -> (ForestDelta, Option<&'source Seal>) {
    if can_append_delta(source, &delta) {
        return (delta, Some(source));
    }
    let cells = snapshot.checkpoint_cells();
    let directory = account_rows(
        cells
            .directory
            .iter()
            .map(|_row| DIRECTORY_ROW_ENCODED_BYTES_MAX),
    );
    let ledger = account_rows(
        cells
            .ledger
            .iter()
            .map(|(_uid, cell)| ledger_row_bytes(cell)),
    );
    assert_full_plan(directory, ledger);
    (cells, None)
}

fn can_append_delta(source: &Seal, delta: &ForestDelta) -> bool {
    let directory = account_rows(
        delta
            .directory
            .iter()
            .map(|_row| DIRECTORY_ROW_ENCODED_BYTES_MAX),
    );
    let ledger = account_rows(
        delta
            .ledger
            .iter()
            .map(|(_uid, cell)| ledger_row_bytes(cell)),
    );
    select_delta(source, directory, ledger)
}

#[cfg(test)]
fn plan_delta_rows(base: &Forest, snapshot: &Forest) -> PlannedRows {
    let delta = snapshot.delta_since(base);
    PlannedRows {
        directory: chunk_rows(delta.directory, |_row| DIRECTORY_ROW_ENCODED_BYTES_MAX),
        ledger: chunk_rows(delta.ledger, |(_uid, cell)| ledger_row_bytes(cell)),
    }
}

#[cfg(test)]
#[derive(Debug)]
struct PlannedRows {
    directory: Vec<Vec<(StreamPath, DirectoryEntry)>>,
    ledger: Vec<Vec<(StreamUid, LedgerCell)>>,
}

#[cfg(test)]
fn chunk_rows<Row>(
    rows: impl IntoIterator<Item = Row>,
    row_bytes: impl Fn(&Row) -> u64,
) -> Vec<Vec<Row>> {
    let mut chunks = Vec::new();
    for_each_chunk(rows, row_bytes, |chunk, _estimate| {
        chunks.push(chunk);
        true
    });
    chunks
}

fn account_rows(rows: impl IntoIterator<Item = u64>) -> ChunkAccounting {
    let mut accounting = ChunkAccounting::default();
    for_each_chunk(
        rows,
        |row_bytes| *row_bytes,
        |_rows, table_bytes| {
            accounting.tables = accounting
                .tables
                .checked_add(1)
                .expect("a checkpoint table count fits in usize");
            accounting.bytes = accounting
                .bytes
                .checked_add(table_bytes)
                .expect("a checkpoint byte estimate fits in u64");
            true
        },
    );
    accounting
}

fn select_delta(source: &Seal, directory: ChunkAccounting, ledger: ChunkAccounting) -> bool {
    let Some(directory_runs) = source
        .directory()
        .runs()
        .len()
        .checked_add(usize::from(directory.tables != 0))
    else {
        return false;
    };
    let Some(ledger_runs) = source
        .ledger()
        .runs()
        .len()
        .checked_add(usize::from(ledger.tables != 0))
    else {
        return false;
    };
    let carried_objects = source.tables().count();
    let Some(fresh_objects) = directory.tables.checked_add(ledger.tables) else {
        return false;
    };
    let carried_bytes = source
        .tables()
        .map(|table| table.object_bytes().get())
        .sum::<u64>();
    let Some(fresh_bytes) = directory.bytes.checked_add(ledger.bytes) else {
        return false;
    };
    directory_runs <= TREE_RUNS_MAX
        && ledger_runs <= TREE_RUNS_MAX
        && carried_objects
            .checked_add(fresh_objects)
            .is_some_and(|objects| objects <= PARTITION_BOOTSTRAP_OBJECTS_MAX_V2)
        && carried_bytes
            .checked_add(fresh_bytes)
            .is_some_and(|bytes| bytes <= PARTITION_BOOTSTRAP_BYTES_MAX_V2)
}

fn assert_full_plan(directory: ChunkAccounting, ledger: ChunkAccounting) {
    let objects = directory
        .tables
        .checked_add(ledger.tables)
        .expect("a full-base table count fits in usize");
    assert!(
        objects <= PARTITION_BOOTSTRAP_OBJECTS_MAX_V2,
        "encoded chunking keeps a maximum-capacity full base inside the object bound"
    );
    let bytes = directory
        .bytes
        .checked_add(ledger.bytes)
        .expect("a full-base byte estimate fits in u64");
    assert!(
        bytes <= PARTITION_BOOTSTRAP_BYTES_MAX_V2,
        "encoded chunking keeps a maximum-capacity full base inside the byte bound"
    );
}

fn build_directory_tree(
    partition: PartitionId,
    generation: SealGeneration,
    attempt: AttemptId,
    ordinal: &mut u32,
    rows: impl IntoIterator<Item = (StreamPath, DirectoryEntry)>,
    carried: Option<&TreeVersion>,
    emit: &mut impl FnMut(EncodedTable) -> bool,
) -> Option<TreeVersion> {
    let mut references = Vec::new();
    for_each_chunk(
        rows,
        |_row| DIRECTORY_ROW_ENCODED_BYTES_MAX,
        |rows, estimate| {
            let key = next_table_key(generation, attempt, ordinal, StoreKind::Directory);
            let encoded = EncodedTable::encode_directory(partition, key, &rows)
                .expect("planned Directory rows fit the durable table encoding");
            assert!(
                encoded.table().object_bytes().get() <= estimate,
                "Directory encoded accounting dominates the exact frozen table length"
            );
            references.push(encoded.table());
            emit(encoded)
        },
    )
    .then(|| build_manifest(carried, references))
}

fn build_ledger_tree(
    partition: PartitionId,
    generation: SealGeneration,
    attempt: AttemptId,
    ordinal: &mut u32,
    rows: impl IntoIterator<Item = (StreamUid, LedgerCell)>,
    carried: Option<&TreeVersion>,
    emit: &mut impl FnMut(EncodedTable) -> bool,
) -> Option<TreeVersion> {
    let mut references = Vec::new();
    for_each_chunk(
        rows,
        |(_uid, cell)| ledger_row_bytes(cell),
        |rows, estimate| {
            let key = next_table_key(generation, attempt, ordinal, StoreKind::Ledger);
            let encoded = EncodedTable::encode_ledger(partition, key, &rows)
                .expect("planned Ledger rows fit the durable table encoding");
            assert!(
                encoded.table().object_bytes().get() <= estimate,
                "Ledger encoded accounting dominates the exact frozen table length"
            );
            references.push(encoded.table());
            emit(encoded)
        },
    )
    .then(|| build_manifest(carried, references))
}

const fn ledger_row_bytes(cell: &LedgerCell) -> u64 {
    match cell {
        LedgerCell::Value(_) => LEDGER_VALUE_ROW_ENCODED_BYTES_MAX,
        LedgerCell::Delete => LEDGER_DELETE_ROW_ENCODED_BYTES_MAX,
    }
}

fn for_each_chunk<Row>(
    rows: impl IntoIterator<Item = Row>,
    row_bytes: impl Fn(&Row) -> u64,
    mut emit: impl FnMut(Vec<Row>, u64) -> bool,
) -> bool {
    let mut chunk = Vec::new();
    let mut bytes = SST_ARCHIVE_FIXED_BYTES_MAX;
    for row in rows {
        let additional = row_bytes(&row);
        assert!(
            SST_ARCHIVE_FIXED_BYTES_MAX
                .checked_add(additional)
                .is_some_and(|one_row| one_row <= SST_TABLE_TARGET_BYTES),
            "one worst-case encoded row fits the checkpoint table target"
        );
        let extended = bytes
            .checked_add(additional)
            .expect("a checkpoint table estimate fits in u64");
        if !chunk.is_empty() && extended > SST_TABLE_TARGET_BYTES {
            if !emit(std::mem::take(&mut chunk), bytes) {
                return false;
            }
            bytes = SST_ARCHIVE_FIXED_BYTES_MAX;
        }
        bytes = bytes
            .checked_add(additional)
            .expect("a checkpoint table estimate fits in u64");
        chunk.push(row);
    }
    if !chunk.is_empty() && !emit(chunk, bytes) {
        return false;
    }
    true
}

fn next_table_key(
    generation: SealGeneration,
    attempt: AttemptId,
    ordinal: &mut u32,
    store: StoreKind,
) -> TableKey {
    let current = *ordinal;
    *ordinal = ordinal
        .checked_add(1)
        .expect("a checkpoint table ordinal fits in u32");
    let fresh = FreshIdentity::new(generation, attempt, current)
        .expect("checkpoint planning constructs a valid fresh table identity");
    TableKey::new(TableObjectId::new(fresh, store))
}

fn build_manifest(carried: Option<&TreeVersion>, tables: Vec<TableRef>) -> TreeVersion {
    let fresh = if tables.is_empty() {
        None
    } else {
        Some(SortedRun::try_from(tables).expect("checkpoint tables form one legal sorted run"))
    };
    let runs = match (carried, fresh) {
        (Some(carried), Some(fresh)) => {
            let capacity = carried
                .runs()
                .len()
                .checked_add(1)
                .expect("a legal carried tree has room for one run count");
            let mut runs = Vec::with_capacity(capacity);
            runs.push(fresh);
            runs.extend_from_slice(carried.runs());
            runs
        }
        (Some(carried), None) => carried.runs().to_vec(),
        (None, Some(fresh)) => vec![fresh],
        (None, None) => Vec::new(),
    };
    TreeVersion::try_from(runs).expect("checkpoint planning constructs a legal tree version")
}

fn assemble_checkpoint_seal(
    partition: PartitionId,
    generation: SealGeneration,
    cut: BatchId,
    owner: OwnerToken,
    directory: TreeVersion,
    ledger: TreeVersion,
) -> (Seal, EncodedAuthoritySeal) {
    let successor = Seal::new(
        partition,
        generation,
        WalReplayPoint::Through { batch: cut, owner },
        directory,
        ledger,
    )
    .expect("checkpoint planning constructs a valid exact-successor Seal");
    let encoded = EncodedAuthoritySeal::try_from(&successor)
        .expect("a planned checkpoint Seal fits the durable encoding bound");
    (successor, encoded)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;

    use proptest::prelude::*;
    use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};
    use strom_storage_domain::{OperationFact, StreamUid};

    use super::*;

    proptest! {
        #[test]
        fn generated_delta_rows_transform_the_exact_base_into_the_snapshot(
            actions in prop::collection::vec(any::<u8>(), 1..80),
            raw_split in any::<usize>(),
        ) {
            let split = raw_split
                .checked_rem(actions.len())
                .expect("generated action vectors are nonempty");
            let (base_actions, suffix_actions) = actions
                .split_at_checked(split)
                .expect("the generated split lies inside the action vector");
            let mut batch = 1u64;
            let mut base = Forest::empty();
            apply_actions(&mut base, base_actions, &mut batch);
            let mut snapshot = base.clone();
            apply_actions(&mut snapshot, suffix_actions, &mut batch);

            let rows = plan_delta_rows(&base, &snapshot);
            let base_cells = base.checkpoint_cells();
            let mut directory = base_cells.directory.into_iter().collect::<BTreeMap<_, _>>();
            for (key, entry) in rows.directory.into_iter().flatten() {
                directory.insert(key, entry);
            }
            let mut ledger = base_cells
                .ledger
                .into_iter()
                .collect::<BTreeMap<_, _>>();
            for (uid, cell) in rows.ledger.into_iter().flatten() {
                match cell {
                    LedgerCell::Value(_) => {
                        ledger.insert(uid, cell);
                    }
                    LedgerCell::Delete => {
                        ledger.remove(&uid);
                    }
                }
            }
            let snapshot_cells = snapshot.checkpoint_cells();
            let expected_directory = snapshot_cells
                .directory
                .into_iter()
                .collect::<BTreeMap<_, _>>();
            let expected_ledger = snapshot_cells
                .ledger
                .into_iter()
                .collect::<BTreeMap<_, _>>();
            prop_assert_eq!(directory, expected_directory);
            prop_assert_eq!(ledger, expected_ledger);
        }
    }

    #[test]
    fn create_then_delete_emits_a_directory_tombstone_and_no_ledger_row()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = stream_path("events/a")?;
        let uid = StreamUid::try_from(1)?;
        let mut snapshot = Forest::empty();
        snapshot.strict_fold(
            BatchId::try_from(1)?,
            &OperationFact::StreamCreated {
                path: path.clone(),
                uid,
                content_type: StreamContentType::octet_stream(),
                expiry: ExpiryPolicy::None,
                lifecycle: StreamLifecycle::Open,
            },
        )?;
        snapshot.strict_fold(
            BatchId::try_from(2)?,
            &OperationFact::StreamDeleted {
                path: path.clone(),
                uid,
            },
        )?;

        let rows = plan_delta_rows(&Forest::empty(), &snapshot);
        assert_eq!(
            vec![vec![(path, DirectoryEntry::Tombstone(uid))]],
            rows.directory
        );
        assert!(rows.ledger.is_empty());
        Ok(())
    }

    #[test]
    fn no_candidate_is_constructed_at_the_source_cut() -> Result<(), Box<dyn std::error::Error>> {
        let partition = partition();
        let source = Seal::new(
            partition,
            SealGeneration::genesis(),
            WalReplayPoint::Genesis,
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;
        let owner_claim = source.generation().successor()?;
        let source = source.claim_successor()?;
        let cut = BatchId::try_from(1)?;
        let source = Seal::new(
            partition,
            source.generation(),
            WalReplayPoint::Through {
                batch: cut,
                owner: OwnerToken::from(SealGeneration::genesis()),
            },
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;
        let ticket = strom_storage_protocol::CheckpointTicket::new(
            source.generation(),
            cut,
            AttemptId::new(owner_claim, 0),
        );
        let result = prepare_checkpoint_for_test(CheckpointInput::new(
            ticket,
            source,
            Forest::empty(),
            Forest::empty(),
        ));
        assert!(result.is_err(), "a same-cut advance has no candidate");
        Ok(())
    }

    #[test]
    fn one_run_beyond_the_tree_bound_rewrites_every_nonempty_tree()
    -> Result<(), Box<dyn std::error::Error>> {
        let partition = partition();
        let owner_claim = SealGeneration::try_from(2)?;
        let source_generation = SealGeneration::try_from(3)?;
        let runs = (0..TREE_RUNS_MAX)
            .map(|ordinal| {
                let ordinal = u32::try_from(ordinal)?;
                let fresh =
                    FreshIdentity::new(source_generation, AttemptId::new(owner_claim, 7), ordinal)?;
                let table = TableRef::new(
                    TableObjectId::new(fresh, StoreKind::Directory),
                    NonZeroU64::MIN,
                )?;
                Ok(SortedRun::try_from(vec![table])?)
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        let source = Seal::new(
            partition,
            source_generation,
            WalReplayPoint::Through {
                batch: BatchId::try_from(1)?,
                owner: OwnerToken::from(owner_claim),
            },
            TreeVersion::try_from(runs)?,
            TreeVersion::empty(),
        )?;
        let path = stream_path("events/a")?;
        let uid = StreamUid::try_from(1)?;
        let mut snapshot = Forest::empty();
        snapshot.strict_fold(
            BatchId::try_from(2)?,
            &OperationFact::StreamCreated {
                path,
                uid,
                content_type: StreamContentType::octet_stream(),
                expiry: ExpiryPolicy::None,
                lifecycle: StreamLifecycle::Open,
            },
        )?;

        let ticket = strom_storage_protocol::CheckpointTicket::new(
            source.generation(),
            BatchId::try_from(2)?,
            AttemptId::new(owner_claim, 8),
        );
        let (prepared, _tables) = prepare_checkpoint_for_test(CheckpointInput::new(
            ticket,
            source,
            Forest::empty(),
            snapshot,
        ))?;
        let successor = prepared.successor().clone();
        assert_eq!(1, successor.directory().runs().len());
        assert_eq!(1, successor.ledger().runs().len());
        assert!(
            successor
                .directory()
                .runs()
                .iter()
                .chain(successor.ledger().runs())
                .flat_map(SortedRun::tables)
                .all(|table| table.object().fresh().birth_generation() == successor.generation()),
            "global fallback carries no source table"
        );
        Ok(())
    }

    #[test]
    fn delta_legality_is_inclusive_at_every_aggregate_bound()
    -> Result<(), Box<dyn std::error::Error>> {
        let partition = partition();
        let directory = account_rows([DIRECTORY_ROW_ENCODED_BYTES_MAX]);
        let ledger = ChunkAccounting::default();

        let carried_objects = PARTITION_BOOTSTRAP_OBJECTS_MAX_V2
            .checked_sub(1)
            .expect("the aggregate object bound is nonzero");
        let object_at_bound =
            seal_with_directory_tables(partition, vec![NonZeroU64::MIN; carried_objects])?;
        assert!(select_delta(&object_at_bound, directory, ledger));
        let object_over = seal_with_directory_tables(
            partition,
            vec![NonZeroU64::MIN; PARTITION_BOOTSTRAP_OBJECTS_MAX_V2],
        )?;
        assert!(!select_delta(&object_over, directory, ledger));

        let fresh_bytes = SST_ARCHIVE_FIXED_BYTES_MAX
            .checked_add(DIRECTORY_ROW_ENCODED_BYTES_MAX)
            .expect("one Directory delta estimate fits in u64");
        let table_bytes_max = strom_storage_domain::SST_OBJECT_BYTES_MAX;
        let last_at_bound = table_bytes_max
            .checked_sub(fresh_bytes)
            .and_then(NonZeroU64::new)
            .expect("one fresh row leaves a nonzero final carried table");
        let mut byte_lengths =
            vec![NonZeroU64::new(table_bytes_max).expect("the SST byte bound is nonzero"); 255];
        byte_lengths.push(last_at_bound);
        let bytes_at_bound = seal_with_directory_tables(partition, byte_lengths.clone())?;
        assert!(select_delta(&bytes_at_bound, directory, ledger));
        let last_over = NonZeroU64::new(
            last_at_bound
                .get()
                .checked_add(1)
                .expect("the boundary fixture has one byte of headroom"),
        )
        .expect("the incremented fixture remains nonzero");
        let last = byte_lengths
            .last_mut()
            .expect("the byte-bound fixture has tables");
        *last = last_over;
        let bytes_over = seal_with_directory_tables(partition, byte_lengths)?;
        assert!(!select_delta(&bytes_over, directory, ledger));
        Ok(())
    }

    fn apply_actions(forest: &mut Forest, actions: &[u8], batch: &mut u64) {
        for (index, action) in actions.iter().enumerate() {
            let fact = match action % 3 {
                0 => create_for_index(forest, index, *batch),
                1 => forest
                    .checkpoint_cells()
                    .directory
                    .into_iter()
                    .find_map(|(path, entry)| match entry {
                        DirectoryEntry::Live(uid)
                            if forest
                                .record(uid)
                                .is_some_and(|record| !record.lifecycle().is_closed()) =>
                        {
                            Some(OperationFact::StreamClosed { path, uid })
                        }
                        DirectoryEntry::Live(_) | DirectoryEntry::Tombstone(_) => None,
                    })
                    .unwrap_or_else(|| create_for_index(forest, index, *batch)),
                2..=u8::MAX => forest
                    .checkpoint_cells()
                    .directory
                    .into_iter()
                    .find_map(|(path, entry)| match entry {
                        DirectoryEntry::Live(uid) => {
                            Some(OperationFact::StreamDeleted { path, uid })
                        }
                        DirectoryEntry::Tombstone(_) => None,
                    })
                    .unwrap_or_else(|| create_for_index(forest, index, *batch)),
            };
            let coordinate = BatchId::try_from(*batch).expect("generated batch is nonzero");
            forest
                .strict_fold(coordinate, &fact)
                .expect("generated facts form a valid dense history");
            *batch = batch.checked_add(1).expect("generated history is small");
        }
    }

    fn create_for_index(forest: &Forest, index: usize, batch: u64) -> OperationFact {
        let path = stream_path(&format!("events/generated-{batch}-{index}"))
            .expect("generated path is canonical");
        let uid = u64::try_from(forest.checkpoint_cells().directory.len())
            .expect("the generated forest row count fits in u64")
            .checked_add(1)
            .and_then(|uid| StreamUid::try_from(uid).ok())
            .expect("generated history remains below capacity");
        OperationFact::StreamCreated {
            path,
            uid,
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Open,
        }
    }

    fn partition() -> PartitionId {
        "00112233-4455-6677-8899-aabbccddeeff"
            .parse()
            .expect("test partition is canonical")
    }

    fn stream_path(raw: &str) -> Result<StreamPath, Box<dyn std::error::Error>> {
        Ok(raw.parse()?)
    }

    fn prepare_checkpoint_for_test(
        input: CheckpointInput,
    ) -> Result<(PreparedCheckpoint, Vec<EncodedTable>), CheckpointContradiction> {
        let mut tables = Vec::new();
        let prepared = prepare_checkpoint(input, &mut |table| {
            tables.push(table);
            true
        })?
        .expect("the test table consumer remains open");
        Ok((*prepared, tables))
    }

    fn seal_with_directory_tables(
        partition: PartitionId,
        lengths: Vec<NonZeroU64>,
    ) -> Result<Seal, Box<dyn std::error::Error>> {
        let owner = SealGeneration::genesis();
        let generation = owner.successor()?;
        let tables = lengths
            .into_iter()
            .enumerate()
            .map(|(ordinal, bytes)| {
                let ordinal = u32::try_from(ordinal)?;
                let fresh = FreshIdentity::new(generation, AttemptId::new(owner, 90), ordinal)?;
                Ok(TableRef::new(
                    TableObjectId::new(fresh, StoreKind::Directory),
                    bytes,
                )?)
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        Ok(Seal::new(
            partition,
            generation,
            WalReplayPoint::Through {
                batch: BatchId::try_from(1)?,
                owner: OwnerToken::from(owner),
            },
            TreeVersion::try_from(vec![SortedRun::try_from(tables)?])?,
            TreeVersion::empty(),
        )?)
    }
}
