//! Pure advancing-checkpoint planning and its bounded storage pipeline.

#![expect(
    clippy::disallowed_types,
    reason = "the single writer and checkpoint task share this one enumerated publication handshake"
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::{StreamExt as _, stream};
use imbl::ordmap::DiffItem;
use strom_object_store::{CreateEvidence, ObjectStoreAdapter};
use strom_storage_domain::{
    AttemptId, BatchId, DIRECTORY_ROW_ENCODED_BYTES_MAX, DirectoryEntry, DirectoryKey,
    FreshIdentity, LEDGER_DELETE_ROW_ENCODED_BYTES_MAX, LEDGER_VALUE_ROW_ENCODED_BYTES_MAX,
    LedgerCell, OwnerToken, PARTITION_BOOTSTRAP_BYTES_MAX_V2, PARTITION_BOOTSTRAP_OBJECTS_MAX_V2,
    PartitionId, SST_ARCHIVE_FIXED_BYTES_MAX, SST_TABLE_TARGET_BYTES, Seal, SealGeneration,
    SortedRun, StoreKind, StreamUid, TREE_RUNS_MAX, TableKey, TableObjectId, TableRef, TreeVersion,
    WalReplayPoint, encode_directory_sst, encode_ledger_sst,
};
use tokio::sync::{Notify, Semaphore, mpsc, oneshot};

use crate::Forest;
use crate::store::{
    CandidateTableEvidence, EncodedSeal, EncodedTable, SealStore, SealStoreError, TableStore,
    TableStoreError, WalStore, targeted_table_deletes,
};

pub(crate) const WAL_SUFFIX_CHECKPOINT_SPAN_TRIGGER: u64 = 512;
const CHECKPOINT_CHILD_CREATES_MAX: usize = 16;
const CHECKPOINT_PREPARATIONS_MAX: usize = 2;
// Keep preparation behind the fixed-width child-create pipeline instead of
// retaining a checkpoint's encoded tables in aggregate.
const CHECKPOINT_TABLE_CHANNEL_MAX: usize = 1;
static CHECKPOINT_PREPARATIONS: Semaphore = Semaphore::const_new(CHECKPOINT_PREPARATIONS_MAX);

#[derive(Debug)]
pub(crate) struct CheckpointInput {
    pub(crate) source: Seal,
    pub(crate) base: Forest,
    pub(crate) snapshot: Forest,
    pub(crate) cut: BatchId,
    pub(crate) attempt: AttemptId,
}

#[derive(Debug)]
pub(crate) struct PreparedCheckpoint {
    source: Seal,
    successor: Seal,
    snapshot: Forest,
    encoded_seal: EncodedSeal,
}

#[derive(Debug, thiserror::Error)]
#[error("{detail}")]
struct CheckpointContradiction {
    detail: String,
}

impl From<String> for CheckpointContradiction {
    fn from(detail: String) -> Self {
        Self { detail }
    }
}

impl PreparedCheckpoint {
    pub(crate) const fn cut(&self) -> Option<BatchId> {
        advancing_batch(self.successor.replay())
    }

    pub(crate) fn into_install(self) -> CheckpointInstall {
        CheckpointInstall {
            source: self.source,
            successor: self.successor,
            snapshot: self.snapshot,
        }
    }
}

pub(crate) struct CheckpointInstall {
    pub(crate) source: Seal,
    pub(crate) successor: Seal,
    pub(crate) snapshot: Forest,
}

#[derive(Debug)]
pub(crate) enum CheckpointOutcome {
    Abandoned,
    Contradiction {
        cut: BatchId,
        detail: String,
    },
    Seal {
        prepared: Box<PreparedCheckpoint>,
        evidence: Result<CreateEvidence, SealStoreError>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct PublicationGate(Arc<PublicationState>);

#[derive(Debug)]
struct PublicationState {
    claimed: AtomicBool,
    notify: Notify,
}

impl PublicationGate {
    pub(crate) fn new() -> Self {
        Self(Arc::new(PublicationState {
            claimed: AtomicBool::new(false),
            notify: Notify::new(),
        }))
    }

    pub(crate) fn cancel_before_publish(&self) -> bool {
        self.claim()
    }

    fn claim(&self) -> bool {
        let claimed = self
            .0
            .claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if claimed {
            self.0.notify.notify_waiters();
        }
        claimed
    }

    fn begin_publish(&self) -> bool {
        self.claim()
    }

    #[cfg(test)]
    pub(crate) fn test_begin_publish(&self) -> bool {
        self.begin_publish()
    }

    #[cfg(test)]
    pub(crate) fn test_is_claimed(&self) -> bool {
        self.0.claimed.load(Ordering::Acquire)
    }

    async fn claimed(&self) {
        loop {
            if self.0.claimed.load(Ordering::Acquire) {
                return;
            }
            let notified = self.0.notify.notified();
            if self.0.claimed.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ChunkAccounting {
    tables: usize,
    bytes: u64,
}

#[derive(Debug)]
enum CheckpointPlan {
    Delta,
    Full,
}

pub(crate) async fn execute_checkpoint(
    adapter: ObjectStoreAdapter,
    input: CheckpointInput,
    publication: PublicationGate,
) -> CheckpointOutcome {
    let cut = input.cut;
    let permit = tokio::select! {
        biased;
        () = publication.claimed() => return CheckpointOutcome::Abandoned,
        permit = CHECKPOINT_PREPARATIONS.acquire() => match permit {
            Ok(permit) => permit,
            Err(_closed) => {
            return CheckpointOutcome::Contradiction {
                cut,
                detail: "checkpoint preparation gate is closed".into(),
            };
            }
        },
    };
    let (table_sender, table_receiver) = mpsc::channel(CHECKPOINT_TABLE_CHANNEL_MAX);
    let (prepared_sender, prepared_receiver) = oneshot::channel();
    let preparation = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let prepared = prepare_checkpoint(input, &mut |table| {
            table_sender.blocking_send(table).is_ok()
        });
        let _consumer_may_be_gone = prepared_sender.send(prepared);
    });

    let store = TableStore::new(adapter.clone());
    let table_result = establish_tables(&store, table_receiver, publication.clone()).await;
    if let Err(join_error) = preparation.await {
        return CheckpointOutcome::Contradiction {
            cut,
            detail: format!("checkpoint preparation task failed: {join_error}"),
        };
    }
    match table_result {
        Ok(()) => {}
        Err(EstablishTableError::Abandon) => return CheckpointOutcome::Abandoned,
        Err(EstablishTableError::Contradiction { detail }) => {
            return CheckpointOutcome::Contradiction { cut, detail };
        }
    }
    let prepared = match prepared_receiver.await {
        Ok(Ok(Some(prepared))) => prepared,
        Ok(Ok(None)) => return CheckpointOutcome::Abandoned,
        Ok(Err(error)) => {
            return CheckpointOutcome::Contradiction {
                cut,
                detail: error.to_string(),
            };
        }
        Err(_sender_dropped) => {
            return CheckpointOutcome::Contradiction {
                cut,
                detail: "checkpoint preparation ended without a result".into(),
            };
        }
    };

    if !publication.begin_publish() {
        return CheckpointOutcome::Abandoned;
    }
    let seal_store = SealStore::new(adapter);
    let evidence = seal_store.create_seal(&prepared.encoded_seal).await;
    CheckpointOutcome::Seal { prepared, evidence }
}

pub(crate) async fn collect_advance(adapter: ObjectStoreAdapter, source: Seal, successor: Seal) {
    let partition = successor.partition();
    let previous_cut = replay_batch(source.replay());
    let Some(cut) = advancing_batch(successor.replay()) else {
        return;
    };
    let wal_store = WalStore::new(adapter.clone());
    let mut batch = match previous_cut {
        Some(previous) => match previous.successor() {
            Ok(batch) => batch,
            Err(_exhausted) => return,
        },
        None => BatchId::try_from(1).expect("batch one is a legal WAL coordinate"),
    };
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
        batch = match batch.successor() {
            Ok(next) => next,
            Err(_exhausted) => return,
        };
    }

    let table_store = TableStore::new(adapter);
    for proof in targeted_table_deletes(&source, &successor) {
        if table_store.delete_table(proof).await.is_err() {
            return;
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum EstablishTableError {
    #[error("checkpoint table establishment was abandoned")]
    Abandon,
    #[error("checkpoint table establishment found a contradiction: {detail}")]
    Contradiction { detail: String },
}

async fn establish_tables(
    store: &TableStore,
    mut receiver: mpsc::Receiver<EncodedTable>,
    publication: PublicationGate,
) -> Result<(), EstablishTableError> {
    let tables = stream::poll_fn(move |context| receiver.poll_recv(context));
    let creates = tables
        .map(|table| {
            let store = store.clone();
            async move { establish_table(&store, &table).await }
        })
        .buffer_unordered(CHECKPOINT_CHILD_CREATES_MAX);
    futures::pin_mut!(creates);
    loop {
        tokio::select! {
            biased;
            () = publication.claimed() => return Err(EstablishTableError::Abandon),
            result = creates.next() => match result {
                Some(result) => result?,
                None => return Ok(()),
            }
        }
    }
}

async fn establish_table(
    store: &TableStore,
    candidate: &EncodedTable,
) -> Result<(), EstablishTableError> {
    match store.create_table(candidate).await {
        Ok(CreateEvidence::Direct | CreateEvidence::DurableMatch) => Ok(()),
        Ok(CreateEvidence::NotOurs) => Err(EstablishTableError::Contradiction {
            detail: "foreign bytes occupy a fresh checkpoint table identity".into(),
        }),
        Ok(CreateEvidence::Unresolved) => match store.reconcile_table(candidate).await {
            Ok(CandidateTableEvidence::Match) => Ok(()),
            Ok(CandidateTableEvidence::Foreign) => Err(EstablishTableError::Contradiction {
                detail: "an unresolved fresh checkpoint table contains foreign bytes".into(),
            }),
            Ok(CandidateTableEvidence::Absent)
            | Err(TableStoreError::Retryable { .. } | TableStoreError::Rejected { .. }) => {
                Err(EstablishTableError::Abandon)
            }
            Err(TableStoreError::Contradiction { detail }) => {
                Err(EstablishTableError::Contradiction { detail })
            }
        },
        Err(TableStoreError::Retryable { .. } | TableStoreError::Rejected { .. }) => {
            Err(EstablishTableError::Abandon)
        }
        Err(TableStoreError::Contradiction { detail }) => {
            Err(EstablishTableError::Contradiction { detail })
        }
    }
}

fn prepare_checkpoint(
    input: CheckpointInput,
    emit: &mut impl FnMut(EncodedTable) -> bool,
) -> Result<Option<Box<PreparedCheckpoint>>, CheckpointContradiction> {
    let CheckpointInput {
        source,
        base,
        snapshot,
        cut,
        attempt,
    } = input;
    let partition = source.partition();
    let owner_claim = attempt.owner_claim();
    let previous_cut = replay_batch(source.replay());
    if previous_cut.is_some_and(|previous| cut <= previous) {
        return Err("checkpoint cut does not advance its source Seal"
            .to_owned()
            .into());
    }
    let generation = source
        .generation()
        .successor()
        .map_err(|error| error.to_string())?;

    let plan = plan_checkpoint(&source, &base, &snapshot);
    let mut ordinal = 0u32;
    let (directory, ledger) = match plan {
        CheckpointPlan::Delta => {
            let Some(directory) = build_directory_tree(
                partition,
                generation,
                attempt,
                &mut ordinal,
                delta_directory_rows(&base, &snapshot),
                Some(source.directory()),
                emit,
            ) else {
                return Ok(None);
            };
            let Some(ledger) = build_ledger_tree(
                partition,
                generation,
                attempt,
                &mut ordinal,
                delta_ledger_rows(&base, &snapshot),
                Some(source.ledger()),
                emit,
            ) else {
                return Ok(None);
            };
            (directory, ledger)
        }
        CheckpointPlan::Full => {
            let directory_rows = snapshot
                .directory_rows()
                .iter()
                .map(|(key, entry)| (key.clone(), *entry));
            let Some(directory) = build_directory_tree(
                partition,
                generation,
                attempt,
                &mut ordinal,
                directory_rows,
                None,
                emit,
            ) else {
                return Ok(None);
            };
            let ledger_rows = snapshot
                .ledger_rows()
                .iter()
                .map(|(uid, record)| (*uid, LedgerCell::Value(record.clone())));
            let Some(ledger) = build_ledger_tree(
                partition,
                generation,
                attempt,
                &mut ordinal,
                ledger_rows,
                None,
                emit,
            ) else {
                return Ok(None);
            };
            (directory, ledger)
        }
    };
    let (successor, encoded_seal) = assemble_checkpoint_seal(
        partition,
        generation,
        cut,
        OwnerToken::from(owner_claim),
        directory,
        ledger,
    );
    Ok(Some(Box::new(PreparedCheckpoint {
        source,
        successor,
        snapshot,
        encoded_seal,
    })))
}

const fn replay_batch(replay: WalReplayPoint) -> Option<BatchId> {
    match replay {
        WalReplayPoint::Genesis => None,
        WalReplayPoint::Through { batch, owner: _ } => Some(batch),
    }
}

const fn advancing_batch(replay: WalReplayPoint) -> Option<BatchId> {
    match replay {
        WalReplayPoint::Through { batch, owner: _ } => Some(batch),
        WalReplayPoint::Genesis => None,
    }
}

fn plan_checkpoint(source: &Seal, base: &Forest, snapshot: &Forest) -> CheckpointPlan {
    if let Some(delta) = plan_delta(source, base, snapshot) {
        return delta;
    }
    let directory = account_rows(
        snapshot
            .directory_rows()
            .iter()
            .map(|_row| DIRECTORY_ROW_ENCODED_BYTES_MAX),
    );
    let ledger = account_rows(
        snapshot
            .ledger_rows()
            .iter()
            .map(|_row| LEDGER_VALUE_ROW_ENCODED_BYTES_MAX),
    );
    assert_full_plan(directory, ledger);
    CheckpointPlan::Full
}

fn plan_delta(source: &Seal, base: &Forest, snapshot: &Forest) -> Option<CheckpointPlan> {
    let directory = account_rows(
        base.directory_rows()
            .diff(snapshot.directory_rows())
            .filter_map(|difference| match difference {
                DiffItem::Add(_, _) | DiffItem::Update { .. } => {
                    Some(DIRECTORY_ROW_ENCODED_BYTES_MAX)
                }
                DiffItem::Remove(key, _entry) => {
                    assert!(
                        snapshot.directory_rows().contains_key(key),
                        "Directory path occupancy is permanent"
                    );
                    None
                }
            }),
    );
    let ledger = account_rows(
        base.ledger_rows()
            .diff(snapshot.ledger_rows())
            .map(|difference| match difference {
                DiffItem::Add(_, _) | DiffItem::Update { .. } => LEDGER_VALUE_ROW_ENCODED_BYTES_MAX,
                DiffItem::Remove(_, _) => LEDGER_DELETE_ROW_ENCODED_BYTES_MAX,
            }),
    );
    select_delta(source, directory, ledger)
}

#[cfg(test)]
fn plan_delta_rows(base: &Forest, snapshot: &Forest) -> PlannedRows {
    PlannedRows {
        directory: chunk_rows(delta_directory_rows(base, snapshot), |_row| {
            DIRECTORY_ROW_ENCODED_BYTES_MAX
        }),
        ledger: chunk_rows(delta_ledger_rows(base, snapshot), |(_uid, cell)| {
            ledger_row_bytes(cell)
        }),
    }
}

#[cfg(test)]
#[derive(Debug)]
struct PlannedRows {
    directory: Vec<Vec<(DirectoryKey, DirectoryEntry)>>,
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

fn select_delta(
    source: &Seal,
    directory: ChunkAccounting,
    ledger: ChunkAccounting,
) -> Option<CheckpointPlan> {
    let directory_runs = source
        .directory()
        .runs()
        .len()
        .checked_add(usize::from(directory.tables != 0))?;
    let ledger_runs = source
        .ledger()
        .runs()
        .len()
        .checked_add(usize::from(ledger.tables != 0))?;
    let carried_objects = seal_tables(source).count();
    let fresh_objects = directory.tables.checked_add(ledger.tables)?;
    let carried_bytes = seal_tables(source)
        .map(|table| table.object_bytes().get())
        .sum::<u64>();
    let fresh_bytes = directory.bytes.checked_add(ledger.bytes)?;
    (directory_runs <= TREE_RUNS_MAX
        && ledger_runs <= TREE_RUNS_MAX
        && carried_objects
            .checked_add(fresh_objects)
            .is_some_and(|objects| objects <= PARTITION_BOOTSTRAP_OBJECTS_MAX_V2)
        && carried_bytes
            .checked_add(fresh_bytes)
            .is_some_and(|bytes| bytes <= PARTITION_BOOTSTRAP_BYTES_MAX_V2))
    .then_some(CheckpointPlan::Delta)
}

fn seal_tables(seal: &Seal) -> impl Iterator<Item = &TableRef> {
    [seal.directory(), seal.ledger()]
        .into_iter()
        .flat_map(TreeVersion::runs)
        .flat_map(SortedRun::tables)
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
    rows: impl IntoIterator<Item = (DirectoryKey, DirectoryEntry)>,
    carried: Option<&TreeVersion>,
    emit: &mut impl FnMut(EncodedTable) -> bool,
) -> Option<TreeVersion> {
    let mut references = Vec::new();
    for_each_chunk(
        rows,
        |_row| DIRECTORY_ROW_ENCODED_BYTES_MAX,
        |rows, estimate| {
            let key = next_table_key(generation, attempt, ordinal, StoreKind::Directory);
            let encoded = EncodedTable::new(
                key,
                encode_directory_sst(partition, &key, &rows)
                    .expect("planned Directory rows fit the durable table encoding"),
            );
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

fn delta_directory_rows<'forest>(
    base: &'forest Forest,
    snapshot: &'forest Forest,
) -> impl Iterator<Item = (DirectoryKey, DirectoryEntry)> + 'forest {
    base.directory_rows()
        .diff(snapshot.directory_rows())
        .filter_map(move |difference| match difference {
            DiffItem::Add(key, entry) => Some((key.clone(), *entry)),
            DiffItem::Update {
                old: (_old_key, _old_entry),
                new: (key, entry),
            } => Some((key.clone(), *entry)),
            DiffItem::Remove(key, _entry) => {
                assert!(
                    snapshot.directory_rows().contains_key(key),
                    "Directory path occupancy is permanent"
                );
                None
            }
        })
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
            let encoded = EncodedTable::new(
                key,
                encode_ledger_sst(partition, &key, &rows)
                    .expect("planned Ledger rows fit the durable table encoding"),
            );
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

fn delta_ledger_rows<'forest>(
    base: &'forest Forest,
    snapshot: &'forest Forest,
) -> impl Iterator<Item = (StreamUid, LedgerCell)> + 'forest {
    base.ledger_rows()
        .diff(snapshot.ledger_rows())
        .map(|difference| match difference {
            DiffItem::Add(uid, record) => (*uid, LedgerCell::Value(record.clone())),
            DiffItem::Update {
                old: (_old_uid, _old_record),
                new: (uid, record),
            } => (*uid, LedgerCell::Value(record.clone())),
            DiffItem::Remove(uid, _record) => (*uid, LedgerCell::Delete),
        })
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
) -> (Seal, EncodedSeal) {
    let successor = Seal::new(
        partition,
        generation,
        WalReplayPoint::Through { batch: cut, owner },
        directory,
        ledger,
    )
    .expect("checkpoint planning constructs a valid exact-successor Seal");
    let encoded = EncodedSeal::new(&successor)
        .expect("a planned checkpoint Seal fits the durable encoding bound");
    (successor, encoded)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use proptest::prelude::*;
    use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};
    use strom_storage_domain::{OperationFact, StreamUid, WalBody, WalFacts, WalObject};

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
            let mut directory = base.directory_rows().clone();
            for (key, entry) in rows.directory.into_iter().flatten() {
                directory.insert(key, entry);
            }
            let mut ledger = base.ledger_rows().clone();
            for (uid, cell) in rows.ledger.into_iter().flatten() {
                match cell {
                    LedgerCell::Value(record) => {
                        ledger.insert(uid, record);
                    }
                    LedgerCell::Delete => {
                        ledger.remove(&uid);
                    }
                }
            }
            prop_assert_eq!(&directory, snapshot.directory_rows());
            prop_assert_eq!(&ledger, snapshot.ledger_rows());
        }
    }

    #[test]
    fn shutdown_and_seal_send_share_one_atomic_boundary() {
        let cancelled = PublicationGate::new();
        assert!(cancelled.cancel_before_publish());
        assert!(!cancelled.begin_publish());

        let publishing = PublicationGate::new();
        assert!(publishing.begin_publish());
        assert!(!publishing.cancel_before_publish());
    }

    #[test]
    fn create_then_delete_emits_a_directory_tombstone_and_no_ledger_row()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = directory_key("events/a")?;
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
        let result = prepare_checkpoint_for_test(CheckpointInput {
            source,
            base: Forest::empty(),
            snapshot: Forest::empty(),
            cut,
            attempt: AttemptId::new(owner_claim, 0),
        });
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
        let path = directory_key("events/a")?;
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

        let (prepared, _tables) = prepare_checkpoint_for_test(CheckpointInput {
            source,
            base: Forest::empty(),
            snapshot,
            cut: BatchId::try_from(2)?,
            attempt: AttemptId::new(owner_claim, 8),
        })?;
        let successor = prepared.into_install().successor;
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
        assert!(select_delta(&object_at_bound, directory, ledger).is_some());
        let object_over = seal_with_directory_tables(
            partition,
            vec![NonZeroU64::MIN; PARTITION_BOOTSTRAP_OBJECTS_MAX_V2],
        )?;
        assert!(select_delta(&object_over, directory, ledger).is_none());

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
        assert!(select_delta(&bytes_at_bound, directory, ledger).is_some());
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
        assert!(select_delta(&bytes_over, directory, ledger).is_none());
        Ok(())
    }

    #[tokio::test]
    async fn collection_crosses_fences_and_absence_then_deletes_a_later_run()
    -> Result<(), Box<dyn std::error::Error>> {
        let adapter = ObjectStoreAdapter::in_memory();
        let partition = partition();
        let owner = OwnerToken::from(SealGeneration::genesis());
        let wal_store = WalStore::new(adapter.clone());
        let fence = crate::store::EncodedWal::new(&WalObject::new(
            partition,
            BatchId::try_from(1)?,
            owner,
            WalBody::Fence,
        ))?;
        assert_eq!(CreateEvidence::Direct, wal_store.create_wal(&fence).await?);
        let run = crate::store::EncodedWal::new(&WalObject::new(
            partition,
            BatchId::try_from(3)?,
            owner,
            WalBody::Run(WalFacts::try_from(vec![OperationFact::StreamCreated {
                path: directory_key("events/a")?,
                uid: StreamUid::try_from(1)?,
                content_type: StreamContentType::octet_stream(),
                expiry: ExpiryPolicy::None,
                lifecycle: StreamLifecycle::Open,
            }])?),
        ))?;
        assert_eq!(CreateEvidence::Direct, wal_store.create_wal(&run).await?);
        let source = Seal::new(
            partition,
            SealGeneration::genesis(),
            WalReplayPoint::Genesis,
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;
        let successor = Seal::new(
            partition,
            source.generation().successor()?,
            WalReplayPoint::Through {
                batch: BatchId::try_from(3)?,
                owner,
            },
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;

        collect_advance(adapter, source, successor).await;
        assert!(
            wal_store
                .read_wal(partition, fence.batch())
                .await?
                .is_some()
        );
        assert!(wal_store.read_wal(partition, run.batch()).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn targeted_collection_deletes_only_source_tables_dropped_by_the_successor()
    -> Result<(), Box<dyn std::error::Error>> {
        let adapter = ObjectStoreAdapter::in_memory();
        let partition = partition();
        let owner_claim = SealGeneration::genesis();
        let source_generation = owner_claim.successor()?;
        let key = TableKey::new(TableObjectId::new(
            FreshIdentity::new(source_generation, AttemptId::new(owner_claim, 1), 0)?,
            StoreKind::Directory,
        ));
        let rows = vec![(
            directory_key("events/dead")?,
            DirectoryEntry::Tombstone(StreamUid::try_from(1)?),
        )];
        let encoded = EncodedTable::new(key, encode_directory_sst(partition, &key, &rows)?);
        let table_store = TableStore::new(adapter.clone());
        assert_eq!(
            CreateEvidence::Direct,
            table_store.create_table(&encoded).await?
        );
        let source = Seal::new(
            partition,
            source_generation,
            WalReplayPoint::Through {
                batch: BatchId::try_from(1)?,
                owner: OwnerToken::from(owner_claim),
            },
            TreeVersion::try_from(vec![SortedRun::try_from(vec![encoded.table()])?])?,
            TreeVersion::empty(),
        )?;
        let successor_generation = source_generation.successor()?;
        let successor = Seal::new(
            partition,
            successor_generation,
            WalReplayPoint::Through {
                batch: BatchId::try_from(2)?,
                owner: OwnerToken::from(source_generation),
            },
            TreeVersion::empty(),
            TreeVersion::empty(),
        )?;

        collect_advance(adapter, source, successor).await;
        assert!(matches!(
            table_store.read_table(partition, &encoded.table()).await,
            Err(TableStoreError::Contradiction { .. })
        ));
        Ok(())
    }

    fn apply_actions(forest: &mut Forest, actions: &[u8], batch: &mut u64) {
        for (index, action) in actions.iter().enumerate() {
            let fact = match action % 3 {
                0 => create_for_index(forest, index, *batch),
                1 => forest
                    .directory_rows()
                    .iter()
                    .find_map(|(path, entry)| match entry {
                        DirectoryEntry::Live(uid)
                            if forest
                                .record(*uid)
                                .is_some_and(|record| !record.lifecycle().is_closed()) =>
                        {
                            Some(OperationFact::StreamClosed {
                                path: path.clone(),
                                uid: *uid,
                            })
                        }
                        DirectoryEntry::Live(_) | DirectoryEntry::Tombstone(_) => None,
                    })
                    .unwrap_or_else(|| create_for_index(forest, index, *batch)),
                2..=u8::MAX => forest
                    .directory_rows()
                    .iter()
                    .find_map(|(path, entry)| match entry {
                        DirectoryEntry::Live(uid) => Some(OperationFact::StreamDeleted {
                            path: path.clone(),
                            uid: *uid,
                        }),
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
        let path = directory_key(&format!("events/generated-{batch}-{index}"))
            .expect("generated path is canonical");
        let uid = forest
            .path_count()
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

    fn directory_key(raw: &str) -> Result<DirectoryKey, Box<dyn std::error::Error>> {
        Ok(DirectoryKey::try_from(Box::<[u8]>::from(raw.as_bytes()))?)
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
