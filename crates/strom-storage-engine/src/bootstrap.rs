//! Bounded bootstrap from the newest Seal through an authored takeover fence.

use imbl::OrdMap;
use rand::RngCore as _;
use strom_common::Entropy;
use strom_object_store::ObjectStoreAdapter;
use strom_storage_domain::{
    BatchId, DIRECTORY_ROW_LOGICAL_BYTES_MAX, DirectoryKey, LEDGER_VALUE_ROW_LOGICAL_BYTES_MAX,
    LedgerCell, OperationFact, OwnerToken, PARTITION_BOOTSTRAP_BYTES_MAX_V2,
    PARTITION_BOOTSTRAP_OBJECTS_MAX_V2, PARTITION_RESIDENT_LOGICAL_BYTES_MAX_V2, PartitionId, Seal,
    SealGeneration, StreamUid, TableRef, WAL_SUFFIX_COORDINATES_MAX_V2, WalBody, WalObject,
    WalReplayPoint,
};

use crate::forest::Forest;
use crate::forest::ForestContradiction;
use crate::store::{
    EncodedAuthoritySeal, EncodedGenesisSeal, EncodedWal, GenesisEstablishment, SealPublication,
    SealStore, TableRows, TableStore, TypedStoreError, WalEstablishment, WalStore,
};
use crate::writer::WriterState;

/// Why a partition did not become Ready.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum BootstrapExit {
    #[error("bootstrap should be retried: {detail}")]
    Retryable { detail: String },
    #[error("bootstrap claim was fenced by Seal generation {observed:?}")]
    Fenced { observed: SealGeneration },
    #[error("bootstrap found a durable contradiction: {detail}")]
    Contradiction { detail: String },
}

#[derive(Debug)]
pub(crate) struct Ready {
    state: WriterState,
}

#[derive(Debug)]
pub(crate) struct AuthoredClaim {
    generation: SealGeneration,
    owner: OwnerToken,
}

impl AuthoredClaim {
    pub(crate) fn new(generation: SealGeneration) -> Self {
        Self {
            generation,
            owner: OwnerToken::from(generation),
        }
    }

    pub(crate) const fn generation(&self) -> SealGeneration {
        self.generation
    }

    pub(crate) const fn owner(&self) -> OwnerToken {
        self.owner
    }
}

impl Ready {
    pub(crate) const fn partition(&self) -> PartitionId {
        self.state.partition()
    }

    pub(crate) const fn forest(&self) -> &Forest {
        self.state.durable_forest()
    }

    pub(crate) fn into_state(self) -> WriterState {
        self.state
    }
}

#[derive(Debug)]
struct ReplayComplete {
    claim: AuthoredClaim,
    seal: Seal,
    base: Forest,
    forest: Forest,
    durable_batch: BatchId,
    next_batch: BatchId,
}

#[derive(Debug, Clone, Copy)]
struct BoundedFence {
    batch: BatchId,
    next_batch: BatchId,
}

#[derive(Debug)]
enum BootstrapPhase {
    DiscoverHead,
    ReadHead {
        generation: SealGeneration,
    },
    PublishClaim {
        candidate: Seal,
        encoded: EncodedAuthoritySeal,
        plan: BootstrapPlan,
    },
    LoadAdmissionBase {
        claim: AuthoredClaim,
        seal: Seal,
        plan: BootstrapPlan,
    },
    PlaceFence {
        claim: AuthoredClaim,
        seal: Seal,
        base: Forest,
        forest: Forest,
        fence: BoundedFence,
        listed_tail: Option<BatchId>,
    },
    Replay {
        claim: AuthoredClaim,
        seal: Seal,
        base: Forest,
        forest: Forest,
        next: BatchId,
        fence: BoundedFence,
        owner: Option<OwnerToken>,
    },
    RefreshAnomaly {
        claim: AuthoredClaim,
        detail: String,
    },
    FinalRefresh {
        replayed: ReplayComplete,
    },
    Ready(Ready),
}

#[derive(Debug)]
struct BootstrapPlan {
    directory: Vec<PlannedRun>,
    ledger: Vec<PlannedRun>,
}

#[derive(Debug)]
struct PlannedRun {
    tables: Vec<TableRef>,
}

#[derive(Debug, Default)]
struct MergedRows {
    directory: OrdMap<DirectoryKey, strom_storage_domain::DirectoryEntry>,
    ledger: OrdMap<StreamUid, strom_storage_domain::StreamRecord>,
    resident_bytes: u64,
}

enum FenceTailGate {
    Clear,
    RefreshAnomaly { detail: String },
}

pub(crate) async fn bootstrap(
    adapter: ObjectStoreAdapter,
    mut entropy: Entropy,
) -> Result<Ready, BootstrapExit> {
    let seal_store = SealStore::new(adapter.clone());
    let wal_store = WalStore::new(adapter.clone());
    let table_store = TableStore::new(adapter);
    let mut partition = None;
    let mut genesis_entropy = entropy.fork("partition-id");
    let mut phase = BootstrapPhase::DiscoverHead;

    loop {
        phase = match phase {
            BootstrapPhase::DiscoverHead => {
                let observed = seal_store
                    .newest_generation()
                    .await
                    .map_err(map_typed_store_error)?;
                let generation = if let Some(generation) = observed {
                    generation
                } else {
                    let minted = mint_partition_id(&mut genesis_entropy);
                    match provision_genesis(&seal_store, minted).await? {
                        GenesisProvision::Established => SealGeneration::genesis(),
                        GenesisProvision::LostRace => {
                            phase = BootstrapPhase::DiscoverHead;
                            continue;
                        }
                    }
                };
                BootstrapPhase::ReadHead { generation }
            }
            BootstrapPhase::ReadHead { generation } => {
                let head = seal_store
                    .read_seal(generation)
                    .await
                    .map_err(map_typed_store_error)?
                    .ok_or_else(|| BootstrapExit::Contradiction {
                        detail: format!("newest Seal {generation:?} is absent"),
                    })?;
                partition = Some(head.partition());
                let plan = plan_bootstrap_sources(&head)?;
                let candidate =
                    head.claim_successor()
                        .map_err(|source| BootstrapExit::Contradiction {
                            detail: format!(
                                "newest Seal cannot form an exact claim successor: {source}"
                            ),
                        })?;
                let encoded = EncodedAuthoritySeal::new(&candidate).map_err(|source| {
                    BootstrapExit::Contradiction {
                        detail: format!("claim Seal could not be encoded: {source}"),
                    }
                })?;
                BootstrapPhase::PublishClaim {
                    candidate,
                    encoded,
                    plan,
                }
            }
            BootstrapPhase::PublishClaim {
                candidate,
                encoded,
                plan,
            } => match seal_store
                .publish_authority(&encoded)
                .await
                .map_err(map_typed_store_error)?
            {
                SealPublication::Authored => {
                    let generation = candidate.generation();
                    BootstrapPhase::LoadAdmissionBase {
                        claim: AuthoredClaim::new(generation),
                        seal: candidate,
                        plan,
                    }
                }
                SealPublication::NoAuthority => {
                    return Err(BootstrapExit::Fenced {
                        observed: candidate.generation(),
                    });
                }
                SealPublication::Unresolved => {
                    return Err(BootstrapExit::Retryable {
                        detail: format!(
                            "claim create at {:?} is unresolved",
                            candidate.generation()
                        ),
                    });
                }
            },
            BootstrapPhase::LoadAdmissionBase { claim, seal, plan } => {
                let partition = partition.expect("ReadHead discovers the partition identity");
                let forest = load_admission_base(&table_store, partition, plan).await?;
                let listed_tail = wal_store
                    .newest_surviving_batch()
                    .await
                    .map_err(map_typed_store_error)?;
                let replay = seal.replay();
                let candidate = plan_fence_candidate(replay.batch(), listed_tail)?;
                let fence = bound_fence(replay.batch(), candidate)?;
                BootstrapPhase::PlaceFence {
                    claim,
                    seal,
                    base: forest.clone(),
                    forest,
                    fence,
                    listed_tail,
                }
            }
            BootstrapPhase::PlaceFence {
                claim,
                seal,
                base,
                forest,
                fence,
                listed_tail,
            } => {
                let partition = partition.expect("ReadHead discovers the partition identity");
                let replay = seal.replay();
                if let FenceTailGate::RefreshAnomaly { detail } = guard_fence_tail(
                    &wal_store,
                    partition,
                    replay.batch(),
                    listed_tail,
                    claim.owner,
                )
                .await?
                {
                    phase = BootstrapPhase::RefreshAnomaly { claim, detail };
                    continue;
                }

                let candidate = fence.batch;
                let object = WalObject::new(partition, candidate, claim.owner, WalBody::Fence);
                let encoded =
                    EncodedWal::new(&object).map_err(|source| BootstrapExit::Contradiction {
                        detail: format!("takeover FENCE could not be encoded: {source}"),
                    })?;
                let establishment = wal_store
                    .establish_wal(&encoded)
                    .await
                    .map_err(map_typed_store_error)?;
                let established = match establishment {
                    WalEstablishment::Durable => true,
                    WalEstablishment::Occupied => false,
                    WalEstablishment::UnresolvedAbsent => {
                        return Err(BootstrapExit::Retryable {
                            detail: format!(
                                "takeover FENCE create at {candidate:?} is unresolved and absent on reconciliation"
                            ),
                        });
                    }
                };
                if established {
                    BootstrapPhase::Replay {
                        claim,
                        seal,
                        base,
                        forest,
                        next: replay_start(replay)?,
                        fence,
                        owner: replay_owner(replay),
                    }
                } else {
                    let listed_tail = wal_store
                        .newest_surviving_batch()
                        .await
                        .map_err(map_typed_store_error)?;
                    BootstrapPhase::PlaceFence {
                        claim,
                        seal,
                        base,
                        forest,
                        fence: replan_fence(replay.batch(), candidate, listed_tail)?,
                        listed_tail,
                    }
                }
            }
            BootstrapPhase::Replay {
                claim,
                seal,
                base,
                mut forest,
                next,
                fence,
                mut owner,
            } => {
                let partition = partition.expect("ReadHead discovers the partition identity");
                let observed = match wal_store.read_wal(partition, next).await {
                    Ok(Some(observed)) => observed,
                    Ok(None) => {
                        phase = BootstrapPhase::RefreshAnomaly {
                            claim,
                            detail: format!("WAL coordinate {next:?} is absent below the FENCE"),
                        };
                        continue;
                    }
                    Err(TypedStoreError::Contradiction { detail }) => {
                        phase = BootstrapPhase::RefreshAnomaly { claim, detail };
                        continue;
                    }
                    Err(
                        error @ (TypedStoreError::Retryable { .. }
                        | TypedStoreError::Rejected { .. }),
                    ) => {
                        return Err(map_typed_store_error(error));
                    }
                };
                let object = observed.object();
                let anomaly = match object.body() {
                    WalBody::Fence => {
                        if owner.is_some_and(|current| object.owner() <= current) {
                            Some(format!(
                                "FENCE at {next:?} does not strictly increase the replay owner"
                            ))
                        } else {
                            owner = Some(object.owner());
                            None
                        }
                    }
                    WalBody::Run(facts) => {
                        if owner == Some(object.owner()) {
                            fold_replay_facts(&mut forest, next, facts.as_slice())
                        } else {
                            Some(format!("RUN at {next:?} does not match the replay owner"))
                        }
                    }
                };
                if let Some(detail) = anomaly {
                    BootstrapPhase::RefreshAnomaly { claim, detail }
                } else if next == fence.batch {
                    if owner == Some(claim.owner) {
                        BootstrapPhase::FinalRefresh {
                            replayed: ReplayComplete {
                                claim,
                                seal,
                                base,
                                forest,
                                durable_batch: fence.batch,
                                next_batch: fence.next_batch,
                            },
                        }
                    } else {
                        BootstrapPhase::RefreshAnomaly {
                            claim,
                            detail: format!(
                                "replay through FENCE {fence:?} ended under a foreign owner"
                            ),
                        }
                    }
                } else {
                    let next =
                        next.successor()
                            .map_err(|_exhausted| BootstrapExit::Contradiction {
                                detail: "WAL replay coordinate is exhausted before the FENCE"
                                    .into(),
                            })?;
                    BootstrapPhase::Replay {
                        claim,
                        seal,
                        base,
                        forest,
                        next,
                        fence,
                        owner,
                    }
                }
            }
            BootstrapPhase::RefreshAnomaly { claim, detail } => {
                let newest = seal_store
                    .newest_generation()
                    .await
                    .map_err(map_typed_store_error)?;
                match newest {
                    Some(observed) if observed > claim.generation => {
                        return Err(BootstrapExit::Fenced { observed });
                    }
                    Some(observed) if observed == claim.generation => {
                        return Err(BootstrapExit::Contradiction { detail });
                    }
                    Some(observed) => {
                        return Err(BootstrapExit::Contradiction {
                            detail: format!(
                                "Seal head regressed from authored claim {:?} to {observed:?} while classifying: {detail}",
                                claim.generation
                            ),
                        });
                    }
                    None => {
                        return Err(BootstrapExit::Contradiction {
                            detail: format!(
                                "Seal namespace became empty while classifying replay anomaly: {detail}"
                            ),
                        });
                    }
                }
            }
            BootstrapPhase::FinalRefresh { replayed } => {
                let newest = seal_store
                    .newest_generation()
                    .await
                    .map_err(map_typed_store_error)?;
                match newest {
                    Some(observed) if observed == replayed.claim.generation => {
                        let partition =
                            partition.expect("ReadHead discovers the partition identity");
                        assert_eq!(
                            partition,
                            replayed.seal.partition(),
                            "the discovered partition matches the selected Seal"
                        );
                        BootstrapPhase::Ready(Ready {
                            state: WriterState::new(
                                replayed.claim,
                                replayed.seal,
                                replayed.base,
                                replayed.forest,
                                replayed.durable_batch,
                                replayed.next_batch,
                            ),
                        })
                    }
                    Some(observed) if observed > replayed.claim.generation => {
                        return Err(BootstrapExit::Fenced { observed });
                    }
                    Some(observed) => {
                        return Err(BootstrapExit::Contradiction {
                            detail: format!(
                                "final Seal refresh regressed from {:?} to {observed:?}",
                                replayed.claim.generation
                            ),
                        });
                    }
                    None => {
                        return Err(BootstrapExit::Contradiction {
                            detail: "Seal namespace is empty during final refresh".into(),
                        });
                    }
                }
            }
            BootstrapPhase::Ready(ready) => return Ok(ready),
        };
    }
}

fn plan_bootstrap_sources(seal: &Seal) -> Result<BootstrapPlan, BootstrapExit> {
    let mut objects = 0usize;
    let mut bytes = 0u64;
    for table in [seal.directory(), seal.ledger()]
        .into_iter()
        .flat_map(strom_storage_domain::TreeVersion::runs)
        .flat_map(strom_storage_domain::SortedRun::tables)
    {
        objects = objects.checked_add(1).ok_or_else(bootstrap_source_bound)?;
        bytes = bytes
            .checked_add(table.object_bytes().get())
            .ok_or_else(bootstrap_source_bound)?;
    }
    if objects > PARTITION_BOOTSTRAP_OBJECTS_MAX_V2 || bytes > PARTITION_BOOTSTRAP_BYTES_MAX_V2 {
        return Err(bootstrap_source_bound());
    }
    Ok(BootstrapPlan {
        directory: plan_tree(seal.directory()),
        ledger: plan_tree(seal.ledger()),
    })
}

fn bootstrap_source_bound() -> BootstrapExit {
    BootstrapExit::Contradiction {
        detail: "Seal-selected table sources exceed a V2 aggregate bootstrap bound".into(),
    }
}

fn plan_tree(tree: &strom_storage_domain::TreeVersion) -> Vec<PlannedRun> {
    tree.runs()
        .iter()
        .rev()
        .map(|run| PlannedRun {
            tables: run.tables().to_vec(),
        })
        .collect()
}

async fn load_admission_base(
    store: &TableStore,
    partition: PartitionId,
    plan: BootstrapPlan,
) -> Result<Forest, BootstrapExit> {
    let mut merged = MergedRows::default();

    for run in plan.directory {
        let mut previous_last: Option<DirectoryKey> = None;
        for table in run.tables {
            let rows = match store
                .read_table(partition, &table)
                .await
                .map_err(map_typed_store_error)?
            {
                TableRows::Directory(rows) => rows,
                TableRows::Ledger(_) => {
                    return Err(BootstrapExit::Contradiction {
                        detail: "Directory manifest selected a Ledger table".into(),
                    });
                }
            };
            previous_last = merge_directory_table(&mut merged, previous_last.as_ref(), rows)?;
        }
    }

    for run in plan.ledger {
        let mut previous_last: Option<StreamUid> = None;
        for table in run.tables {
            let rows = match store
                .read_table(partition, &table)
                .await
                .map_err(map_typed_store_error)?
            {
                TableRows::Ledger(rows) => rows,
                TableRows::Directory(_) => {
                    return Err(BootstrapExit::Contradiction {
                        detail: "Ledger manifest selected a Directory table".into(),
                    });
                }
            };
            previous_last = merge_ledger_table(&mut merged, previous_last, rows)?;
        }
    }

    Forest::try_from_merged(merged.directory, merged.ledger).map_err(map_forest_error)
}

fn merge_directory_table(
    merged: &mut MergedRows,
    previous_last: Option<&DirectoryKey>,
    rows: Vec<(DirectoryKey, strom_storage_domain::DirectoryEntry)>,
) -> Result<Option<DirectoryKey>, BootstrapExit> {
    let first = rows
        .first()
        .expect("checked SST decoding produces a nonempty table");
    if previous_last.is_some_and(|previous| first.0 <= *previous) {
        return Err(BootstrapExit::Contradiction {
            detail: "Directory tables within one sorted run overlap or are unordered".into(),
        });
    }
    let last = rows.last().map(|(key, _entry)| key.clone());
    for (key, entry) in rows {
        if !merged.directory.contains_key(&key) {
            merged.resident_bytes =
                add_resident_bytes(merged.resident_bytes, DIRECTORY_ROW_LOGICAL_BYTES_MAX)?;
        }
        merged.directory.insert(key, entry);
    }
    Ok(last)
}

fn merge_ledger_table(
    merged: &mut MergedRows,
    previous_last: Option<StreamUid>,
    rows: Vec<(StreamUid, LedgerCell)>,
) -> Result<Option<StreamUid>, BootstrapExit> {
    let first = rows
        .first()
        .expect("checked SST decoding produces a nonempty table");
    if previous_last.is_some_and(|previous| first.0 <= previous) {
        return Err(BootstrapExit::Contradiction {
            detail: "Ledger tables within one sorted run overlap or are unordered".into(),
        });
    }
    let last = rows.last().map(|(uid, _cell)| *uid);
    for (uid, cell) in rows {
        match cell {
            LedgerCell::Value(record) => {
                if !merged.ledger.contains_key(&uid) {
                    merged.resident_bytes = add_resident_bytes(
                        merged.resident_bytes,
                        LEDGER_VALUE_ROW_LOGICAL_BYTES_MAX,
                    )?;
                }
                merged.ledger.insert(uid, record);
            }
            LedgerCell::Delete => {
                if merged.ledger.remove(&uid).is_some() {
                    merged.resident_bytes = merged
                        .resident_bytes
                        .checked_sub(LEDGER_VALUE_ROW_LOGICAL_BYTES_MAX)
                        .expect("resident accounting removes only an existing record");
                }
            }
        }
    }
    Ok(last)
}

fn replan_fence(
    cut: Option<BatchId>,
    occupied: BatchId,
    listed_tail: Option<BatchId>,
) -> Result<BoundedFence, BootstrapExit> {
    let candidate = plan_fence_candidate(cut, listed_tail)?;
    if candidate <= occupied {
        return Err(BootstrapExit::Contradiction {
            detail: format!("WAL list did not advance past occupied FENCE candidate {occupied:?}"),
        });
    }
    bound_fence(cut, candidate)
}

fn plan_fence_candidate(
    cut: Option<BatchId>,
    listed_tail: Option<BatchId>,
) -> Result<BatchId, BootstrapExit> {
    match cut.into_iter().chain(listed_tail).max() {
        Some(tail) => tail
            .successor()
            .map_err(|_exhausted| BootstrapExit::Retryable {
                detail: "WAL coordinate space is exhausted before takeover".into(),
            }),
        None => BatchId::try_from(1).map_err(|_zero| BootstrapExit::Contradiction {
            detail: "batch one must be a legal WAL coordinate".into(),
        }),
    }
}

async fn guard_fence_tail(
    store: &WalStore,
    partition: PartitionId,
    cut: Option<BatchId>,
    listed_tail: Option<BatchId>,
    claim_owner: OwnerToken,
) -> Result<FenceTailGate, BootstrapExit> {
    let Some(tail) = listed_tail.filter(|tail| cut.is_none_or(|cut| *tail > cut)) else {
        return Ok(FenceTailGate::Clear);
    };
    let observed = match store.read_wal(partition, tail).await {
        Ok(Some(observed)) => observed,
        Ok(None) => {
            return Err(BootstrapExit::Retryable {
                detail: format!("listed WAL tail {tail:?} disappeared before FENCE placement"),
            });
        }
        Err(TypedStoreError::Contradiction { detail }) => {
            return Ok(FenceTailGate::RefreshAnomaly { detail });
        }
        Err(error) => return Err(map_typed_store_error(error)),
    };
    if observed.object().owner() >= claim_owner {
        return Ok(FenceTailGate::RefreshAnomaly {
            detail: format!(
                "listed WAL tail {tail:?} has owner {:?}, not older than the authored owner",
                observed.object().owner()
            ),
        });
    }
    Ok(FenceTailGate::Clear)
}

fn bound_fence(cut: Option<BatchId>, fence: BatchId) -> Result<BoundedFence, BootstrapExit> {
    let next_batch = fence
        .successor()
        .map_err(|_exhausted| BootstrapExit::Retryable {
            detail: "WAL coordinate space has no RUN coordinate after the takeover FENCE".into(),
        })?;
    let cut = cut.map_or(0, BatchId::get);
    let span = fence
        .get()
        .checked_sub(cut)
        .filter(|span| *span > 0)
        .ok_or_else(|| BootstrapExit::Contradiction {
            detail: "takeover FENCE is not strictly after the Seal replay cut".into(),
        })?;
    if span > WAL_SUFFIX_COORDINATES_MAX_V2 {
        return Err(BootstrapExit::Retryable {
            detail: format!(
                "WAL suffix through takeover FENCE spans {span} coordinates; the bound is {WAL_SUFFIX_COORDINATES_MAX_V2}"
            ),
        });
    }
    Ok(BoundedFence {
        batch: fence,
        next_batch,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GenesisProvision {
    Established,
    LostRace,
}

async fn provision_genesis(
    store: &SealStore,
    partition: PartitionId,
) -> Result<GenesisProvision, BootstrapExit> {
    let generation = SealGeneration::genesis();
    let genesis = Seal::new(
        partition,
        generation,
        WalReplayPoint::Genesis,
        strom_storage_domain::TreeVersion::empty(),
        strom_storage_domain::TreeVersion::empty(),
    )
    .expect("canonical empty genesis satisfies every Seal invariant");
    let encoded =
        EncodedGenesisSeal::new(&genesis).map_err(|source| BootstrapExit::Contradiction {
            detail: format!("canonical genesis could not be encoded: {source}"),
        })?;
    match store
        .establish_genesis(&encoded)
        .await
        .map_err(map_typed_store_error)?
    {
        GenesisEstablishment::Established => Ok(GenesisProvision::Established),
        GenesisEstablishment::LostRace => Ok(GenesisProvision::LostRace),
        GenesisEstablishment::Unresolved => Err(BootstrapExit::Retryable {
            detail: "canonical genesis create is unresolved".into(),
        }),
    }
}

fn mint_partition_id(entropy: &mut Entropy) -> PartitionId {
    loop {
        let mut bytes = [0; 16];
        entropy.rng().fill_bytes(&mut bytes);
        if let Ok(partition) = PartitionId::try_from(bytes) {
            return partition;
        }
    }
}

const fn replay_owner(replay: WalReplayPoint) -> Option<OwnerToken> {
    match replay {
        WalReplayPoint::Genesis => None,
        WalReplayPoint::Through { batch: _, owner } => Some(owner),
    }
}

fn replay_start(replay: WalReplayPoint) -> Result<BatchId, BootstrapExit> {
    match replay {
        WalReplayPoint::Genesis => {
            BatchId::try_from(1).map_err(|_zero| BootstrapExit::Contradiction {
                detail: "batch one must be a legal WAL coordinate".into(),
            })
        }
        WalReplayPoint::Through { batch, owner: _ } => {
            batch
                .successor()
                .map_err(|_exhausted| BootstrapExit::Retryable {
                    detail: "Seal replay cut occupies the final WAL coordinate".into(),
                })
        }
    }
}

fn fold_replay_facts(
    forest: &mut Forest,
    batch: BatchId,
    facts: &[OperationFact],
) -> Option<String> {
    for fact in facts {
        if let Err(contradiction) = forest.strict_fold(batch, fact) {
            return Some(format!(
                "fact in WAL RUN {batch:?} contradicts recovered state: {contradiction}"
            ));
        }
    }
    None
}

fn add_resident_bytes(current: u64, additional: u64) -> Result<u64, BootstrapExit> {
    let total = current.checked_add(additional).ok_or_else(resident_bound)?;
    if total > PARTITION_RESIDENT_LOGICAL_BYTES_MAX_V2 {
        return Err(resident_bound());
    }
    Ok(total)
}

fn resident_bound() -> BootstrapExit {
    BootstrapExit::Contradiction {
        detail: "merged Directory and Ledger rows exceed the V2 resident logical-byte bound".into(),
    }
}

fn map_forest_error(error: ForestContradiction) -> BootstrapExit {
    BootstrapExit::Contradiction {
        detail: format!("merged Directory and Ledger rows disagree: {error}"),
    }
}

fn map_typed_store_error(error: TypedStoreError) -> BootstrapExit {
    match error {
        TypedStoreError::Retryable { detail } => BootstrapExit::Retryable { detail },
        TypedStoreError::Rejected { detail } | TypedStoreError::Contradiction { detail } => {
            BootstrapExit::Contradiction { detail }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use strom_domain::{ExpiryPolicy, StreamContentType, StreamLifecycle};
    use strom_object_store::{CreateEvidence, FrozenBytes, ObjectKey};
    use strom_storage_domain::{
        AttemptId, FreshIdentity, LedgerCell, SortedRun, StoreKind, StreamRecord, TableObjectId,
        TreeVersion, WalKey, encode_wal,
    };

    use super::*;

    #[test]
    fn fence_planning_clamps_to_the_greater_cut_or_surviving_tail()
    -> Result<(), Box<dyn std::error::Error>> {
        let cut = BatchId::try_from(10)?;
        let older_tail = BatchId::try_from(3)?;
        let newer_tail = BatchId::try_from(19)?;
        assert_eq!(BatchId::try_from(1)?, plan_fence_candidate(None, None)?);
        assert_eq!(
            BatchId::try_from(11)?,
            plan_fence_candidate(Some(cut), Some(older_tail))?
        );
        assert_eq!(
            BatchId::try_from(20)?,
            plan_fence_candidate(Some(cut), Some(newer_tail))?
        );
        Ok(())
    }

    #[test]
    fn occupied_fence_replanning_rejects_a_nonadvancing_list()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            replan_fence(None, BatchId::try_from(1)?, None),
            Err(BootstrapExit::Contradiction { .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn stale_claimant_stops_before_appending_a_decreasing_owner_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        let adapter = ObjectStoreAdapter::in_memory();
        let store = WalStore::new(adapter.clone());
        let partition = partition();
        let stale_generation = SealGeneration::genesis().successor()?;
        let newer_generation = stale_generation.successor()?;
        let newer_fence = WalObject::new(
            partition,
            BatchId::try_from(1)?,
            OwnerToken::from(newer_generation),
            WalBody::Fence,
        );
        assert_eq!(
            CreateEvidence::Direct,
            adapter
                .create_if_absent(
                    &ObjectKey::try_from(WalKey::from(newer_fence.batch()).to_string())?,
                    FrozenBytes::try_from(encode_wal(&newer_fence)?)?,
                )
                .await?
        );

        assert!(matches!(
            guard_fence_tail(
                &store,
                partition,
                None,
                Some(BatchId::try_from(1)?),
                OwnerToken::from(stale_generation),
            )
            .await?,
            FenceTailGate::RefreshAnomaly { .. }
        ));
        assert!(
            store
                .read_wal(partition, BatchId::try_from(2)?)
                .await?
                .is_none(),
            "the stale claimant performs no create after observing the newer owner"
        );
        Ok(())
    }

    #[test]
    fn replay_span_accepts_the_bound_and_rejects_one_beyond()
    -> Result<(), Box<dyn std::error::Error>> {
        let cut = BatchId::try_from(50)?;
        let at_bound = BatchId::try_from(
            cut.get()
                .checked_add(WAL_SUFFIX_COORDINATES_MAX_V2)
                .expect("test coordinate fits"),
        )?;
        assert_eq!(at_bound, bound_fence(Some(cut), at_bound)?.batch);
        assert!(matches!(
            bound_fence(Some(cut), at_bound.successor()?),
            Err(BootstrapExit::Retryable { .. })
        ));
        assert!(matches!(
            bound_fence(
                Some(BatchId::try_from(u64::MAX - 1)?),
                BatchId::try_from(u64::MAX)?
            ),
            Err(BootstrapExit::Retryable { .. })
        ));
        Ok(())
    }

    #[test]
    fn pure_table_merge_rejects_run_overlap() -> Result<(), Box<dyn std::error::Error>> {
        let uid = StreamUid::try_from(1)?;
        let mut merged = MergedRows::default();
        let last = merge_ledger_table(
            &mut merged,
            None,
            vec![(uid, LedgerCell::Value(stream_record()?))],
        )?;
        assert!(matches!(
            merge_ledger_table(
                &mut merged,
                last,
                vec![(uid, LedgerCell::Value(stream_record()?))],
            ),
            Err(BootstrapExit::Contradiction { .. })
        ));
        Ok(())
    }

    #[test]
    fn source_planner_checks_aggregate_object_and_byte_bounds_before_loading()
    -> Result<(), Box<dyn std::error::Error>> {
        let object_limit = seal_with_tables(PARTITION_BOOTSTRAP_OBJECTS_MAX_V2, NonZeroU64::MIN)?;
        assert_eq!(
            Ok(()),
            plan_bootstrap_sources(&object_limit).map(|_plan| ())
        );
        let object_over = seal_with_tables(
            PARTITION_BOOTSTRAP_OBJECTS_MAX_V2
                .checked_add(1)
                .expect("test count fits"),
            NonZeroU64::MIN,
        )?;
        assert!(matches!(
            plan_bootstrap_sources(&object_over),
            Err(BootstrapExit::Contradiction { .. })
        ));

        let table_bytes_max = NonZeroU64::new(strom_storage_domain::SST_OBJECT_BYTES_MAX)
            .expect("the SST bound is nonzero");
        let byte_limit_count = 256usize;
        assert_eq!(
            Some(PARTITION_BOOTSTRAP_BYTES_MAX_V2),
            u64::try_from(byte_limit_count)?
                .checked_mul(strom_storage_domain::SST_OBJECT_BYTES_MAX),
            "the fixture count lands exactly on the aggregate byte bound"
        );
        let byte_limit = seal_with_tables(byte_limit_count, table_bytes_max)?;
        assert_eq!(Ok(()), plan_bootstrap_sources(&byte_limit).map(|_plan| ()));
        let byte_over = seal_with_tables(
            byte_limit_count.checked_add(1).expect("test count fits"),
            table_bytes_max,
        )?;
        assert!(matches!(
            plan_bootstrap_sources(&byte_over),
            Err(BootstrapExit::Contradiction { .. })
        ));
        Ok(())
    }

    fn seal_with_tables(
        count: usize,
        object_bytes: NonZeroU64,
    ) -> Result<Seal, Box<dyn std::error::Error>> {
        let generation = SealGeneration::genesis().successor()?;
        let owner = SealGeneration::genesis();
        let mut tables = Vec::with_capacity(count);
        for ordinal in 0..count {
            let ordinal = u32::try_from(ordinal)?;
            let fresh = FreshIdentity::new(generation, AttemptId::new(owner, 1), ordinal)?;
            tables.push(TableRef::new(
                TableObjectId::new(fresh, StoreKind::Directory),
                object_bytes,
            )?);
        }
        let directory = TreeVersion::try_from(vec![SortedRun::try_from(tables)?])?;
        Ok(Seal::new(
            partition(),
            generation,
            WalReplayPoint::Genesis,
            directory,
            TreeVersion::empty(),
        )?)
    }

    fn stream_record() -> Result<StreamRecord, Box<dyn std::error::Error>> {
        Ok(StreamRecord::new(
            StreamContentType::octet_stream(),
            ExpiryPolicy::None,
            StreamLifecycle::Open,
            BatchId::try_from(1)?,
        ))
    }

    fn partition() -> PartitionId {
        "00112233-4455-6677-8899-aabbccddeeff"
            .parse()
            .expect("test partition is canonical")
    }
}
