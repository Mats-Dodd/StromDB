//! Pure bounded bootstrap correctness machine.

use std::collections::VecDeque;

use imbl::OrdMap;
use strom_domain::StreamPath;
use strom_storage_domain::{
    BatchId, DIRECTORY_ROW_LOGICAL_BYTES_MAX, DecodedTable, DirectoryEntry, EncodedAuthoritySeal,
    EncodedGenesisSeal, EncodedWal, LEDGER_VALUE_ROW_LOGICAL_BYTES_MAX, LedgerCell, OwnerToken,
    PARTITION_BOOTSTRAP_BYTES_MAX_V2, PARTITION_BOOTSTRAP_OBJECTS_MAX_V2,
    PARTITION_RESIDENT_LOGICAL_BYTES_MAX_V2, PartitionId, Seal, SealGeneration, StreamRecord,
    StreamUid, TableRef, WAL_SUFFIX_COORDINATES_MAX_V2, WalBody, WalObject, WalReplayPoint,
};

use crate::suffix::{self, TakeoverFence, TakeoverFenceError};
use crate::writer::AuthoredClaim;
use crate::{
    Forest, ForestContradiction, GenesisEstablishment, SealPublication, TypedStoreError,
    WalEstablishment, WriterRecovery,
};

/// Why a partition did not become ready.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BootstrapExit {
    #[error("bootstrap should be retried: {detail}")]
    Retryable { detail: String },
    #[error("bootstrap claim was fenced by Seal generation {observed:?}")]
    Fenced { observed: SealGeneration },
    #[error("bootstrap found a durable contradiction: {detail}")]
    Contradiction { detail: String },
}

/// One observation delivered to the pure bootstrap machine.
#[derive(Debug)]
pub enum BootstrapEvent {
    Started {
        genesis_partition: PartitionId,
    },
    HeadObserved(Option<SealGeneration>),
    GenesisEstablished(GenesisEstablishment),
    SealRead(Option<Seal>),
    ClaimPublished(SealPublication),
    TableRead {
        table: TableRef,
        decoded: DecodedTable,
    },
    WalTailObserved(Option<BatchId>),
    WalRead(Option<WalObject>),
    FenceEstablished(WalEstablishment),
    StoreFailed(TypedStoreError),
}

/// One completion-producing operation requested by bootstrap.
#[derive(Debug)]
pub enum BootstrapEffect {
    ObserveHead,
    EstablishGenesis(EncodedGenesisSeal),
    ReadSeal {
        generation: SealGeneration,
    },
    PublishClaim(EncodedAuthoritySeal),
    ReadTable {
        partition: PartitionId,
        table: TableRef,
    },
    ObserveWalTail,
    ReadWal {
        partition: PartitionId,
        batch: BatchId,
    },
    EstablishFence(EncodedWal),
}

/// One complete synchronous bootstrap transition.
#[derive(Debug)]
pub enum BootstrapStep {
    Effect(BootstrapEffect),
    Complete(WriterRecovery),
    Exit(BootstrapExit),
}

#[derive(Debug)]
enum BootstrapState {
    Initial,
    DiscoveringHead {
        genesis_partition: PartitionId,
    },
    EstablishingGenesis {
        genesis_partition: PartitionId,
    },
    ReadingSeal {
        generation: SealGeneration,
    },
    PublishingClaim {
        candidate: Seal,
        sources: VecDeque<PlannedTable>,
    },
    ReadingTable {
        load: BaseLoad,
        source: PlannedTable,
    },
    ObservingWalTail {
        loaded: LoadedBootstrap,
        occupied: Option<BatchId>,
    },
    ReadingWalTail {
        loaded: LoadedBootstrap,
        fence: TakeoverFence,
        listed_tail: BatchId,
    },
    EstablishingFence {
        loaded: LoadedBootstrap,
        fence: TakeoverFence,
    },
    Replaying(Replay),
    RefreshingAnomaly {
        claim: AuthoredClaim,
        detail: String,
    },
    RefreshingFinal {
        recovery: WriterRecovery,
    },
    Complete,
}

#[derive(Debug)]
struct ClaimedBootstrap {
    claim: AuthoredClaim,
    seal: Seal,
}

#[derive(Debug)]
struct LoadedBootstrap {
    claimed: ClaimedBootstrap,
    base: Forest,
    durable: Forest,
}

#[derive(Debug)]
struct Replay {
    loaded: LoadedBootstrap,
    next: BatchId,
    fence: TakeoverFence,
    owner: Option<OwnerToken>,
}

#[derive(Debug)]
struct PlannedTable {
    table: TableRef,
    starts_run: bool,
}

#[derive(Debug)]
struct BaseLoad {
    claimed: ClaimedBootstrap,
    remaining: VecDeque<PlannedTable>,
    merged: MergedRows,
    previous_directory: Option<StreamPath>,
    previous_ledger: Option<StreamUid>,
}

#[derive(Debug, Default)]
struct MergedRows {
    directory: OrdMap<StreamPath, DirectoryEntry>,
    ledger: OrdMap<StreamUid, StreamRecord>,
    resident_bytes: u64,
}

/// Pure bootstrap protocol. Exactly one effect may be outstanding.
#[derive(Debug)]
pub struct BootstrapMachine {
    state: Option<BootstrapState>,
}

impl BootstrapMachine {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: Some(BootstrapState::Initial),
        }
    }

    /// Apply one observation and return the next exact effect or terminal result.
    ///
    /// # Panics
    ///
    /// Panics when an event does not complete the effect issued by the current
    /// state or when bootstrap is driven after termination.
    pub fn handle(&mut self, event: BootstrapEvent) -> BootstrapStep {
        let state = self
            .state
            .take()
            .expect("a bootstrap transition owns its current state");
        match event {
            BootstrapEvent::Started { genesis_partition } => {
                assert!(
                    matches!(state, BootstrapState::Initial),
                    "bootstrap starts exactly once before receiving completions"
                );
                self.effect(
                    BootstrapState::DiscoveringHead { genesis_partition },
                    BootstrapEffect::ObserveHead,
                )
            }
            BootstrapEvent::HeadObserved(result) => self.handle_head(state, result),
            BootstrapEvent::GenesisEstablished(result) => self.handle_genesis(&state, result),
            BootstrapEvent::SealRead(result) => self.handle_seal(&state, result),
            BootstrapEvent::ClaimPublished(result) => self.handle_claim(state, result),
            BootstrapEvent::TableRead { table, decoded } => {
                self.handle_table(state, table, decoded)
            }
            BootstrapEvent::WalTailObserved(result) => self.handle_wal_tail(state, result),
            BootstrapEvent::WalRead(result) => self.handle_wal_read(state, result),
            BootstrapEvent::FenceEstablished(result) => self.handle_fence(state, result),
            BootstrapEvent::StoreFailed(error) => self.handle_store_failure(state, error),
        }
    }

    fn handle_head(
        &mut self,
        state: BootstrapState,
        observed: Option<SealGeneration>,
    ) -> BootstrapStep {
        match state {
            BootstrapState::DiscoveringHead { genesis_partition } => match observed {
                Some(generation) => self.effect(
                    BootstrapState::ReadingSeal { generation },
                    BootstrapEffect::ReadSeal { generation },
                ),
                None => self.establish_genesis(genesis_partition),
            },
            BootstrapState::RefreshingAnomaly { claim, detail } => match observed {
                Some(observed) if observed > claim.generation() => {
                    self.exit(BootstrapExit::Fenced { observed })
                }
                Some(observed) if observed == claim.generation() => {
                    self.exit(BootstrapExit::Contradiction { detail })
                }
                Some(observed) => self.exit(BootstrapExit::Contradiction {
                    detail: format!(
                        "Seal head regressed from authored claim {:?} to {observed:?} while classifying: {detail}",
                        claim.generation()
                    ),
                }),
                None => self.exit(BootstrapExit::Contradiction {
                    detail: format!(
                        "Seal namespace became empty while classifying replay anomaly: {detail}"
                    ),
                }),
            },
            BootstrapState::RefreshingFinal { recovery } => match observed {
                Some(observed) if observed == recovery.claim.generation() => {
                    self.complete(recovery)
                }
                Some(observed) if observed > recovery.claim.generation() => {
                    self.exit(BootstrapExit::Fenced { observed })
                }
                Some(observed) => self.exit(BootstrapExit::Contradiction {
                    detail: format!(
                        "final Seal refresh regressed from {:?} to {observed:?}",
                        recovery.claim.generation()
                    ),
                }),
                None => self.exit(BootstrapExit::Contradiction {
                    detail: "Seal namespace is empty during final refresh".into(),
                }),
            },
            state
            @ (BootstrapState::Initial
            | BootstrapState::EstablishingGenesis { .. }
            | BootstrapState::ReadingSeal { .. }
            | BootstrapState::PublishingClaim { .. }
            | BootstrapState::ReadingTable { .. }
            | BootstrapState::ObservingWalTail { .. }
            | BootstrapState::ReadingWalTail { .. }
            | BootstrapState::EstablishingFence { .. }
            | BootstrapState::Replaying(_)
            | BootstrapState::Complete) => unexpected(&state, "HeadObserved"),
        }
    }

    fn establish_genesis(&mut self, partition: PartitionId) -> BootstrapStep {
        let genesis = Seal::new(
            partition,
            SealGeneration::genesis(),
            WalReplayPoint::Genesis,
            strom_storage_domain::TreeVersion::empty(),
            strom_storage_domain::TreeVersion::empty(),
        )
        .expect("canonical empty genesis satisfies every Seal invariant");
        match EncodedGenesisSeal::try_from(&genesis) {
            Ok(encoded) => self.effect(
                BootstrapState::EstablishingGenesis {
                    genesis_partition: partition,
                },
                BootstrapEffect::EstablishGenesis(encoded),
            ),
            Err(source) => self.exit(BootstrapExit::Contradiction {
                detail: format!("canonical genesis could not be encoded: {source}"),
            }),
        }
    }

    fn handle_genesis(
        &mut self,
        state: &BootstrapState,
        establishment: GenesisEstablishment,
    ) -> BootstrapStep {
        let BootstrapState::EstablishingGenesis { genesis_partition } = state else {
            unexpected(state, "GenesisEstablished");
        };
        match establishment {
            GenesisEstablishment::Established => self.effect(
                BootstrapState::ReadingSeal {
                    generation: SealGeneration::genesis(),
                },
                BootstrapEffect::ReadSeal {
                    generation: SealGeneration::genesis(),
                },
            ),
            GenesisEstablishment::LostRace => self.effect(
                BootstrapState::DiscoveringHead {
                    genesis_partition: *genesis_partition,
                },
                BootstrapEffect::ObserveHead,
            ),
            GenesisEstablishment::Unresolved => self.exit(BootstrapExit::Retryable {
                detail: "canonical genesis create is unresolved".into(),
            }),
        }
    }

    fn handle_seal(&mut self, state: &BootstrapState, observed: Option<Seal>) -> BootstrapStep {
        let BootstrapState::ReadingSeal { generation } = state else {
            unexpected(state, "SealRead");
        };
        let head = match observed {
            Some(head) => {
                assert_eq!(
                    *generation,
                    head.generation(),
                    "a Seal read completion retains its exact issued generation"
                );
                head
            }
            None => {
                return self.exit(BootstrapExit::Contradiction {
                    detail: format!("newest Seal {generation:?} is absent"),
                });
            }
        };
        let sources = match plan_bootstrap_sources(&head) {
            Ok(sources) => sources,
            Err(exit) => return self.exit(exit),
        };
        let candidate = match head.claim_successor() {
            Ok(candidate) => candidate,
            Err(source) => {
                return self.exit(BootstrapExit::Contradiction {
                    detail: format!("newest Seal cannot form an exact claim successor: {source}"),
                });
            }
        };
        match EncodedAuthoritySeal::try_from(&candidate) {
            Ok(encoded) => self.effect(
                BootstrapState::PublishingClaim { candidate, sources },
                BootstrapEffect::PublishClaim(encoded),
            ),
            Err(source) => self.exit(BootstrapExit::Contradiction {
                detail: format!("claim Seal could not be encoded: {source}"),
            }),
        }
    }

    fn handle_claim(
        &mut self,
        state: BootstrapState,
        publication: SealPublication,
    ) -> BootstrapStep {
        let BootstrapState::PublishingClaim { candidate, sources } = state else {
            unexpected(&state, "ClaimPublished");
        };
        match publication {
            SealPublication::Authored => {
                let claimed = ClaimedBootstrap {
                    claim: AuthoredClaim::new(candidate.generation()),
                    seal: candidate,
                };
                self.read_next_table(BaseLoad {
                    claimed,
                    remaining: sources,
                    merged: MergedRows::default(),
                    previous_directory: None,
                    previous_ledger: None,
                })
            }
            SealPublication::NoAuthority => self.exit(BootstrapExit::Fenced {
                observed: candidate.generation(),
            }),
            SealPublication::Unresolved => self.exit(BootstrapExit::Retryable {
                detail: format!("claim create at {:?} is unresolved", candidate.generation()),
            }),
        }
    }

    fn read_next_table(&mut self, mut load: BaseLoad) -> BootstrapStep {
        let Some(source) = load.remaining.pop_front() else {
            let forest = match Forest::try_from((load.merged.directory, load.merged.ledger)) {
                Ok(forest) => forest,
                Err(error) => return self.exit(map_forest_error(error)),
            };
            let loaded = LoadedBootstrap {
                claimed: load.claimed,
                base: forest.clone(),
                durable: forest,
            };
            return self.effect(
                BootstrapState::ObservingWalTail {
                    loaded,
                    occupied: None,
                },
                BootstrapEffect::ObserveWalTail,
            );
        };
        let effect = BootstrapEffect::ReadTable {
            partition: load.claimed.seal.partition(),
            table: source.table,
        };
        self.effect(BootstrapState::ReadingTable { load, source }, effect)
    }

    fn handle_table(
        &mut self,
        state: BootstrapState,
        table: TableRef,
        decoded: DecodedTable,
    ) -> BootstrapStep {
        let BootstrapState::ReadingTable { mut load, source } = state else {
            unexpected(&state, "TableRead");
        };
        assert_eq!(
            source.table, table,
            "a table read completion retains its exact issued identity"
        );
        let merge = match decoded {
            DecodedTable::Directory(rows) => {
                assert_eq!(
                    strom_storage_domain::StoreKind::Directory,
                    source.table.object().store(),
                    "a typed Directory completion matches its requested table store"
                );
                if source.starts_run {
                    load.previous_directory = None;
                }
                merge_directory_table(&mut load.merged, load.previous_directory.as_ref(), rows)
                    .map(|last| load.previous_directory = last)
            }
            DecodedTable::Ledger(rows) => {
                assert_eq!(
                    strom_storage_domain::StoreKind::Ledger,
                    source.table.object().store(),
                    "a typed Ledger completion matches its requested table store"
                );
                if source.starts_run {
                    load.previous_ledger = None;
                }
                merge_ledger_table(&mut load.merged, load.previous_ledger, rows)
                    .map(|last| load.previous_ledger = last)
            }
        };
        match merge {
            Ok(()) => self.read_next_table(load),
            Err(exit) => self.exit(exit),
        }
    }

    fn handle_wal_tail(
        &mut self,
        state: BootstrapState,
        listed_tail: Option<BatchId>,
    ) -> BootstrapStep {
        let BootstrapState::ObservingWalTail { loaded, occupied } = state else {
            unexpected(&state, "WalTailObserved");
        };
        let cut = loaded.claimed.seal.replay().batch();
        let candidate = match plan_fence_candidate(cut, listed_tail) {
            Ok(candidate) => candidate,
            Err(exit) => return self.exit(exit),
        };
        match occupied {
            Some(occupied) if candidate <= occupied => {
                return self.exit(BootstrapExit::Contradiction {
                    detail: format!(
                        "WAL list did not advance past occupied FENCE candidate {occupied:?}"
                    ),
                });
            }
            Some(_) | None => {}
        }
        let fence = match suffix::bound_takeover_fence(cut, candidate) {
            Ok(fence) => fence,
            Err(error) => return self.exit(map_takeover_fence_error(error)),
        };
        if let Some(tail) = listed_tail.filter(|tail| cut.is_none_or(|cut| *tail > cut)) {
            let partition = loaded.claimed.seal.partition();
            self.effect(
                BootstrapState::ReadingWalTail {
                    loaded,
                    fence,
                    listed_tail: tail,
                },
                BootstrapEffect::ReadWal {
                    partition,
                    batch: tail,
                },
            )
        } else {
            self.establish_fence(loaded, fence)
        }
    }

    fn handle_wal_read(
        &mut self,
        state: BootstrapState,
        observed: Option<WalObject>,
    ) -> BootstrapStep {
        match state {
            BootstrapState::ReadingWalTail {
                loaded,
                fence,
                listed_tail,
            } => self.handle_tail_read(loaded, fence, listed_tail, observed),
            BootstrapState::Replaying(replay) => self.handle_replay_read(replay, observed),
            state @ (BootstrapState::Initial
            | BootstrapState::DiscoveringHead { .. }
            | BootstrapState::EstablishingGenesis { .. }
            | BootstrapState::ReadingSeal { .. }
            | BootstrapState::PublishingClaim { .. }
            | BootstrapState::ReadingTable { .. }
            | BootstrapState::ObservingWalTail { .. }
            | BootstrapState::EstablishingFence { .. }
            | BootstrapState::RefreshingAnomaly { .. }
            | BootstrapState::RefreshingFinal { .. }
            | BootstrapState::Complete) => unexpected(&state, "WalRead"),
        }
    }

    fn handle_tail_read(
        &mut self,
        loaded: LoadedBootstrap,
        fence: TakeoverFence,
        listed_tail: BatchId,
        observed: Option<WalObject>,
    ) -> BootstrapStep {
        let Some(observed) = observed else {
            return self.exit(BootstrapExit::Retryable {
                detail: format!(
                    "listed WAL tail {listed_tail:?} disappeared before FENCE placement"
                ),
            });
        };
        assert_eq!(
            listed_tail,
            observed.batch(),
            "a WAL read completion retains its exact issued batch"
        );
        assert_eq!(
            loaded.claimed.seal.partition(),
            observed.partition(),
            "a WAL read completion retains its exact issued partition"
        );
        if observed.owner() >= loaded.claimed.claim.owner() {
            let detail = format!(
                "listed WAL tail {listed_tail:?} has owner {:?}, not older than the authored owner",
                observed.owner()
            );
            self.refresh_anomaly(loaded.claimed.claim, detail)
        } else {
            self.establish_fence(loaded, fence)
        }
    }

    fn establish_fence(&mut self, loaded: LoadedBootstrap, fence: TakeoverFence) -> BootstrapStep {
        let object = WalObject::new(
            loaded.claimed.seal.partition(),
            fence.batch(),
            loaded.claimed.claim.owner(),
            WalBody::Fence,
        );
        match EncodedWal::new(&object) {
            Ok(encoded) => self.effect(
                BootstrapState::EstablishingFence { loaded, fence },
                BootstrapEffect::EstablishFence(encoded),
            ),
            Err(source) => self.exit(BootstrapExit::Contradiction {
                detail: format!("takeover FENCE could not be encoded: {source}"),
            }),
        }
    }

    fn handle_fence(
        &mut self,
        state: BootstrapState,
        establishment: WalEstablishment,
    ) -> BootstrapStep {
        let BootstrapState::EstablishingFence { loaded, fence } = state else {
            unexpected(&state, "FenceEstablished");
        };
        match establishment {
            WalEstablishment::Durable => {
                let replay = loaded.claimed.seal.replay();
                let next = match replay_start(replay) {
                    Ok(next) => next,
                    Err(exit) => return self.exit(exit),
                };
                let partition = loaded.claimed.seal.partition();
                self.effect(
                    BootstrapState::Replaying(Replay {
                        loaded,
                        next,
                        fence,
                        owner: replay_owner(replay),
                    }),
                    BootstrapEffect::ReadWal {
                        partition,
                        batch: next,
                    },
                )
            }
            WalEstablishment::Occupied => self.effect(
                BootstrapState::ObservingWalTail {
                    loaded,
                    occupied: Some(fence.batch()),
                },
                BootstrapEffect::ObserveWalTail,
            ),
            WalEstablishment::UnresolvedAbsent => self.exit(BootstrapExit::Retryable {
                detail: format!(
                    "takeover FENCE create at {:?} is unresolved and absent on reconciliation",
                    fence.batch()
                ),
            }),
        }
    }

    fn handle_replay_read(
        &mut self,
        mut replay: Replay,
        observed: Option<WalObject>,
    ) -> BootstrapStep {
        let Some(observed) = observed else {
            return self.refresh_anomaly(
                replay.loaded.claimed.claim,
                format!("WAL coordinate {:?} is absent below the FENCE", replay.next),
            );
        };
        assert_eq!(
            replay.next,
            observed.batch(),
            "a replay completion retains its exact issued batch"
        );
        assert_eq!(
            replay.loaded.claimed.seal.partition(),
            observed.partition(),
            "a replay completion retains its exact issued partition"
        );
        if replay.next == replay.fence.batch() {
            assert!(
                matches!(observed.body(), WalBody::Fence),
                "the established takeover coordinate reads back as a FENCE"
            );
            assert_eq!(
                replay.loaded.claimed.claim.owner(),
                observed.owner(),
                "the established takeover FENCE retains the authored owner"
            );
        }
        let anomaly = match observed.body() {
            WalBody::Fence => {
                if replay
                    .owner
                    .is_some_and(|current| observed.owner() <= current)
                {
                    Some(format!(
                        "FENCE at {:?} does not strictly increase the replay owner",
                        replay.next
                    ))
                } else {
                    replay.owner = Some(observed.owner());
                    None
                }
            }
            WalBody::Run(facts) => {
                if replay.owner == Some(observed.owner()) {
                    fold_replay_facts(&mut replay.loaded.durable, replay.next, facts.as_slice())
                } else {
                    Some(format!(
                        "RUN at {:?} does not match the replay owner",
                        replay.next
                    ))
                }
            }
        };
        if let Some(detail) = anomaly {
            return self.refresh_anomaly(replay.loaded.claimed.claim, detail);
        }
        if replay.next == replay.fence.batch() {
            assert_eq!(
                Some(replay.loaded.claimed.claim.owner()),
                replay.owner,
                "replay ends under the authored takeover owner"
            );
            let recovery = WriterRecovery {
                claim: replay.loaded.claimed.claim,
                seal: replay.loaded.claimed.seal,
                base: replay.loaded.base,
                durable: replay.loaded.durable,
                durable_batch: replay.fence.batch(),
            };
            return self.effect(
                BootstrapState::RefreshingFinal { recovery },
                BootstrapEffect::ObserveHead,
            );
        }
        replay.next = match replay.next.successor() {
            Ok(next) => next,
            Err(_exhausted) => {
                return self.exit(BootstrapExit::Contradiction {
                    detail: "WAL replay coordinate is exhausted before the FENCE".into(),
                });
            }
        };
        let partition = replay.loaded.claimed.seal.partition();
        let batch = replay.next;
        self.effect(
            BootstrapState::Replaying(replay),
            BootstrapEffect::ReadWal { partition, batch },
        )
    }

    fn handle_store_failure(
        &mut self,
        state: BootstrapState,
        error: TypedStoreError,
    ) -> BootstrapStep {
        match (state, error) {
            (
                BootstrapState::ReadingWalTail { loaded, .. },
                TypedStoreError::Contradiction { detail },
            ) => self.refresh_anomaly(loaded.claimed.claim, detail),
            (BootstrapState::Replaying(replay), TypedStoreError::Contradiction { detail }) => {
                self.refresh_anomaly(replay.loaded.claimed.claim, detail)
            }
            (state @ (BootstrapState::Initial | BootstrapState::Complete), _error) => {
                unexpected(&state, "StoreFailed")
            }
            (_state, error) => self.exit(store_exit(error)),
        }
    }

    fn refresh_anomaly(&mut self, claim: AuthoredClaim, detail: String) -> BootstrapStep {
        self.effect(
            BootstrapState::RefreshingAnomaly { claim, detail },
            BootstrapEffect::ObserveHead,
        )
    }

    fn effect(&mut self, state: BootstrapState, effect: BootstrapEffect) -> BootstrapStep {
        self.state = Some(state);
        BootstrapStep::Effect(effect)
    }

    fn complete(&mut self, recovery: WriterRecovery) -> BootstrapStep {
        self.state = Some(BootstrapState::Complete);
        BootstrapStep::Complete(recovery)
    }

    fn exit(&mut self, exit: BootstrapExit) -> BootstrapStep {
        self.state = Some(BootstrapState::Complete);
        BootstrapStep::Exit(exit)
    }
}

impl Default for BootstrapMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[expect(
    clippy::panic,
    reason = "an event/state mismatch is an interpreter protocol violation"
)]
fn unexpected(state: &BootstrapState, event: &str) -> ! {
    panic!("bootstrap state {state:?} cannot receive {event}")
}

fn plan_bootstrap_sources(seal: &Seal) -> Result<VecDeque<PlannedTable>, BootstrapExit> {
    let mut objects = 0usize;
    let mut bytes = 0u64;
    for table in seal.tables() {
        objects = objects.checked_add(1).ok_or_else(bootstrap_source_bound)?;
        bytes = bytes
            .checked_add(table.object_bytes().get())
            .ok_or_else(bootstrap_source_bound)?;
    }
    if objects > PARTITION_BOOTSTRAP_OBJECTS_MAX_V2 || bytes > PARTITION_BOOTSTRAP_BYTES_MAX_V2 {
        return Err(bootstrap_source_bound());
    }
    let mut sources = VecDeque::with_capacity(objects);
    sources.extend(tree_sources(seal.directory()));
    sources.extend(tree_sources(seal.ledger()));
    Ok(sources)
}

fn bootstrap_source_bound() -> BootstrapExit {
    BootstrapExit::Contradiction {
        detail: "Seal-selected table sources exceed a V2 aggregate bootstrap bound".into(),
    }
}

fn tree_sources(tree: &strom_storage_domain::TreeVersion) -> Vec<PlannedTable> {
    let mut sources = Vec::new();
    for run in tree.runs().iter().rev() {
        for (index, table) in run.tables().iter().enumerate() {
            sources.push(PlannedTable {
                table: *table,
                starts_run: index == 0,
            });
        }
    }
    sources
}

fn merge_directory_table(
    merged: &mut MergedRows,
    previous_last: Option<&StreamPath>,
    rows: Vec<(StreamPath, DirectoryEntry)>,
) -> Result<Option<StreamPath>, BootstrapExit> {
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

fn map_takeover_fence_error(error: TakeoverFenceError) -> BootstrapExit {
    match error {
        TakeoverFenceError::NoRunCoordinate => BootstrapExit::Retryable {
            detail: "WAL coordinate space has no RUN coordinate after the takeover FENCE".into(),
        },
        TakeoverFenceError::NotAfterCut => BootstrapExit::Contradiction {
            detail: "takeover FENCE is not strictly after the Seal replay cut".into(),
        },
        TakeoverFenceError::SpanExceeded { span } => BootstrapExit::Retryable {
            detail: format!(
                "WAL suffix through takeover FENCE spans {span} coordinates; the bound is {WAL_SUFFIX_COORDINATES_MAX_V2}"
            ),
        },
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
    facts: &[strom_storage_domain::OperationFact],
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

fn store_exit(error: TypedStoreError) -> BootstrapExit {
    match error {
        TypedStoreError::Retryable { detail } => BootstrapExit::Retryable { detail },
        TypedStoreError::Rejected { detail } | TypedStoreError::Contradiction { detail } => {
            BootstrapExit::Contradiction { detail }
        }
    }
}
