//! Proptest strategies for valid storage-domain values.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use proptest::prelude::{Just, Strategy, prop_oneof};

use crate::{
    BatchId, DirectoryEntry, DirectoryKey, LedgerCell, OperationFact, OwnerToken, PartitionId,
    Seal, SealGeneration, StreamRecord, StreamUid, TreeVersion, WalBody, WalFacts, WalObject,
    WalReplayPoint,
};

pub fn ledger_rows() -> impl Strategy<Value = Vec<(StreamUid, LedgerCell)>> {
    proptest::collection::vec((stream_uid(), ledger_cell()), 1..=16).prop_map(|rows| {
        rows.into_iter()
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .collect()
    })
}

pub fn directory_rows() -> impl Strategy<Value = Vec<(DirectoryKey, DirectoryEntry)>> {
    proptest::collection::vec((directory_key(), directory_entry()), 1..=16).prop_map(|rows| {
        rows.into_iter()
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .collect()
    })
}

/// # Panics
///
/// Panics if a generated fact vector within `1..=16` violates the published
/// WAL fact-count bound.
pub fn wal_object() -> impl Strategy<Value = WalObject> {
    (
        partition_id(),
        proptest::collection::vec(operation_fact(), 1..=16),
        batch_id(),
        owner_token(),
    )
        .prop_flat_map(|(partition, facts, batch, owner)| {
            let facts = WalFacts::try_from(facts)
                .expect("the generated fact count is inside the WAL bound");
            prop_oneof![
                Just(WalObject::new(partition, batch, owner, WalBody::Run(facts))),
                Just(WalObject::new(partition, batch, owner, WalBody::Fence)),
            ]
        })
}

pub fn seal() -> impl Strategy<Value = Seal> {
    (
        partition_id(),
        proptest::option::of((batch_id(), owner_token())),
        seal_generation(),
    )
        .prop_filter_map(
            "canonical empty trees always form a V2 Seal",
            |(partition, replay, generation)| {
                let replay = match replay {
                    Some((batch, owner)) => WalReplayPoint::Through { batch, owner },
                    None => WalReplayPoint::Genesis,
                };
                Seal::new(
                    partition,
                    generation,
                    replay,
                    TreeVersion::empty(),
                    TreeVersion::empty(),
                )
                .ok()
            },
        )
}

pub fn partition_id() -> impl Strategy<Value = PartitionId> {
    proptest::array::uniform16(proptest::num::u8::ANY)
        .prop_filter_map("nil is reserved", |bytes| PartitionId::try_from(bytes).ok())
}

pub fn operation_fact() -> impl Strategy<Value = OperationFact> {
    prop_oneof![
        (
            directory_key(),
            stream_uid(),
            strom_domain::strategy::stream_content_type(),
            strom_domain::strategy::expiry_policy(),
        )
            .prop_map(
                |(path, uid, content_type, expiry)| OperationFact::StreamCreated {
                    path,
                    uid,
                    content_type,
                    expiry,
                }
            ),
        (directory_key(), stream_uid())
            .prop_map(|(path, uid)| OperationFact::StreamClosed { path, uid }),
        (directory_key(), stream_uid())
            .prop_map(|(path, uid)| OperationFact::StreamDeleted { path, uid }),
    ]
}

pub fn directory_key() -> impl Strategy<Value = DirectoryKey> {
    strom_domain::strategy::stream_id().prop_map(|stream_id| DirectoryKey::from(&stream_id))
}

pub fn directory_entry() -> impl Strategy<Value = DirectoryEntry> {
    prop_oneof![
        stream_uid().prop_map(DirectoryEntry::Live),
        stream_uid().prop_map(DirectoryEntry::Tombstone),
    ]
}

/// Generates only nonzero stream identities.
///
/// # Panics
///
/// The mapping asserts the strategy's lower bound remains one.
pub fn stream_uid() -> impl Strategy<Value = StreamUid> {
    (1u64..).prop_map(|raw| {
        StreamUid::from(NonZeroU64::new(raw).expect("the uid strategy starts at one"))
    })
}

pub fn ledger_cell() -> impl Strategy<Value = LedgerCell> {
    prop_oneof![
        stream_record().prop_map(LedgerCell::Value),
        Just(LedgerCell::Delete)
    ]
}

pub fn stream_record() -> impl Strategy<Value = StreamRecord> {
    (
        strom_domain::strategy::stream_content_type(),
        strom_domain::strategy::expiry_policy(),
        strom_domain::strategy::stream_lifecycle(),
        batch_id(),
    )
        .prop_map(|(content_type, expiry, lifecycle, created_at)| {
            StreamRecord::new(content_type, expiry, lifecycle, created_at)
        })
}

/// # Panics
///
/// Panics if a value drawn from one upwards stops constructing a nonzero value.
pub fn batch_id() -> impl Strategy<Value = BatchId> {
    (1u64..).prop_map(|raw| {
        BatchId::from(NonZeroU64::new(raw).expect("the batch strategy starts at one"))
    })
}

pub fn owner_token() -> impl Strategy<Value = OwnerToken> {
    seal_generation().prop_map(OwnerToken::from)
}

/// # Panics
///
/// Panics if a value drawn from one upwards stops constructing a nonzero value.
pub fn seal_generation() -> impl Strategy<Value = SealGeneration> {
    (1u64..).prop_map(|raw| {
        SealGeneration::from(NonZeroU64::new(raw).expect("the generation strategy starts at one"))
    })
}
