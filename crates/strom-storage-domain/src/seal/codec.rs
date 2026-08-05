//! Seal V2 envelope codec.

use std::fmt;
use std::marker::PhantomData;

use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{
    AttemptId, FreshIdentity, KeyBound, RangeVersion, Seal, SealFormat, SealGeneration,
    SealIdentity, SortedRun, StoreKind, TableObjectId, TableRef, TreeVersion, WalReplayPoint,
};
use crate::bounds::{
    DIRECTORY_KEY_BYTES_MAX, RUN_TABLES_MAX, SEAL_ENCODED_BYTES_MAX, TREE_RANGES_MAX_V2,
    TREE_RUNS_MAX,
};
use crate::envelope::{DecodeError, EncodeError, ObjectKind, decode_frame, encode_frame};
use crate::{OwnerToken, PartitionId};

const VERSION: u8 = 2;

/// # Errors
///
/// Returns [`EncodeError`] when serialization fails or the complete frame
/// exceeds [`SEAL_ENCODED_BYTES_MAX`].
pub fn encode_seal(seal: &Seal) -> Result<Vec<u8>, EncodeError> {
    encode_frame(
        ObjectKind::Seal,
        VERSION,
        &SealWire::from(seal),
        SEAL_ENCODED_BYTES_MAX,
    )
}

/// # Errors
///
/// Returns [`DecodeError`] at the first failed envelope, body, manifest, or
/// identity gate.
pub fn decode_seal(identity: &SealIdentity, bytes: &[u8]) -> Result<Seal, DecodeError> {
    let body = decode_frame(ObjectKind::Seal, VERSION, bytes, SEAL_ENCODED_BYTES_MAX)?;
    let (wire, trailing) = postcard::take_from_bytes::<SealWire>(body)
        .map_err(|_detail| DecodeError::MalformedBody)?;
    let seal = Seal::try_from(wire)?;
    if !trailing.is_empty() {
        return Err(DecodeError::TrailingBytes {
            bytes_actual: trailing.len(),
        });
    }
    if seal.identity() != *identity {
        return Err(DecodeError::IdentityMismatch);
    }
    Ok(seal)
}

#[derive(Debug, Serialize, Deserialize)]
struct SealWire {
    partition: [u8; 16],
    generation: u64,
    replay: WalReplayPointWire,
    format: u8,
    directory: TreeVersionWire,
    ledger: TreeVersionWire,
    tally: TreeVersionWire,
    annals: TreeVersionWire,
}

impl From<&Seal> for SealWire {
    fn from(seal: &Seal) -> Self {
        Self {
            partition: *seal.identity().partition().as_bytes(),
            generation: seal.identity().generation().get(),
            replay: WalReplayPointWire::from(seal.replay()),
            format: 2,
            directory: TreeVersionWire::from(seal.directory()),
            ledger: TreeVersionWire::from(seal.ledger()),
            tally: TreeVersionWire::from(seal.tally()),
            annals: TreeVersionWire::from(seal.annals()),
        }
    }
}

impl TryFrom<SealWire> for Seal {
    type Error = DecodeError;

    fn try_from(wire: SealWire) -> Result<Self, Self::Error> {
        let partition =
            PartitionId::try_from(wire.partition).map_err(|_detail| DecodeError::InvalidBody)?;
        let generation = SealGeneration::try_from(wire.generation)
            .map_err(|_detail| DecodeError::InvalidBody)?;
        if wire.format != 2 {
            return Err(DecodeError::FormatMismatch);
        }
        Seal::new(
            partition,
            generation,
            WalReplayPoint::try_from(wire.replay)?,
            SealFormat::V2,
            TreeVersion::try_from(wire.directory)?,
            TreeVersion::try_from(wire.ledger)?,
            TreeVersion::try_from(wire.tally)?,
            TreeVersion::try_from(wire.annals)?,
        )
        .map_err(|_detail| DecodeError::FormatMismatch)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum WalReplayPointWire {
    Genesis,
    Through { batch: u64, owner: u64 },
}

impl From<WalReplayPoint> for WalReplayPointWire {
    fn from(replay: WalReplayPoint) -> Self {
        match replay {
            WalReplayPoint::Genesis => Self::Genesis,
            WalReplayPoint::Through { batch, owner } => Self::Through {
                batch: batch.get(),
                owner: owner.encoded(),
            },
        }
    }
}

impl TryFrom<WalReplayPointWire> for WalReplayPoint {
    type Error = DecodeError;

    fn try_from(wire: WalReplayPointWire) -> Result<Self, Self::Error> {
        match wire {
            WalReplayPointWire::Genesis => Ok(Self::Genesis),
            WalReplayPointWire::Through { batch, owner } => Ok(Self::Through {
                batch: crate::BatchId::try_from(batch)
                    .map_err(|_detail| DecodeError::InvalidBody)?,
                owner: OwnerToken::from(
                    SealGeneration::try_from(owner).map_err(|_detail| DecodeError::InvalidBody)?,
                ),
            }),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TreeVersionWire {
    ranges: BoundedVecWire<RangeVersionWire, TREE_RANGES_MAX_V2>,
}

impl From<&TreeVersion> for TreeVersionWire {
    fn from(tree: &TreeVersion) -> Self {
        Self {
            ranges: BoundedVecWire(tree.ranges().iter().map(RangeVersionWire::from).collect()),
        }
    }
}

impl TryFrom<TreeVersionWire> for TreeVersion {
    type Error = DecodeError;

    fn try_from(wire: TreeVersionWire) -> Result<Self, Self::Error> {
        let ranges = wire
            .ranges
            .0
            .into_iter()
            .map(RangeVersion::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_from_ranges(ranges).map_err(|_detail| DecodeError::InvalidBody)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RangeVersionWire {
    start: KeyBoundWire,
    end: KeyBoundWire,
    runs: BoundedVecWire<SortedRunWire, TREE_RUNS_MAX>,
}

impl From<&RangeVersion> for RangeVersionWire {
    fn from(range: &RangeVersion) -> Self {
        Self {
            start: KeyBoundWire::from(range.start()),
            end: KeyBoundWire::from(range.end()),
            runs: BoundedVecWire(range.runs().iter().map(SortedRunWire::from).collect()),
        }
    }
}

impl TryFrom<RangeVersionWire> for RangeVersion {
    type Error = DecodeError;

    fn try_from(wire: RangeVersionWire) -> Result<Self, Self::Error> {
        let runs = wire
            .runs
            .0
            .into_iter()
            .map(SortedRun::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(
            KeyBound::try_from(wire.start)?,
            KeyBound::try_from(wire.end)?,
            runs,
        )
        .map_err(|_detail| DecodeError::InvalidBody)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum KeyBoundWire {
    Reserved,
    Minimum,
    Key(BoundedVecWire<u8, DIRECTORY_KEY_BYTES_MAX>),
    Maximum,
}

impl From<&KeyBound> for KeyBoundWire {
    fn from(bound: &KeyBound) -> Self {
        match bound {
            KeyBound::Minimum => Self::Minimum,
            KeyBound::Key(key) => Self::Key(BoundedVecWire(key.to_vec())),
            KeyBound::Maximum => Self::Maximum,
        }
    }
}

impl TryFrom<KeyBoundWire> for KeyBound {
    type Error = DecodeError;

    fn try_from(wire: KeyBoundWire) -> Result<Self, Self::Error> {
        match wire {
            KeyBoundWire::Reserved => Err(DecodeError::InvalidBody),
            KeyBoundWire::Minimum => Ok(Self::Minimum),
            KeyBoundWire::Key(key) => Ok(Self::Key(key.0.into_boxed_slice())),
            KeyBoundWire::Maximum => Ok(Self::Maximum),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SortedRunWire {
    tables: BoundedVecWire<TableRefWire, RUN_TABLES_MAX>,
}

impl From<&SortedRun> for SortedRunWire {
    fn from(run: &SortedRun) -> Self {
        Self {
            tables: BoundedVecWire(run.tables().iter().map(TableRefWire::from).collect()),
        }
    }
}

impl TryFrom<SortedRunWire> for SortedRun {
    type Error = DecodeError;

    fn try_from(wire: SortedRunWire) -> Result<Self, Self::Error> {
        let tables = wire
            .tables
            .0
            .into_iter()
            .map(TableRef::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_from_tables(tables).map_err(|_detail| DecodeError::InvalidBody)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TableRefWire {
    object: TableObjectIdWire,
    object_bytes: u64,
}

impl From<&TableRef> for TableRefWire {
    fn from(table: &TableRef) -> Self {
        Self {
            object: TableObjectIdWire::from(table.object()),
            object_bytes: table.object_bytes().get(),
        }
    }
}

impl TryFrom<TableRefWire> for TableRef {
    type Error = DecodeError;

    fn try_from(wire: TableRefWire) -> Result<Self, Self::Error> {
        let bytes = std::num::NonZeroU64::new(wire.object_bytes).ok_or(DecodeError::InvalidBody)?;
        Self::new(TableObjectId::try_from(wire.object)?, bytes)
            .map_err(|_detail| DecodeError::InvalidBody)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TableObjectIdWire {
    fresh: FreshIdentityWire,
    store: StoreKindWire,
}

impl From<TableObjectId> for TableObjectIdWire {
    fn from(object: TableObjectId) -> Self {
        Self {
            fresh: FreshIdentityWire::from(object.fresh()),
            store: StoreKindWire::from(object.store()),
        }
    }
}

impl TryFrom<TableObjectIdWire> for TableObjectId {
    type Error = DecodeError;

    fn try_from(wire: TableObjectIdWire) -> Result<Self, Self::Error> {
        Ok(Self::new(
            FreshIdentity::try_from(wire.fresh)?,
            StoreKind::try_from(wire.store)?,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct FreshIdentityWire {
    birth_generation: u64,
    attempt: AttemptIdWire,
    ordinal: u32,
}

impl From<FreshIdentity> for FreshIdentityWire {
    fn from(fresh: FreshIdentity) -> Self {
        Self {
            birth_generation: fresh.birth_generation().get(),
            attempt: AttemptIdWire::from(fresh.attempt()),
            ordinal: fresh.ordinal(),
        }
    }
}

impl TryFrom<FreshIdentityWire> for FreshIdentity {
    type Error = DecodeError;

    fn try_from(wire: FreshIdentityWire) -> Result<Self, Self::Error> {
        Self::new(
            SealGeneration::try_from(wire.birth_generation)
                .map_err(|_detail| DecodeError::InvalidBody)?,
            AttemptId::try_from(wire.attempt)?,
            wire.ordinal,
        )
        .map_err(|_detail| DecodeError::InvalidBody)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct AttemptIdWire {
    owner_claim: u64,
    local_counter: u64,
}

impl From<AttemptId> for AttemptIdWire {
    fn from(attempt: AttemptId) -> Self {
        Self {
            owner_claim: attempt.owner_claim().get(),
            local_counter: attempt.local_counter(),
        }
    }
}

impl TryFrom<AttemptIdWire> for AttemptId {
    type Error = DecodeError;

    fn try_from(wire: AttemptIdWire) -> Result<Self, Self::Error> {
        Ok(Self::new(
            SealGeneration::try_from(wire.owner_claim)
                .map_err(|_detail| DecodeError::InvalidBody)?,
            wire.local_counter,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum StoreKindWire {
    Reserved,
    Directory,
    Ledger,
    Tally,
    Annals,
}

impl From<StoreKind> for StoreKindWire {
    fn from(store: StoreKind) -> Self {
        match store {
            StoreKind::Directory => Self::Directory,
            StoreKind::Ledger => Self::Ledger,
            StoreKind::Tally => Self::Tally,
            StoreKind::Annals => Self::Annals,
        }
    }
}

impl TryFrom<StoreKindWire> for StoreKind {
    type Error = DecodeError;

    fn try_from(wire: StoreKindWire) -> Result<Self, Self::Error> {
        match wire {
            StoreKindWire::Reserved => Err(DecodeError::InvalidBody),
            StoreKindWire::Directory => Ok(Self::Directory),
            StoreKindWire::Ledger => Ok(Self::Ledger),
            StoreKindWire::Tally => Ok(Self::Tally),
            StoreKindWire::Annals => Ok(Self::Annals),
        }
    }
}

#[derive(Debug)]
struct BoundedVecWire<Value, const MAX: usize>(Vec<Value>);

impl<Value: Serialize, const MAX: usize> Serialize for BoundedVecWire<Value, MAX> {
    fn serialize<SerializerType: Serializer>(
        &self,
        serializer: SerializerType,
    ) -> Result<SerializerType::Ok, SerializerType::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de, Value: Deserialize<'de>, const MAX: usize> Deserialize<'de>
    for BoundedVecWire<Value, MAX>
{
    fn deserialize<DeserializerType: Deserializer<'de>>(
        deserializer: DeserializerType,
    ) -> Result<Self, DeserializerType::Error> {
        deserializer.deserialize_seq(BoundedVecVisitor::<Value, MAX>(PhantomData))
    }
}

struct BoundedVecVisitor<Value, const MAX: usize>(PhantomData<Value>);

impl<'de, Value: Deserialize<'de>, const MAX: usize> Visitor<'de>
    for BoundedVecVisitor<Value, MAX>
{
    type Value = BoundedVecWire<Value, MAX>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "at most {MAX} elements")
    }

    fn visit_seq<Sequence: SeqAccess<'de>>(
        self,
        mut seq: Sequence,
    ) -> Result<Self::Value, Sequence::Error> {
        if seq.size_hint().is_some_and(|declared| declared > MAX) {
            return Err(serde::de::Error::invalid_length(
                seq.size_hint().unwrap_or(MAX),
                &self,
            ));
        }
        let mut values = Vec::new();
        while values.len() < MAX {
            let Some(value) = seq.next_element()? else {
                return Ok(BoundedVecWire(values));
            };
            values.push(value);
        }
        if seq.next_element::<IgnoredAny>()?.is_some() {
            return Err(serde::de::Error::invalid_length(
                MAX.saturating_add(1),
                &self,
            ));
        }
        Ok(BoundedVecWire(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_decoder_rejects_another_format_and_non_empty_deferred_store()
    -> Result<(), Box<dyn std::error::Error>> {
        let partition: PartitionId = "00112233-4455-6677-8899-aabbccddeeff".parse()?;
        let identity = SealIdentity::new(partition, SealGeneration::try_from(2)?);

        let wrong_format = seal_wire(partition, 1, empty_tree_wire());
        assert_eq!(
            decode_seal(&identity, &encode_wire(&wrong_format)?),
            Err(DecodeError::FormatMismatch),
            "Seal envelope V2 accepts only SealFormat::V2"
        );

        let non_empty_tally = seal_wire(partition, 2, one_table_tree_wire(StoreKindWire::Tally));
        assert_eq!(
            decode_seal(&identity, &encode_wire(&non_empty_tally)?),
            Err(DecodeError::FormatMismatch),
            "Tally remains canonically empty in SealFormat::V2"
        );
        Ok(())
    }

    #[test]
    fn manifest_sequence_bound_rejects_max_plus_one_ranges()
    -> Result<(), Box<dyn std::error::Error>> {
        let partition: PartitionId = "00112233-4455-6677-8899-aabbccddeeff".parse()?;
        let identity = SealIdentity::new(partition, SealGeneration::try_from(2)?);
        let over_bound = TreeVersionWire {
            ranges: BoundedVecWire(vec![
                full_range_wire(Vec::new()),
                full_range_wire(Vec::new()),
            ]),
        };
        let wire = SealWire {
            partition: *partition.as_bytes(),
            generation: 2,
            replay: WalReplayPointWire::Genesis,
            format: 2,
            directory: over_bound,
            ledger: empty_tree_wire(),
            tally: empty_tree_wire(),
            annals: empty_tree_wire(),
        };

        assert_eq!(
            decode_seal(&identity, &encode_wire(&wire)?),
            Err(DecodeError::MalformedBody),
            "the bounded visitor rejects range MAX + 1 while parsing"
        );
        Ok(())
    }

    fn seal_wire(partition: PartitionId, format: u8, tally: TreeVersionWire) -> SealWire {
        SealWire {
            partition: *partition.as_bytes(),
            generation: 2,
            replay: WalReplayPointWire::Genesis,
            format,
            directory: empty_tree_wire(),
            ledger: empty_tree_wire(),
            tally,
            annals: empty_tree_wire(),
        }
    }

    fn empty_tree_wire() -> TreeVersionWire {
        TreeVersionWire {
            ranges: BoundedVecWire(vec![full_range_wire(Vec::new())]),
        }
    }

    fn one_table_tree_wire(store: StoreKindWire) -> TreeVersionWire {
        let table = TableRefWire {
            object: TableObjectIdWire {
                fresh: FreshIdentityWire {
                    birth_generation: 2,
                    attempt: AttemptIdWire {
                        owner_claim: 1,
                        local_counter: 0,
                    },
                    ordinal: 0,
                },
                store,
            },
            object_bytes: 1,
        };
        TreeVersionWire {
            ranges: BoundedVecWire(vec![full_range_wire(vec![SortedRunWire {
                tables: BoundedVecWire(vec![table]),
            }])]),
        }
    }

    fn full_range_wire(runs: Vec<SortedRunWire>) -> RangeVersionWire {
        RangeVersionWire {
            start: KeyBoundWire::Minimum,
            end: KeyBoundWire::Maximum,
            runs: BoundedVecWire(runs),
        }
    }

    fn encode_wire(wire: &SealWire) -> Result<Vec<u8>, EncodeError> {
        encode_frame(ObjectKind::Seal, VERSION, wire, SEAL_ENCODED_BYTES_MAX)
    }
}
