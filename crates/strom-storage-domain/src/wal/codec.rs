//! WAL envelope codec and bounded fact-sequence parser.

use std::fmt;

use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::{BatchId, BoundedNonEmptyVec, OperationFact, WalFence, WalIdentity, WalObject, WalRun};
use crate::bounds::{WAL_ENCODED_BYTES_MAX, WAL_RUN_FACTS_MAX};
use crate::envelope::{DecodeError, EncodeError, ObjectKind, decode_frame, encode_frame};
use crate::ledger::key::LedgerKeyWire;
use crate::wire::{ExpiryPolicyWire, parse_content_type};
use crate::{LedgerKey, OwnerToken, PartitionId, SealGeneration, StreamUid};

const VERSION: u8 = 1;

/// # Errors
///
/// Returns [`EncodeError`] when serialization fails or the complete frame
/// exceeds [`WAL_ENCODED_BYTES_MAX`].
pub fn encode_wal(object: &WalObject) -> Result<Vec<u8>, EncodeError> {
    encode_frame(ObjectKind::Wal, VERSION, object, WAL_ENCODED_BYTES_MAX)
}

/// # Errors
///
/// Returns [`DecodeError`] at the first failed envelope, body, or identity
/// gate.
pub fn decode_wal(identity: &WalIdentity, bytes: &[u8]) -> Result<WalObject, DecodeError> {
    let body = decode_frame(ObjectKind::Wal, VERSION, bytes, WAL_ENCODED_BYTES_MAX)?;
    let (wire, trailing) = postcard::take_from_bytes::<WalObjectWire>(body)
        .map_err(|_detail| DecodeError::MalformedBody)?;
    let object = WalObject::try_from(wire)?;
    if !trailing.is_empty() {
        return Err(DecodeError::TrailingBytes {
            bytes_actual: trailing.len(),
        });
    }
    if object.identity() != *identity {
        return Err(DecodeError::IdentityMismatch);
    }
    Ok(object)
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
enum WalObjectWire {
    Run(WalRunWire),
    Fence(WalFenceWire),
}

impl TryFrom<WalObjectWire> for WalObject {
    type Error = DecodeError;

    fn try_from(wire: WalObjectWire) -> Result<Self, Self::Error> {
        match wire {
            WalObjectWire::Run(run) => WalRun::try_from(run).map(Self::Run),
            WalObjectWire::Fence(fence) => WalFence::try_from(fence).map(Self::Fence),
        }
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
struct WalRunWire {
    partition: [u8; 16],
    batch: u64,
    owner: u64,
    facts: BoundedFactsWire,
}

impl TryFrom<WalRunWire> for WalRun {
    type Error = DecodeError;

    fn try_from(wire: WalRunWire) -> Result<Self, Self::Error> {
        let partition = parse_partition(wire.partition)?;
        let batch = parse_batch(wire.batch)?;
        let owner = parse_owner(wire.owner)?;
        let facts = wire
            .facts
            .0
            .into_iter()
            .map(OperationFact::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let facts =
            BoundedNonEmptyVec::try_from(facts).map_err(|_detail| DecodeError::InvalidBody)?;
        Ok(Self::new(partition, batch, owner, facts))
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
struct WalFenceWire {
    partition: [u8; 16],
    batch: u64,
    owner: u64,
}

impl TryFrom<WalFenceWire> for WalFence {
    type Error = DecodeError;

    fn try_from(wire: WalFenceWire) -> Result<Self, Self::Error> {
        Ok(Self::new(
            parse_partition(wire.partition)?,
            parse_batch(wire.batch)?,
            parse_owner(wire.owner)?,
        ))
    }
}

#[derive(Debug)]
struct BoundedFactsWire(Vec<OperationFactWire>);

impl<'de> Deserialize<'de> for BoundedFactsWire {
    fn deserialize<DeserializerType: Deserializer<'de>>(
        deserializer: DeserializerType,
    ) -> Result<Self, DeserializerType::Error> {
        deserializer.deserialize_seq(BoundedFactsVisitor)
    }
}

#[cfg(test)]
impl serde::Serialize for BoundedFactsWire {
    fn serialize<Serializer: serde::Serializer>(
        &self,
        serializer: Serializer,
    ) -> Result<Serializer::Ok, Serializer::Error> {
        serde::Serialize::serialize(&self.0, serializer)
    }
}

struct BoundedFactsVisitor;

impl<'de> Visitor<'de> for BoundedFactsVisitor {
    type Value = BoundedFactsWire;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "between one and {WAL_RUN_FACTS_MAX} operation facts"
        )
    }

    fn visit_seq<Sequence: SeqAccess<'de>>(
        self,
        mut seq: Sequence,
    ) -> Result<Self::Value, Sequence::Error> {
        if seq
            .size_hint()
            .is_some_and(|facts_declared| facts_declared > WAL_RUN_FACTS_MAX)
        {
            return Err(serde::de::Error::invalid_length(
                seq.size_hint().unwrap_or(WAL_RUN_FACTS_MAX),
                &self,
            ));
        }
        let mut facts = Vec::new();
        while facts.len() < WAL_RUN_FACTS_MAX {
            let Some(fact) = seq.next_element()? else {
                return Ok(BoundedFactsWire(facts));
            };
            facts.push(fact);
        }
        if seq.next_element::<IgnoredAny>()?.is_some() {
            return Err(serde::de::Error::invalid_length(
                WAL_RUN_FACTS_MAX.saturating_add(1),
                &self,
            ));
        }
        Ok(BoundedFactsWire(facts))
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
enum OperationFactWire {
    Created {
        path: Vec<u8>,
        uid: u64,
        content_type: String,
        expiry: ExpiryPolicyWire,
    },
    Closed {
        path: Vec<u8>,
        uid: u64,
    },
    Deleted {
        path: Vec<u8>,
        uid: u64,
    },
}

impl TryFrom<OperationFactWire> for OperationFact {
    type Error = DecodeError;

    fn try_from(wire: OperationFactWire) -> Result<Self, Self::Error> {
        match wire {
            OperationFactWire::Created {
                path,
                uid,
                content_type,
                expiry,
            } => Ok(Self::StreamCreated {
                path: LedgerKey::try_from(LedgerKeyWire::from(path))?,
                uid: parse_uid(uid)?,
                content_type: parse_content_type(&content_type)?,
                expiry: strom_domain::ExpiryPolicy::try_from(expiry)?,
            }),
            OperationFactWire::Closed { path, uid } => Ok(Self::StreamClosed {
                path: LedgerKey::try_from(LedgerKeyWire::from(path))?,
                uid: parse_uid(uid)?,
            }),
            OperationFactWire::Deleted { path, uid } => Ok(Self::StreamDeleted {
                path: LedgerKey::try_from(LedgerKeyWire::from(path))?,
                uid: parse_uid(uid)?,
            }),
        }
    }
}

fn parse_partition(raw: [u8; 16]) -> Result<PartitionId, DecodeError> {
    PartitionId::try_from(raw).map_err(|_detail| DecodeError::InvalidBody)
}

fn parse_batch(raw: u64) -> Result<BatchId, DecodeError> {
    BatchId::try_from(raw).map_err(|_detail| DecodeError::InvalidBody)
}

fn parse_owner(raw: u64) -> Result<OwnerToken, DecodeError> {
    SealGeneration::try_from(raw)
        .map(OwnerToken::from)
        .map_err(|_detail| DecodeError::InvalidBody)
}

fn parse_uid(raw: u64) -> Result<StreamUid, DecodeError> {
    StreamUid::try_from(raw).map_err(|_detail| DecodeError::InvalidBody)
}

#[cfg(test)]
mod tests {
    use serde::de::DeserializeSeed;

    use super::*;

    #[test]
    fn bounded_fact_wire_crosses_both_count_boundaries() -> Result<(), Box<dyn std::error::Error>> {
        let partition = partition()?;
        let identity = WalIdentity::new(partition, BatchId::try_from(1)?);

        let empty = run_wire(*partition.as_bytes(), 1, 1, Vec::new());
        assert_eq!(
            decode_wal(&identity, &encode_wire(&empty)?),
            Err(DecodeError::InvalidBody),
            "a decoded run must be non-empty"
        );

        let at_max = run_wire(
            *partition.as_bytes(),
            1,
            1,
            deleted_facts(WAL_RUN_FACTS_MAX),
        );
        let decoded = decode_wal(&identity, &encode_wire(&at_max)?)?;
        let WalObject::Run(run) = decoded else {
            return Err("the encoded RUN wire must decode as a RUN".into());
        };
        assert_eq!(
            run.facts().len(),
            WAL_RUN_FACTS_MAX,
            "the exact fact-count bound is accepted"
        );

        let over_max = run_wire(
            *partition.as_bytes(),
            1,
            1,
            deleted_facts(WAL_RUN_FACTS_MAX.saturating_add(1)),
        );
        assert_eq!(
            decode_wal(&identity, &encode_wire(&over_max)?),
            Err(DecodeError::MalformedBody),
            "an over-bound declared sequence length is rejected during postcard parsing"
        );
        Ok(())
    }

    #[test]
    fn over_bound_length_hint_is_rejected_before_requesting_an_element() {
        let mut sequence = HintOnlySequence {
            reads: 0,
            hint: WAL_RUN_FACTS_MAX.saturating_add(1),
        };
        let result = BoundedFactsVisitor.visit_seq(&mut sequence);
        assert!(
            result.is_err(),
            "a declared count above the bound must fail"
        );
        assert_eq!(
            sequence.reads, 0,
            "the visitor must not materialize any fact after an over-bound declaration"
        );
    }

    #[test]
    fn accepted_length_hint_never_sizes_the_fact_allocation() {
        let mut sequence = HintOnlySequence {
            reads: 0,
            hint: WAL_RUN_FACTS_MAX,
        };
        let facts = BoundedFactsVisitor
            .visit_seq(&mut sequence)
            .expect("an empty sequence with an in-bound hint is structurally parseable");
        assert_eq!(
            facts.0.capacity(),
            0,
            "a declared postcard length never allocates storage for facts not materialized"
        );
    }

    #[test]
    fn every_wal_wire_invariant_reenters_its_domain_constructor()
    -> Result<(), Box<dyn std::error::Error>> {
        let partition = partition()?;
        let identity = WalIdentity::new(partition, BatchId::try_from(1)?);
        let invalid_wires = [
            run_wire([0; 16], 1, 1, deleted_facts(1)),
            run_wire(*partition.as_bytes(), 0, 1, deleted_facts(1)),
            run_wire(*partition.as_bytes(), 1, 0, deleted_facts(1)),
            run_wire(
                *partition.as_bytes(),
                1,
                1,
                vec![OperationFactWire::Deleted {
                    path: b"events/abc".to_vec(),
                    uid: 0,
                }],
            ),
            run_wire(
                *partition.as_bytes(),
                1,
                1,
                vec![OperationFactWire::Deleted {
                    path: b"events//abc".to_vec(),
                    uid: 1,
                }],
            ),
            run_wire(
                *partition.as_bytes(),
                1,
                1,
                vec![OperationFactWire::Created {
                    path: b"events/abc".to_vec(),
                    uid: 1,
                    content_type: String::from("not-a-media-type"),
                    expiry: ExpiryPolicyWire::None,
                }],
            ),
            run_wire(
                *partition.as_bytes(),
                1,
                1,
                vec![OperationFactWire::Created {
                    path: b"events/abc".to_vec(),
                    uid: 1,
                    content_type: String::from("application/octet-stream"),
                    expiry: ExpiryPolicyWire::SlidingTtl(0),
                }],
            ),
        ];
        for wire in invalid_wires {
            assert_eq!(
                decode_wal(&identity, &encode_wire(&wire)?),
                Err(DecodeError::InvalidBody),
                "foreign WAL fields must not bypass their canonical domain parser"
            );
        }
        Ok(())
    }

    fn partition() -> Result<PartitionId, crate::PartitionIdError> {
        "00112233-4455-6677-8899-aabbccddeeff".parse()
    }

    fn run_wire(
        partition: [u8; 16],
        batch: u64,
        owner: u64,
        facts: Vec<OperationFactWire>,
    ) -> WalObjectWire {
        WalObjectWire::Run(WalRunWire {
            partition,
            batch,
            owner,
            facts: BoundedFactsWire(facts),
        })
    }

    fn deleted_facts(count: usize) -> Vec<OperationFactWire> {
        (0..count)
            .map(|_ordinal| OperationFactWire::Deleted {
                path: b"events/abc".to_vec(),
                uid: 1,
            })
            .collect()
    }

    fn encode_wire(wire: &WalObjectWire) -> Result<Vec<u8>, EncodeError> {
        encode_frame(ObjectKind::Wal, VERSION, wire, WAL_ENCODED_BYTES_MAX)
    }

    struct HintOnlySequence {
        reads: usize,
        hint: usize,
    }

    impl<'de> SeqAccess<'de> for &mut HintOnlySequence {
        type Error = serde::de::value::Error;

        fn next_element_seed<Seed: DeserializeSeed<'de>>(
            &mut self,
            _seed: Seed,
        ) -> Result<Option<Seed::Value>, Self::Error> {
            self.reads = self.reads.saturating_add(1);
            Ok(None)
        }

        fn size_hint(&self) -> Option<usize> {
            Some(self.hint)
        }
    }
}
