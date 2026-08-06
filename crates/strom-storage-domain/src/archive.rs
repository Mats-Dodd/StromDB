//! Shared rkyv boundary for durable storage objects.

use rkyv::api::high::{HighSerializer, to_bytes_in_with_alloc};
use rkyv::rancor::{Failure, Fallible, Source};
use rkyv::ser::allocator::{Arena, ArenaHandle};
use rkyv::ser::{Positional, Writer};
use rkyv::string::{ArchivedString, StringResolver};
use rkyv::with::{ArchiveWith, SerializeWith};
use rkyv::{Archive, Archived, Place, Resolver, Serialize, SerializeUnsized};
use strom_domain::{ExpiresAt, ExpiryPolicy, StreamContentType, StreamLifecycle, StreamTtl};

/// A durable object could not be encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EncodeError {
    #[error("domain value could not be archived")]
    Serialization,
    #[error("encoded value exceeds the {bytes_max}-byte bound")]
    EncodedBytesOverMax { bytes_max: usize },
}

/// A durable object could not be decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("encoded value is {bytes_actual} bytes; the bound is {bytes_max}")]
    EncodedBytesOverMax {
        bytes_max: usize,
        bytes_actual: usize,
    },
    #[error("archive is structurally malformed")]
    MalformedArchive,
    #[error("archive violates a domain invariant")]
    InvalidBody,
    #[error("body identity differs from the durable location")]
    IdentityMismatch,
}

pub(crate) fn encode<Value>(value: &Value, bytes_max: usize) -> Result<Vec<u8>, EncodeError>
where
    Value: for<'arena, 'writer> Serialize<
        HighSerializer<&'writer mut BoundedWriter, ArenaHandle<'arena>, Failure>,
    >,
{
    let mut writer = BoundedWriter::new(bytes_max);
    let mut arena = Arena::new();
    if to_bytes_in_with_alloc::<_, _, Failure>(value, &mut writer, arena.acquire()).is_err() {
        return if writer.over_bound {
            Err(EncodeError::EncodedBytesOverMax { bytes_max })
        } else {
            Err(EncodeError::Serialization)
        };
    }
    Ok(writer.bytes)
}

pub(crate) struct BoundedWriter {
    bytes: Vec<u8>,
    bytes_max: usize,
    over_bound: bool,
}

impl BoundedWriter {
    const fn new(bytes_max: usize) -> Self {
        Self {
            bytes: Vec::new(),
            bytes_max,
            over_bound: false,
        }
    }
}

impl Positional for BoundedWriter {
    fn pos(&self) -> usize {
        self.bytes.len()
    }
}

impl Writer<Failure> for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> Result<(), Failure> {
        let bytes_actual = self.bytes.len().saturating_add(bytes.len());
        if bytes_actual > self.bytes_max {
            self.over_bound = true;
            return Err(Failure);
        }
        if bytes.len() > self.bytes.capacity().saturating_sub(self.bytes.len())
            && self.bytes.try_reserve_exact(bytes.len()).is_err()
        {
            return Err(Failure);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

pub(crate) const fn decode_bound(bytes: &[u8], bytes_max: usize) -> Result<(), DecodeError> {
    if bytes.len() > bytes_max {
        return Err(DecodeError::EncodedBytesOverMax {
            bytes_max,
            bytes_actual: bytes.len(),
        });
    }
    Ok(())
}

pub(crate) fn decode_content_type(
    content_type: &ArchivedString,
) -> Result<StreamContentType, DecodeError> {
    let archived = content_type.as_str();
    StreamContentType::validate_canonical(archived)
        .map_err(|_domain_error| DecodeError::InvalidBody)?;
    let parsed: StreamContentType = archived
        .parse()
        .map_err(|_domain_error| DecodeError::InvalidBody)?;
    Ok(parsed)
}

pub(crate) struct ContentTypeAsString;

impl ArchiveWith<StreamContentType> for ContentTypeAsString {
    type Archived = ArchivedString;
    type Resolver = StringResolver;

    fn resolve_with(
        field: &StreamContentType,
        resolver: Self::Resolver,
        out: Place<Self::Archived>,
    ) {
        ArchivedString::resolve_from_str(field.as_str(), resolver, out);
    }
}

impl<SerializerType> SerializeWith<StreamContentType, SerializerType> for ContentTypeAsString
where
    SerializerType: Fallible + ?Sized,
    SerializerType::Error: Source,
    str: SerializeUnsized<SerializerType>,
{
    fn serialize_with(
        field: &StreamContentType,
        serializer: &mut SerializerType,
    ) -> Result<Self::Resolver, SerializerType::Error> {
        ArchivedString::serialize_from_str(field.as_str(), serializer)
    }
}

#[derive(Debug, Archive, Serialize)]
pub(crate) enum ExpiryArchive {
    None,
    SlidingTtl(u64),
    AbsoluteExpiry(i128),
}

impl From<ExpiryPolicy> for ExpiryArchive {
    fn from(expiry: ExpiryPolicy) -> Self {
        match expiry {
            ExpiryPolicy::None => Self::None,
            ExpiryPolicy::SlidingTtl(ttl) => Self::SlidingTtl(ttl.seconds().get()),
            ExpiryPolicy::AbsoluteExpiry(expires_at) => {
                Self::AbsoluteExpiry(i128::from(expires_at))
            }
        }
    }
}

pub(crate) struct ExpiryAsArchive;

impl ArchiveWith<ExpiryPolicy> for ExpiryAsArchive {
    type Archived = Archived<ExpiryArchive>;
    type Resolver = Resolver<ExpiryArchive>;

    fn resolve_with(field: &ExpiryPolicy, resolver: Self::Resolver, out: Place<Self::Archived>) {
        ExpiryArchive::from(*field).resolve(resolver, out);
    }
}

impl<SerializerType> SerializeWith<ExpiryPolicy, SerializerType> for ExpiryAsArchive
where
    SerializerType: Fallible + ?Sized,
    ExpiryArchive: Serialize<SerializerType>,
{
    fn serialize_with(
        field: &ExpiryPolicy,
        serializer: &mut SerializerType,
    ) -> Result<Self::Resolver, SerializerType::Error> {
        ExpiryArchive::from(*field).serialize(serializer)
    }
}

impl TryFrom<&ArchivedExpiryArchive> for ExpiryPolicy {
    type Error = DecodeError;

    fn try_from(expiry: &ArchivedExpiryArchive) -> Result<Self, Self::Error> {
        match expiry {
            ArchivedExpiryArchive::None => Ok(Self::None),
            ArchivedExpiryArchive::SlidingTtl(seconds) => {
                std::num::NonZeroU64::new(seconds.to_native())
                    .map(StreamTtl::from)
                    .map(Self::SlidingTtl)
                    .ok_or(DecodeError::InvalidBody)
            }
            ArchivedExpiryArchive::AbsoluteExpiry(unix_nanoseconds) => {
                ExpiresAt::try_from(unix_nanoseconds.to_native())
                    .map(Self::AbsoluteExpiry)
                    .map_err(|_domain_error| DecodeError::InvalidBody)
            }
        }
    }
}

#[derive(Debug, Archive, Serialize)]
pub(crate) enum LifecycleArchive {
    Open,
    Closed,
}

pub(crate) struct LifecycleAsArchive;

impl ArchiveWith<StreamLifecycle> for LifecycleAsArchive {
    type Archived = Archived<LifecycleArchive>;
    type Resolver = Resolver<LifecycleArchive>;

    fn resolve_with(field: &StreamLifecycle, resolver: Self::Resolver, out: Place<Self::Archived>) {
        LifecycleArchive::from(*field).resolve(resolver, out);
    }
}

impl<SerializerType> SerializeWith<StreamLifecycle, SerializerType> for LifecycleAsArchive
where
    SerializerType: Fallible + ?Sized,
    LifecycleArchive: Serialize<SerializerType>,
{
    fn serialize_with(
        field: &StreamLifecycle,
        serializer: &mut SerializerType,
    ) -> Result<Self::Resolver, SerializerType::Error> {
        LifecycleArchive::from(*field).serialize(serializer)
    }
}

impl From<StreamLifecycle> for LifecycleArchive {
    fn from(lifecycle: StreamLifecycle) -> Self {
        match lifecycle {
            StreamLifecycle::Open => Self::Open,
            StreamLifecycle::Closed => Self::Closed,
        }
    }
}

impl From<&ArchivedLifecycleArchive> for StreamLifecycle {
    fn from(lifecycle: &ArchivedLifecycleArchive) -> Self {
        match lifecycle {
            ArchivedLifecycleArchive::Open => Self::Open,
            ArchivedLifecycleArchive::Closed => Self::Closed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Archive, Serialize)]
    struct ProtocolFields {
        #[rkyv(with = ContentTypeAsString)]
        content_type: StreamContentType,
        #[rkyv(with = ExpiryAsArchive)]
        expiry: ExpiryPolicy,
        #[rkyv(with = LifecycleAsArchive)]
        lifecycle: StreamLifecycle,
    }

    #[test]
    fn protocol_adapters_round_trip_through_canonical_domain_construction() {
        let fields = ProtocolFields {
            content_type: "text/plain; charset=utf-8"
                .parse()
                .expect("the fixture content type is canonical"),
            expiry: ExpiryPolicy::AbsoluteExpiry(
                ExpiresAt::try_from(1_725_000_000_123_456_789i128)
                    .expect("the fixture expiry is representable"),
            ),
            lifecycle: StreamLifecycle::Closed,
        };

        let bytes = encode(&fields, 1_024).expect("the small fixture fits its archive bound");
        let archived = rkyv::access::<ArchivedProtocolFields, Failure>(&bytes)
            .expect("the encoder produces a checked archive");

        assert_eq!(
            decode_content_type(&archived.content_type),
            Ok(fields.content_type),
            "content type is reconstructed through its canonical parser"
        );
        assert_eq!(
            ExpiryPolicy::try_from(&archived.expiry),
            Ok(fields.expiry),
            "expiry is reconstructed through its domain conversions"
        );
        assert_eq!(
            StreamLifecycle::from(&archived.lifecycle),
            fields.lifecycle,
            "lifecycle variants map exhaustively"
        );
    }

    #[test]
    fn complete_archive_bound_is_shared_by_encode_and_decode() {
        let fields = ProtocolFields {
            content_type: StreamContentType::octet_stream(),
            expiry: ExpiryPolicy::None,
            lifecycle: StreamLifecycle::Open,
        };
        let bytes = encode(&fields, 1_024).expect("the small fixture fits its archive bound");
        let first_crossing = bytes.len().saturating_sub(1);

        assert_eq!(
            encode(&fields, first_crossing),
            Err(EncodeError::EncodedBytesOverMax {
                bytes_max: first_crossing,
            }),
            "encode checks the complete archive length"
        );
        assert_eq!(
            decode_bound(&bytes, first_crossing),
            Err(DecodeError::EncodedBytesOverMax {
                bytes_max: first_crossing,
                bytes_actual: bytes.len(),
            }),
            "decode rejects the same complete archive boundary"
        );
    }

    #[test]
    fn expiry_adapter_rejects_a_structurally_valid_zero_ttl() {
        let invalid = ExpiryArchive::SlidingTtl(0);
        let bytes = encode(&invalid, 1_024).expect("the invalid domain fixture still archives");
        let archived = rkyv::access::<ArchivedExpiryArchive, Failure>(&bytes)
            .expect("zero is structurally valid as an archived u64");

        assert_eq!(
            ExpiryPolicy::try_from(archived),
            Err(DecodeError::InvalidBody),
            "checked archive access does not replace protocol-domain validation"
        );
    }
}
