//! rkyv field adapters for protocol types from `strom-domain`.

use rkyv::rancor::{Fallible, Source};
use rkyv::string::{ArchivedString, StringResolver};
use rkyv::with::{ArchiveWith, SerializeWith};
use rkyv::{Archive, Archived, Place, Resolver, Serialize, SerializeUnsized};
use strom_domain::{ExpiresAt, ExpiryPolicy, StreamContentType, StreamLifecycle, StreamTtl};

use super::DecodeError;

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
    use rkyv::rancor::Failure;
    use rkyv::{Archive, Serialize};
    use strom_domain::{ExpiresAt, ExpiryPolicy, StreamContentType, StreamLifecycle};

    use super::*;
    use crate::archive::encode;

    const FIXTURE_EXPIRY_UNIX_NANOSECONDS: i128 = 1_725_000_000_123_456_789;

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
                ExpiresAt::try_from(FIXTURE_EXPIRY_UNIX_NANOSECONDS)
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
    fn expiry_adapter_rejects_a_structurally_valid_zero_ttl() {
        let invalid = ExpiryArchive::SlidingTtl(0);
        let bytes = encode(&invalid, 1_024).expect("the invalid domain fixture still archives");
        let archived = rkyv::access::<ArchivedExpiryArchive, Failure>(&bytes)
            .expect("zero is structurally valid as an archived u64");

        assert_eq!(
            Err(DecodeError::InvalidBody),
            ExpiryPolicy::try_from(archived),
            "checked archive access does not replace protocol-domain validation"
        );
    }
}
