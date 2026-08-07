use strom_domain::{ExpiryPolicy, StreamLifecycle, StreamPath, StreamTtl};
use strom_storage_domain::{
    AttemptId, BatchId, DirectoryEntry, FreshIdentity, LedgerCell, PartitionId, SealGeneration,
    SstDecodeError, SstEncodeError, StoreKind, StreamRecord, StreamUid, TableKey, TableObjectId,
    decode_directory_sst, decode_ledger_sst, encode_directory_sst, encode_ledger_sst,
};

const DIRECTORY_ONE_ROW: &[u8] = &[
    101, 118, 101, 110, 116, 115, 47, 97, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 17, 34, 51, 68, 85, 102,
    119, 136, 153, 170, 187, 204, 221, 238, 255, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 195, 255, 255, 255, 1, 0, 0, 0,
];

const LEDGER_ONE_DELETE: &[u8] = &[
    7, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221,
    238, 255, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    169, 255, 255, 255, 1, 0, 0, 0,
];

#[test]
fn directory_and_ledger_roots_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let partition = partition()?;
    let directory_key = table_key(StoreKind::Directory)?;
    let directory_rows = directory_rows()?;
    let directory = encode_directory_sst(partition, &directory_key, &directory_rows)?;
    assert_eq!(
        directory_rows,
        decode_directory_sst(partition, &directory_key, &directory)?
    );

    let ledger_key = table_key(StoreKind::Ledger)?;
    let ledger_rows = ledger_rows()?;
    let ledger = encode_ledger_sst(partition, &ledger_key, &ledger_rows)?;
    assert_eq!(
        ledger_rows,
        decode_ledger_sst(partition, &ledger_key, &ledger)?
    );
    Ok(())
}

#[test]
fn minimal_fixtures_anchor_each_concrete_archive_root() -> Result<(), Box<dyn std::error::Error>> {
    let partition = partition()?;
    let directory_key = table_key(StoreKind::Directory)?;
    let directory_rows = vec![(key("events/a")?, DirectoryEntry::Live(uid(7)?))];
    assert_eq!(
        DIRECTORY_ONE_ROW,
        encode_directory_sst(partition, &directory_key, &directory_rows)?.as_slice()
    );
    assert_eq!(
        directory_rows,
        decode_directory_sst(partition, &directory_key, DIRECTORY_ONE_ROW)?
    );

    let ledger_key = table_key(StoreKind::Ledger)?;
    let ledger_rows = vec![(uid(7)?, LedgerCell::Delete)];
    assert_eq!(
        LEDGER_ONE_DELETE,
        encode_ledger_sst(partition, &ledger_key, &ledger_rows)?.as_slice()
    );
    assert_eq!(
        ledger_rows,
        decode_ledger_sst(partition, &ledger_key, LEDGER_ONE_DELETE)?
    );
    Ok(())
}

#[test]
fn encoders_reject_empty_unordered_and_wrong_store_inputs() -> Result<(), Box<dyn std::error::Error>>
{
    let partition = partition()?;
    let directory_key = table_key(StoreKind::Directory)?;
    assert_eq!(
        Err(SstEncodeError::EmptyTable),
        encode_directory_sst(partition, &directory_key, &[])
    );
    let mut directory_rows = directory_rows()?;
    directory_rows.swap(0, 1);
    assert_eq!(
        Err(SstEncodeError::RowsNotStrictlyOrdered),
        encode_directory_sst(partition, &directory_key, &directory_rows)
    );
    assert_eq!(
        Err(SstEncodeError::StoreMismatch),
        encode_directory_sst(partition, &table_key(StoreKind::Ledger)?, &directory_rows)
    );
    assert_eq!(
        Err(SstDecodeError::StoreMismatch),
        decode_directory_sst(partition, &table_key(StoreKind::Ledger)?, DIRECTORY_ONE_ROW)
    );

    let ledger_key = table_key(StoreKind::Ledger)?;
    assert_eq!(
        Err(SstEncodeError::EmptyTable),
        encode_ledger_sst(partition, &ledger_key, &[])
    );
    let mut ledger_rows = ledger_rows()?;
    ledger_rows.swap(0, 1);
    assert_eq!(
        Err(SstEncodeError::RowsNotStrictlyOrdered),
        encode_ledger_sst(partition, &ledger_key, &ledger_rows)
    );
    assert_eq!(
        Err(SstEncodeError::StoreMismatch),
        encode_ledger_sst(partition, &table_key(StoreKind::Directory)?, &ledger_rows)
    );
    assert_eq!(
        Err(SstDecodeError::StoreMismatch),
        decode_ledger_sst(
            partition,
            &table_key(StoreKind::Directory)?,
            LEDGER_ONE_DELETE
        )
    );
    Ok(())
}

#[test]
fn decoders_check_structure_before_location_identity() -> Result<(), Box<dyn std::error::Error>> {
    let partition = partition()?;
    let directory_key = table_key(StoreKind::Directory)?;
    let encoded = encode_directory_sst(partition, &directory_key, &directory_rows()?)?;
    let wrong_partition: PartitionId = "10112233-4455-6677-8899-aabbccddeeff".parse()?;
    let wrong_location = TableKey::new(directory_key.object());
    assert_eq!(
        Err(SstDecodeError::IdentityMismatch),
        decode_directory_sst(wrong_partition, &wrong_location, &encoded)
    );
    assert_eq!(
        Err(SstDecodeError::MalformedArchive),
        decode_directory_sst(
            partition,
            &wrong_location,
            encoded
                .get(..encoded.len().saturating_sub(1))
                .ok_or("truncated archive prefix exists")?,
        )
    );

    let ledger_key = table_key(StoreKind::Ledger)?;
    let encoded = encode_ledger_sst(partition, &ledger_key, &ledger_rows()?)?;
    let wrong_location = TableKey::new(ledger_key.object());
    assert_eq!(
        Err(SstDecodeError::IdentityMismatch),
        decode_ledger_sst(wrong_partition, &wrong_location, &encoded)
    );
    assert_eq!(
        Err(SstDecodeError::MalformedArchive),
        decode_ledger_sst(
            partition,
            &wrong_location,
            encoded
                .get(..encoded.len().saturating_sub(1))
                .ok_or("truncated archive prefix exists")?,
        )
    );
    Ok(())
}

#[test]
fn checked_access_accepts_misaligned_object_store_slices() -> Result<(), Box<dyn std::error::Error>>
{
    let partition = partition()?;
    let directory_key = table_key(StoreKind::Directory)?;
    let rows = directory_rows()?;
    let encoded = encode_directory_sst(partition, &directory_key, &rows)?;
    let mut storage = Vec::with_capacity(encoded.len().saturating_add(1));
    storage.push(0);
    storage.extend_from_slice(&encoded);
    assert_eq!(
        rows,
        decode_directory_sst(
            partition,
            &directory_key,
            storage.get(1..).ok_or("misaligned archive exists")?,
        )?
    );
    Ok(())
}

#[test]
fn structurally_valid_noncanonical_values_fail_domain_construction()
-> Result<(), Box<dyn std::error::Error>> {
    let partition = partition()?;
    let directory_key = table_key(StoreKind::Directory)?;
    let encoded = encode_directory_sst(
        partition,
        &directory_key,
        &[(key("events/a")?, DirectoryEntry::Live(uid(7)?))],
    )?;
    let mut damaged = encoded.clone();
    let key_position = damaged
        .windows(b"events/a".len())
        .position(|window| window == b"events/a")
        .ok_or("archived key bytes exist")?;
    let final_byte = key_position
        .checked_add(b"events/a".len().saturating_sub(1))
        .ok_or("key position fits")?;
    *damaged.get_mut(final_byte).ok_or("key byte exists")? = b'/';
    assert_eq!(
        Err(SstDecodeError::InvalidBody),
        decode_directory_sst(partition, &directory_key, &damaged)
    );

    let ledger_key = table_key(StoreKind::Ledger)?;
    let ledger_rows = [(
        uid(7)?,
        LedgerCell::Value(record(
            "application/json",
            ExpiryPolicy::None,
            StreamLifecycle::Open,
            9,
        )?),
    )];
    let mut damaged = encode_ledger_sst(partition, &ledger_key, &ledger_rows)?;
    let separator = damaged
        .windows(b"application/json".len())
        .position(|window| window == b"application/json")
        .and_then(|start| start.checked_add(b"application".len()))
        .ok_or("archived content type exists")?;
    *damaged
        .get_mut(separator)
        .ok_or("content type separator exists")? = b'?';
    assert_eq!(
        Err(SstDecodeError::InvalidBody),
        decode_ledger_sst(partition, &ledger_key, &damaged)
    );
    Ok(())
}

fn partition() -> Result<PartitionId, strom_storage_domain::PartitionIdError> {
    "00112233-4455-6677-8899-aabbccddeeff".parse()
}

fn table_key(store: StoreKind) -> Result<TableKey, Box<dyn std::error::Error>> {
    let fresh = FreshIdentity::new(
        SealGeneration::try_from(2)?,
        AttemptId::new(SealGeneration::genesis(), 4),
        0,
    )?;
    Ok(TableKey::new(TableObjectId::new(fresh, store)))
}

fn directory_rows() -> Result<Vec<(StreamPath, DirectoryEntry)>, Box<dyn std::error::Error>> {
    Ok(vec![
        (key("events/a")?, DirectoryEntry::Live(uid(7)?)),
        (key("events/ab")?, DirectoryEntry::Tombstone(uid(8)?)),
        (key("events/b")?, DirectoryEntry::Live(uid(9)?)),
    ])
}

fn ledger_rows() -> Result<Vec<(StreamUid, LedgerCell)>, Box<dyn std::error::Error>> {
    let ttl = StreamTtl::from(std::num::NonZeroU64::new(3_600).ok_or("TTL is nonzero")?);
    Ok(vec![
        (
            uid(7)?,
            LedgerCell::Value(record(
                "application/json; charset=utf-8",
                ExpiryPolicy::None,
                StreamLifecycle::Closed,
                9,
            )?),
        ),
        (
            uid(8)?,
            LedgerCell::Value(record(
                "text/plain",
                ExpiryPolicy::SlidingTtl(ttl),
                StreamLifecycle::Open,
                10,
            )?),
        ),
        (
            uid(9)?,
            LedgerCell::Value(record(
                "application/octet-stream",
                ExpiryPolicy::AbsoluteExpiry("2030-01-01T00:00:00Z".parse()?),
                StreamLifecycle::Open,
                11,
            )?),
        ),
        (uid(10)?, LedgerCell::Delete),
    ])
}

fn key(raw: &str) -> Result<StreamPath, strom_domain::StreamPathError> {
    raw.parse()
}

fn uid(raw: u64) -> Result<StreamUid, strom_storage_domain::ZeroCoordinate> {
    StreamUid::try_from(raw)
}

fn record(
    content_type: &str,
    expiry: ExpiryPolicy,
    lifecycle: StreamLifecycle,
    created_at: u64,
) -> Result<StreamRecord, Box<dyn std::error::Error>> {
    Ok(StreamRecord::new(
        content_type.parse()?,
        expiry,
        lifecycle,
        BatchId::try_from(created_at)?,
    ))
}
