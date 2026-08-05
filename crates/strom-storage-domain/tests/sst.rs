#![expect(
    clippy::big_endian_bytes,
    reason = "malformed SST tests manipulate the RFC's big-endian wire fields"
)]

use strom_domain::{ExpiryPolicy, StreamId, StreamLifecycle};
use strom_storage_domain::{
    AttemptId, BatchId, DirectoryEntry, DirectoryKey, FreshIdentity, LedgerCell, PartitionId,
    RowField, SST_HEADER_BYTES, STREAM_RECORD_BYTES_MAX, SealGeneration, SstDecodeError, StoreKind,
    StreamRecord, StreamUid, TableKey, TableObjectId, decode_directory_sst, decode_ledger_sst,
    encode_directory_sst, encode_ledger_sst,
};

const DIRECTORY_ONE_ROW: &[u8] = &[
    83, 84, 82, 77, 3, 1, 0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255,
    0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 1, 1, 0, 0,
    0, 8, 1, 0, 0, 0, 0, 0, 0, 0, 7, 101, 118, 101, 110, 116, 115, 47, 97,
];

const DIRECTORY_PREFIX_ROWS: &[u8] = &[
    83, 84, 82, 77, 3, 1, 0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255,
    0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 1, 1, 0, 0,
    0, 8, 1, 0, 0, 0, 0, 0, 0, 0, 7, 101, 118, 101, 110, 116, 115, 47, 97, 0, 8, 0, 1, 2, 0, 0, 0,
    0, 0, 0, 0, 8, 98, 0, 7, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 9, 98,
];

const LEDGER_VALUE_DELETE: &[u8] = &[
    83, 84, 82, 77, 3, 1, 0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255,
    0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 2, 2, 0, 0,
    0, 0, 0, 0, 0, 7, 1, 0, 35, 31, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 106,
    115, 111, 110, 59, 32, 99, 104, 97, 114, 115, 101, 116, 61, 117, 116, 102, 45, 56, 0, 1, 9, 0,
    0, 0, 0, 0, 0, 0, 8, 2,
];

fn partition() -> Result<PartitionId, strom_storage_domain::PartitionIdError> {
    "00112233-4455-6677-8899-aabbccddeeff".parse()
}

fn table_key(store: StoreKind) -> Result<TableKey, Box<dyn std::error::Error>> {
    let fresh = FreshIdentity::new(
        SealGeneration::try_from(2)?,
        AttemptId::new(SealGeneration::genesis(), 4),
        0,
    )?;
    Ok(TableKey::new(
        partition()?,
        TableObjectId::new(fresh, store),
    ))
}

fn key(raw: &str) -> Result<DirectoryKey, strom_domain::StreamIdError> {
    raw.parse::<StreamId>()
        .map(|stream_id| DirectoryKey::from(&stream_id))
}

fn uid(raw: u64) -> Result<StreamUid, strom_storage_domain::ZeroCoordinate> {
    StreamUid::try_from(raw)
}

fn record() -> Result<StreamRecord, Box<dyn std::error::Error>> {
    Ok(StreamRecord::new(
        "application/json; charset=utf-8".parse()?,
        ExpiryPolicy::None,
        StreamLifecycle::Closed,
        BatchId::try_from(9)?,
    ))
}

fn directory_rows() -> Result<Vec<(DirectoryKey, DirectoryEntry)>, Box<dyn std::error::Error>> {
    Ok(vec![
        (key("events/a")?, DirectoryEntry::Live(uid(7)?)),
        (key("events/ab")?, DirectoryEntry::Tombstone(uid(8)?)),
        (key("events/b")?, DirectoryEntry::Live(uid(9)?)),
    ])
}

fn ledger_rows() -> Result<Vec<(StreamUid, LedgerCell)>, Box<dyn std::error::Error>> {
    Ok(vec![
        (uid(7)?, LedgerCell::Value(record()?)),
        (uid(8)?, LedgerCell::Delete),
    ])
}

#[test]
fn complete_sst_golden_vectors_anchor_headers_and_rows() -> Result<(), Box<dyn std::error::Error>> {
    let one_row = vec![(key("events/a")?, DirectoryEntry::Live(uid(7)?))];
    let directory_key = table_key(StoreKind::Directory)?;
    assert_eq!(
        encode_directory_sst(&directory_key, &one_row)?,
        DIRECTORY_ONE_ROW,
        "the complete one-row Directory SST is a durable format anchor"
    );
    assert_eq!(
        decode_directory_sst(&directory_key, DIRECTORY_ONE_ROW)?,
        one_row,
        "the independent one-row Directory bytes decode"
    );

    let prefix_rows = directory_rows()?;
    assert_eq!(
        encode_directory_sst(&directory_key, &prefix_rows)?,
        DIRECTORY_PREFIX_ROWS,
        "the complete prefix-sharing Directory SST is a durable format anchor"
    );
    assert_eq!(
        decode_directory_sst(&directory_key, DIRECTORY_PREFIX_ROWS)?,
        prefix_rows
    );

    let ledger_key = table_key(StoreKind::Ledger)?;
    let ledger_rows = ledger_rows()?;
    assert_eq!(
        encode_ledger_sst(&ledger_key, &ledger_rows)?,
        LEDGER_VALUE_DELETE,
        "the complete Value/Delete Ledger SST is a durable format anchor"
    );
    assert_eq!(
        decode_ledger_sst(&ledger_key, LEDGER_VALUE_DELETE)?,
        ledger_rows
    );
    Ok(())
}

#[test]
fn header_gates_reject_discriminator_and_identity_damage() -> Result<(), Box<dyn std::error::Error>>
{
    let expected = table_key(StoreKind::Directory)?;
    let cases = [
        (
            0,
            b'X',
            SstDecodeError::MagicMismatch {
                observed: [b'X', b'T', b'R', b'M'],
            },
        ),
        (4, 2, SstDecodeError::ObjectKindMismatch { observed: 2 }),
        (5, 0, SstDecodeError::UnsupportedVersion { observed: 0 }),
        (50, 0, SstDecodeError::UnknownStore { observed: 0 }),
        (51, 0, SstDecodeError::UnknownRowCodec { observed: 0 }),
    ];
    for (offset, value, error) in cases {
        let mut damaged = DIRECTORY_ONE_ROW.to_vec();
        *damaged
            .get_mut(offset)
            .ok_or("golden header offset exists")? = value;
        assert_eq!(
            decode_directory_sst(&expected, &damaged),
            Err(error),
            "the damaged header discriminator must fail closed"
        );
    }

    let mut mismatched_codec = DIRECTORY_ONE_ROW.to_vec();
    *mismatched_codec.get_mut(51).ok_or("row codec exists")? = 2;
    assert_eq!(
        decode_directory_sst(&expected, &mismatched_codec),
        Err(SstDecodeError::StoreCodecMismatch)
    );

    let mut wrong_identity = DIRECTORY_ONE_ROW.to_vec();
    *wrong_identity
        .get_mut(29)
        .ok_or("birth generation byte exists")? = 3;
    assert_eq!(
        decode_directory_sst(&expected, &wrong_identity),
        Err(SstDecodeError::IdentityMismatch)
    );
    let wrong_partition: PartitionId = "10112233-4455-6677-8899-aabbccddeeff".parse()?;
    let wrong_location = TableKey::new(wrong_partition, expected.object());
    assert_eq!(
        decode_directory_sst(&wrong_location, DIRECTORY_ONE_ROW),
        Err(SstDecodeError::IdentityMismatch)
    );
    assert_eq!(
        decode_directory_sst(
            &expected,
            DIRECTORY_ONE_ROW
                .get(..SST_HEADER_BYTES)
                .ok_or("golden header exists")?,
        ),
        Err(SstDecodeError::EmptyTable)
    );
    Ok(())
}

#[test]
fn directory_decoder_rejects_eof_inside_every_row_field() -> Result<(), Box<dyn std::error::Error>>
{
    let expected = table_key(StoreKind::Directory)?;
    let cases = [
        (1, RowField::DirectoryShared),
        (3, RowField::DirectorySuffixLength),
        (4, RowField::DirectoryEntryTag),
        (8, RowField::DirectoryUid),
        (14, RowField::DirectorySuffix),
    ];
    for (row_bytes, field) in cases {
        let end = SST_HEADER_BYTES
            .checked_add(row_bytes)
            .ok_or("test offset fits")?;
        assert_eq!(
            decode_directory_sst(
                &expected,
                DIRECTORY_ONE_ROW.get(..end).ok_or("golden prefix exists")?,
            ),
            Err(SstDecodeError::UnexpectedEof { field }),
            "EOF inside {field:?} must not expose a row prefix"
        );
    }
    Ok(())
}

#[test]
fn directory_decoder_enforces_prefix_tag_uid_and_order_gates()
-> Result<(), Box<dyn std::error::Error>> {
    let expected = table_key(StoreKind::Directory)?;

    let mut past_previous = DIRECTORY_ONE_ROW.to_vec();
    *past_previous
        .get_mut(SST_HEADER_BYTES + 1)
        .ok_or("shared byte exists")? = 1;
    assert_eq!(
        decode_directory_sst(&expected, &past_previous),
        Err(SstDecodeError::PrefixPastPreviousKey)
    );

    let mut unknown_tag = DIRECTORY_ONE_ROW.to_vec();
    *unknown_tag
        .get_mut(SST_HEADER_BYTES + 4)
        .ok_or("tag exists")? = 3;
    assert_eq!(
        decode_directory_sst(&expected, &unknown_tag),
        Err(SstDecodeError::UnknownDirectoryTag { observed: 3 })
    );

    let mut zero_uid = DIRECTORY_ONE_ROW.to_vec();
    *zero_uid
        .get_mut(SST_HEADER_BYTES + 12)
        .ok_or("uid byte exists")? = 0;
    assert_eq!(
        decode_directory_sst(&expected, &zero_uid),
        Err(SstDecodeError::ZeroUid)
    );

    let mut malformed_path = DIRECTORY_ONE_ROW.to_vec();
    *malformed_path.last_mut().ok_or("path suffix exists")? = b'/';
    assert_eq!(
        decode_directory_sst(&expected, &malformed_path),
        Err(SstDecodeError::InvalidDirectoryKey)
    );

    let mut non_longest = DIRECTORY_PREFIX_ROWS.to_vec();
    let second = SST_HEADER_BYTES + 21;
    *non_longest
        .get_mut(second + 1)
        .ok_or("shared byte exists")? = 7;
    *non_longest
        .get_mut(second + 3)
        .ok_or("suffix length exists")? = 2;
    non_longest.insert(second + 13, b'a');
    assert_eq!(
        decode_directory_sst(&expected, &non_longest),
        Err(SstDecodeError::NonLongestPrefix)
    );

    let mut duplicate = DIRECTORY_PREFIX_ROWS.to_vec();
    *duplicate
        .get_mut(second + 3)
        .ok_or("suffix length exists")? = 0;
    duplicate.remove(second + 13);
    assert_eq!(
        decode_directory_sst(&expected, &duplicate),
        Err(SstDecodeError::RowsNotStrictlyOrdered)
    );

    let mut unsorted = DIRECTORY_PREFIX_ROWS.to_vec();
    let third = second + 14;
    *unsorted
        .get_mut(third + 1)
        .ok_or("third shared byte exists")? = 8;
    *unsorted.last_mut().ok_or("third suffix exists")? = b'a';
    assert_eq!(
        decode_directory_sst(&expected, &unsorted),
        Err(SstDecodeError::RowsNotStrictlyOrdered)
    );
    Ok(())
}

#[test]
fn ledger_decoder_rejects_eof_order_tags_and_bad_values() -> Result<(), Box<dyn std::error::Error>>
{
    let expected = table_key(StoreKind::Ledger)?;
    let cases = [
        (4, RowField::LedgerUid),
        (8, RowField::LedgerCellTag),
        (10, RowField::LedgerValueLength),
        (12, RowField::LedgerValue),
    ];
    for (row_bytes, field) in cases {
        let end = SST_HEADER_BYTES
            .checked_add(row_bytes)
            .ok_or("test offset fits")?;
        assert_eq!(
            decode_ledger_sst(
                &expected,
                LEDGER_VALUE_DELETE
                    .get(..end)
                    .ok_or("golden prefix exists")?,
            ),
            Err(SstDecodeError::UnexpectedEof { field })
        );
    }

    let mut zero_uid = LEDGER_VALUE_DELETE.to_vec();
    *zero_uid
        .get_mut(SST_HEADER_BYTES + 7)
        .ok_or("uid byte exists")? = 0;
    assert_eq!(
        decode_ledger_sst(&expected, &zero_uid),
        Err(SstDecodeError::ZeroUid)
    );

    let mut unknown_tag = LEDGER_VALUE_DELETE.to_vec();
    *unknown_tag
        .get_mut(SST_HEADER_BYTES + 8)
        .ok_or("tag exists")? = 3;
    assert_eq!(
        decode_ledger_sst(&expected, &unknown_tag),
        Err(SstDecodeError::UnknownLedgerTag { observed: 3 })
    );

    let mut zero_length = LEDGER_VALUE_DELETE.to_vec();
    *zero_length
        .get_mut(SST_HEADER_BYTES + 10)
        .ok_or("length byte exists")? = 0;
    assert_eq!(
        decode_ledger_sst(&expected, &zero_length),
        Err(SstDecodeError::LedgerValueLength)
    );

    let oversized = u16::try_from(STREAM_RECORD_BYTES_MAX + 1)?;
    let mut oversized_value = LEDGER_VALUE_DELETE.to_vec();
    let length = oversized.to_be_bytes();
    *oversized_value
        .get_mut(SST_HEADER_BYTES + 9)
        .ok_or("length byte exists")? = length[0];
    *oversized_value
        .get_mut(SST_HEADER_BYTES + 10)
        .ok_or("length byte exists")? = length[1];
    assert_eq!(
        decode_ledger_sst(&expected, &oversized_value),
        Err(SstDecodeError::LedgerValueLength)
    );

    let mut bad_value = LEDGER_VALUE_DELETE.to_vec();
    *bad_value
        .get_mut(SST_HEADER_BYTES + 11)
        .ok_or("value byte exists")? = 255;
    assert!(matches!(
        decode_ledger_sst(&expected, &bad_value),
        Err(SstDecodeError::StreamRecord(_))
    ));

    let mut trailing_value = LEDGER_VALUE_DELETE.to_vec();
    *trailing_value
        .get_mut(SST_HEADER_BYTES + 10)
        .ok_or("value length exists")? = 36;
    trailing_value.insert(SST_HEADER_BYTES + 11 + 35, 0);
    assert!(matches!(
        decode_ledger_sst(&expected, &trailing_value),
        Err(SstDecodeError::StreamRecord(
            strom_storage_domain::DecodeError::TrailingBytes { bytes_actual: 1 }
        ))
    ));

    let mut duplicate = LEDGER_VALUE_DELETE.to_vec();
    let second_uid_last = SST_HEADER_BYTES + 11 + 35 + 7;
    *duplicate
        .get_mut(second_uid_last)
        .ok_or("second UID exists")? = 7;
    assert_eq!(
        decode_ledger_sst(&expected, &duplicate),
        Err(SstDecodeError::RowsNotStrictlyOrdered)
    );

    let mut unsorted = LEDGER_VALUE_DELETE.to_vec();
    *unsorted
        .get_mut(second_uid_last)
        .ok_or("second UID exists")? = 6;
    assert_eq!(
        decode_ledger_sst(&expected, &unsorted),
        Err(SstDecodeError::RowsNotStrictlyOrdered)
    );
    Ok(())
}
