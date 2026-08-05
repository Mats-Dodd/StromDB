//! Contract suite for the raw adapter seam (stromstyle §7: adapter contracts
//! at an external boundary). Every claim here must also hold against a real
//! S3 endpoint.

use strom_object_store::{
    ByteBound, CreateEvidence, FrozenBytes, KeysBound, ListPageRequest, ObjectKey,
    ObjectStoreAdapter, StoreContradiction, StoreError,
};

fn page_request(
    prefix: &str,
    start_exclusive: Option<&ObjectKey>,
    keys_max: usize,
) -> ListPageRequest {
    ListPageRequest {
        prefix: key(prefix),
        start_exclusive: start_exclusive.cloned(),
        keys_max: KeysBound::try_from(keys_max).expect("test bounds are legal"),
    }
}

fn key(raw: &str) -> ObjectKey {
    raw.parse().expect("test keys are canonical")
}

fn body(raw: &[u8]) -> FrozenBytes {
    FrozenBytes::try_from(raw.to_vec()).expect("test bodies are non-empty and bounded")
}

const READ_BYTES_MAX: u64 = 1024;

fn read_bound() -> ByteBound {
    ByteBound::try_from(READ_BYTES_MAX).expect("nonzero bound parses")
}

#[tokio::test]
async fn first_create_wins_directly_and_read_returns_the_exact_bytes() {
    let adapter = ObjectStoreAdapter::in_memory();
    let coordinate = key("partition/p1/seal/v1/000042");
    let candidate = body(b"seal-candidate-bytes");

    let evidence = adapter
        .create_if_absent(&coordinate, candidate.clone())
        .await
        .expect("create runs");
    assert_eq!(
        CreateEvidence::Direct,
        evidence,
        "an unoccupied coordinate grants Direct"
    );

    let observed = adapter
        .read(&coordinate, read_bound())
        .await
        .expect("read runs")
        .expect("created object is readable");
    assert_eq!(
        observed.body(),
        candidate.as_slice(),
        "read-after-create returns the exact bytes"
    );
}

#[tokio::test]
async fn occupied_coordinate_yields_durable_match_for_equal_bytes_and_not_ours_for_different() {
    let adapter = ObjectStoreAdapter::in_memory();
    let coordinate = key("partition/p1/wal/v1/000007");
    let candidate = body(b"run-bytes");

    adapter
        .create_if_absent(&coordinate, candidate.clone())
        .await
        .expect("first create runs");

    let same = adapter
        .create_if_absent(&coordinate, candidate)
        .await
        .expect("second create runs");
    assert_eq!(
        CreateEvidence::DurableMatch,
        same,
        "identical bytes prove existence, never authorship"
    );

    let different = adapter
        .create_if_absent(&coordinate, body(b"other-bytes"))
        .await
        .expect("third create runs");
    assert_eq!(
        CreateEvidence::NotOurs,
        different,
        "a different occupant fences the caller"
    );
}

#[tokio::test]
async fn absent_objects_read_as_none() {
    let adapter = ObjectStoreAdapter::in_memory();
    let observed = adapter
        .read(&key("partition/p1/seal/v1/absent"), read_bound())
        .await
        .expect("read runs");
    assert!(observed.is_none(), "absence is Ok(None), not an error");
}

#[tokio::test]
async fn oversized_object_is_a_contradiction_not_a_download() {
    let adapter = ObjectStoreAdapter::in_memory();
    let coordinate = key("partition/p1/pack/v1/big");
    adapter
        .create_if_absent(&coordinate, body(&[7u8; 64]))
        .await
        .expect("create runs");

    let bound = ByteBound::try_from(63).expect("nonzero bound parses");
    let outcome = adapter.read(&coordinate, bound).await;
    assert!(
        matches!(
            outcome,
            Err(StoreError::Contradiction(
                StoreContradiction::OversizedObject {
                    bytes_actual: 64,
                    ..
                }
            ))
        ),
        "a body above the caller's bound fails closed, got {outcome:?}"
    );
}

#[tokio::test]
async fn list_pages_are_ordered_bounded_and_resume_exactly_from_the_continuation() {
    let adapter = ObjectStoreAdapter::in_memory();
    let ordinals = ["001", "002", "003", "004", "005"];
    for ordinal in ordinals {
        let coordinate = key(&format!("partition/p1/wal/v1/{ordinal}"));
        adapter
            .create_if_absent(&coordinate, body(b"x"))
            .await
            .expect("create runs");
    }
    // A neighbouring namespace must not leak into the page.
    adapter
        .create_if_absent(&key("partition/p1/seal/v1/001"), body(b"x"))
        .await
        .expect("create runs");

    let first = adapter
        .list_page(page_request("partition/p1/wal/v1", None, 2))
        .await
        .expect("list runs");
    let first_keys: Vec<&str> = first.keys().iter().map(ObjectKey::as_str).collect();
    assert_eq!(
        ["partition/p1/wal/v1/001", "partition/p1/wal/v1/002"],
        *first_keys,
        "the first page is the two smallest keys under the prefix"
    );
    let continuation = first.continuation().expect("more keys exist").clone();
    assert_eq!(
        "partition/p1/wal/v1/002",
        continuation.as_str(),
        "continuation is the last surfaced key"
    );

    let second = adapter
        .list_page(page_request(
            "partition/p1/wal/v1",
            Some(&continuation),
            1000,
        ))
        .await
        .expect("list runs");
    let second_keys: Vec<&str> = second.keys().iter().map(ObjectKey::as_str).collect();
    assert_eq!(
        [
            "partition/p1/wal/v1/003",
            "partition/p1/wal/v1/004",
            "partition/p1/wal/v1/005"
        ],
        *second_keys,
        "the continuation is exclusive and the rest follows in order"
    );
    assert!(
        second.continuation().is_none(),
        "an exhausted listing carries no continuation"
    );
}

#[tokio::test]
async fn newest_head_probe_returns_the_smallest_key_under_reverse_ordinals() {
    let adapter = ObjectStoreAdapter::in_memory();
    // Reverse fixed-width ordinals: a greater generation encodes to a smaller key.
    for reverse_ordinal in ["18446744073709551613", "18446744073709551614"] {
        let coordinate = key(&format!("partition/p1/seal/v1/{reverse_ordinal}"));
        adapter
            .create_if_absent(&coordinate, body(b"seal"))
            .await
            .expect("create runs");
    }

    let head = adapter
        .list_page(page_request("partition/p1/seal/v1", None, 1))
        .await
        .expect("list runs");
    let head_keys: Vec<&str> = head.keys().iter().map(ObjectKey::as_str).collect();
    assert_eq!(
        ["partition/p1/seal/v1/18446744073709551613"],
        *head_keys,
        "MaxKeys=1 must surface the greatest generation"
    );
}

#[tokio::test]
async fn delete_is_idempotent_for_present_and_absent_objects() {
    let adapter = ObjectStoreAdapter::in_memory();
    let coordinate = key("partition/p1/pack/v1/doomed");
    adapter
        .create_if_absent(&coordinate, body(b"pack"))
        .await
        .expect("create runs");

    adapter
        .delete_idempotent(&coordinate)
        .await
        .expect("delete of a present object runs");
    assert!(
        adapter
            .read(&coordinate, read_bound())
            .await
            .expect("read runs")
            .is_none(),
        "a deleted object is absent"
    );

    adapter
        .delete_idempotent(&coordinate)
        .await
        .expect("delete of an absent object also succeeds");
}
