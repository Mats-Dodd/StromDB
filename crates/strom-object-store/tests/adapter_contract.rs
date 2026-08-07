//! Contract suite for the raw adapter seam (stromstyle §7: adapter contracts
//! at an external boundary). Every claim here must also hold against a real
//! S3 endpoint.

#[cfg(feature = "test-support")]
use strom_object_store::test_support::{
    BackendFailure, Fault, FaultStore, Operation, Selection, Target,
};
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

#[cfg(feature = "test-support")]
#[tokio::test]
async fn failure_before_create_is_ambiguous_without_making_bytes_durable() {
    let coordinate = key("partition/p1/wal/v1/fail-before");
    let store = FaultStore::new()
        .inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(coordinate.clone())),
            failure: BackendFailure::Transport,
        })
        .expect("fault selection is unique");
    let adapter = ObjectStoreAdapter::new(store.backend());

    assert_eq!(
        CreateEvidence::Unresolved,
        adapter
            .create_if_absent(&coordinate, body(b"candidate"))
            .await
            .expect("transport loss is evidence, not a definitive error")
    );
    assert!(
        adapter
            .read(&coordinate, read_bound())
            .await
            .expect("the subsequent read passes through")
            .is_none(),
        "failure before storage leaves the coordinate absent"
    );
    store
        .assert_called_once(Operation::Create, &coordinate)
        .expect("the adapter sends one authority-bearing create");
    store.verify().expect("the configured fault ran");
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn response_loss_after_create_is_ambiguous_with_exact_bytes_durable() {
    let coordinate = key("partition/p1/wal/v1/lost-response");
    let candidate = body(b"candidate");
    let store = FaultStore::new()
        .inject(Fault::CreateThenLoseResponse {
            target: Target::Key(coordinate.clone()),
        })
        .expect("fault selection is unique");
    let adapter = ObjectStoreAdapter::new(store.backend());

    assert_eq!(
        CreateEvidence::Unresolved,
        adapter
            .create_if_absent(&coordinate, candidate.clone())
            .await
            .expect("lost response is evidence")
    );
    assert_eq!(
        candidate.as_slice(),
        adapter
            .read(&coordinate, read_bound())
            .await
            .expect("reconciliation read succeeds")
            .expect("the create took effect")
            .body(),
        "the exact candidate became durable before response loss"
    );
    store
        .assert_called_once(Operation::Create, &coordinate)
        .expect("the adapter does not resend an ambiguous create");
    store.verify().expect("the configured fault ran");
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn permission_and_authentication_refusals_are_definitive_create_errors() {
    for (case, failure) in [
        ("permission", BackendFailure::PermissionDenied),
        ("authentication", BackendFailure::Unauthenticated),
    ] {
        let coordinate = key(&format!("partition/p1/wal/v1/{case}"));
        let store = FaultStore::new()
            .inject(Fault::FailBefore {
                selection: Selection::create(Target::Key(coordinate.clone())),
                failure,
            })
            .expect("fault selection is unique");
        let adapter = ObjectStoreAdapter::new(store.backend());

        let outcome = adapter
            .create_if_absent(&coordinate, body(b"candidate"))
            .await;
        assert!(
            matches!(outcome, Err(StoreError::Rejected { .. })),
            "{case} refusal must be definitive, got {outcome:?}"
        );
        store.verify().expect("the configured refusal ran");
    }
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn failed_occupant_metadata_and_body_reads_leave_create_unresolved() {
    let metadata_coordinate = key("partition/p1/wal/v1/occupant-metadata");
    let body_coordinate = key("partition/p1/wal/v1/occupant-body");
    let cases = [
        (
            "metadata",
            metadata_coordinate.clone(),
            Fault::FailBefore {
                selection: Selection::read(Target::Key(metadata_coordinate)),
                failure: BackendFailure::Transport,
            },
        ),
        (
            "body",
            body_coordinate.clone(),
            Fault::FailBody {
                target: Target::Key(body_coordinate),
                failure: BackendFailure::Transport,
            },
        ),
    ];

    for (case, coordinate, fault) in cases {
        let store = FaultStore::new()
            .inject(fault)
            .expect("fault selection is unique");
        let adapter = ObjectStoreAdapter::new(store.backend());
        assert_eq!(
            CreateEvidence::Direct,
            adapter
                .create_if_absent(&coordinate, body(b"occupant"))
                .await
                .expect("test occupant stores")
        );

        assert_eq!(
            CreateEvidence::Unresolved,
            adapter
                .create_if_absent(&coordinate, body(b"foreign!"))
                .await
                .expect("failed reconciliation remains evidence"),
            "a failed occupant {case} read cannot prove ownership"
        );
        store.verify().expect("the reconciliation fault ran");
    }
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn transport_failures_on_read_and_list_are_retryable() {
    let coordinate = key("partition/p1/seal/v1/read-failure");
    let read_store = FaultStore::new()
        .inject(Fault::FailBefore {
            selection: Selection::read(Target::Key(coordinate.clone())),
            failure: BackendFailure::Transport,
        })
        .expect("fault selection is unique");
    let read_adapter = ObjectStoreAdapter::new(read_store.backend());

    let read_outcome = read_adapter.read(&coordinate, read_bound()).await;
    assert!(
        matches!(read_outcome, Err(StoreError::Retryable { .. })),
        "transport read failure is retryable, got {read_outcome:?}"
    );
    read_store.verify().expect("the read fault ran");

    let prefix = key("partition/p1/seal/v1");
    let list_store = FaultStore::new()
        .inject(Fault::FailBefore {
            selection: Selection::list(prefix.clone()),
            failure: BackendFailure::Transport,
        })
        .expect("fault selection is unique");
    let list_adapter = ObjectStoreAdapter::new(list_store.backend());

    let list_outcome = list_adapter
        .list_page(page_request(prefix.as_str(), None, 1))
        .await;
    assert!(
        matches!(list_outcome, Err(StoreError::Retryable { .. })),
        "transport list failure is retryable, got {list_outcome:?}"
    );
    list_store.verify().expect("the list fault ran");
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn bounded_read_rejects_a_body_that_grows_past_underreported_metadata() {
    let coordinate = key("partition/p1/pack/v1/growing-body");
    let store = FaultStore::new()
        .inject(Fault::UnderreportMetadata {
            target: Target::Key(coordinate.clone()),
        })
        .expect("fault selection is unique");
    let adapter = ObjectStoreAdapter::new(store.backend());
    adapter
        .create_if_absent(&coordinate, body(b"123456"))
        .await
        .expect("test body stores");

    let outcome = adapter
        .read(
            &coordinate,
            ByteBound::try_from(5).expect("test bound is nonzero"),
        )
        .await;
    assert!(
        matches!(
            outcome,
            Err(StoreError::Contradiction(
                StoreContradiction::OversizedObject {
                    bytes_max: 5,
                    bytes_actual: 6,
                    ..
                }
            ))
        ),
        "stream growth beyond metadata fails closed, got {outcome:?}"
    );
    store.verify().expect("the metadata fault ran");
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn unordered_backend_listing_is_a_contradiction() {
    let prefix = key("partition/p1/wal/v1");
    let store = FaultStore::new()
        .inject(Fault::ReturnOutOfOrder {
            prefix: prefix.clone(),
        })
        .expect("fault selection is unique");
    let adapter = ObjectStoreAdapter::new(store.backend());
    for suffix in ["001", "002"] {
        adapter
            .create_if_absent(&key(&format!("{prefix}/{suffix}")), body(b"wal"))
            .await
            .expect("test object stores");
    }

    let outcome = adapter
        .list_page(page_request(prefix.as_str(), None, 10))
        .await;
    assert!(
        matches!(
            outcome,
            Err(StoreError::Contradiction(
                StoreContradiction::UnorderedList { .. }
            ))
        ),
        "the adapter must not silently sort a malformed listing, got {outcome:?}"
    );
    store.verify().expect("the ordering fault ran");
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn foreign_backend_list_key_is_a_contradiction() {
    let prefix = key("partition/p1/table/v1");
    let store = FaultStore::new()
        .inject(Fault::ReturnForeignKey {
            prefix: prefix.clone(),
        })
        .expect("fault selection is unique");
    let adapter = ObjectStoreAdapter::new(store.backend());
    adapter
        .create_if_absent(&key(&format!("{prefix}/001")), body(b"table"))
        .await
        .expect("test object stores");

    let outcome = adapter
        .list_page(page_request(prefix.as_str(), None, 10))
        .await;
    assert!(
        matches!(
            outcome,
            Err(StoreError::Contradiction(
                StoreContradiction::ForeignKey { .. }
            ))
        ),
        "a key outside StromDB's canonical vocabulary fails closed, got {outcome:?}"
    );
    store.verify().expect("the foreign-key fault ran");
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn delete_failures_distinguish_before_effect_from_lost_response() {
    let before_coordinate = key("partition/p1/pack/v1/delete-before");
    let after_coordinate = key("partition/p1/pack/v1/delete-after");
    for (case, coordinate, fault, remains) in [
        (
            "before-effect",
            before_coordinate.clone(),
            Fault::FailBefore {
                selection: Selection::delete(Target::Key(before_coordinate)),
                failure: BackendFailure::Transport,
            },
            true,
        ),
        (
            "lost-response",
            after_coordinate.clone(),
            Fault::DeleteThenLoseResponse {
                target: Target::Key(after_coordinate),
            },
            false,
        ),
    ] {
        let store = FaultStore::new()
            .inject(fault)
            .expect("fault selection is unique");
        let adapter = ObjectStoreAdapter::new(store.backend());
        adapter
            .create_if_absent(&coordinate, body(b"pack"))
            .await
            .expect("test object stores");

        let outcome = adapter.delete_idempotent(&coordinate).await;
        assert!(
            matches!(outcome, Err(StoreError::Retryable { .. })),
            "{case} transport loss is retryable, got {outcome:?}"
        );
        assert_eq!(
            remains,
            adapter
                .read(&coordinate, read_bound())
                .await
                .expect("state observation passes through")
                .is_some(),
            "{case} durable effect differs at the response-loss boundary"
        );
        store.verify().expect("the delete fault ran");
    }
}
