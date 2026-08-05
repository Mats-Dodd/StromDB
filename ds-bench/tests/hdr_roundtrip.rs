use hdrhistogram::Histogram;
use hdrhistogram::serialization::{Serializer, V2Serializer};
use std::path::Path;

/// Write HDR files directly (bypassing the env-var-dependent emit_hdr so
/// parallel tests cannot interfere) then prove that `merge_dir_filtered`
/// with a specific prefix only merges the matching files.
#[test]
fn label_prefix_filters_to_matching_files_only() {
    let dir = std::env::temp_dir().join("ds-bench-label-prefix-test");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let write_hdr = |label: &str, values: &[u64], dir: &Path| {
        let mut h = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        for &v in values { h.record(v).unwrap(); }
        let mut buf = Vec::new();
        V2Serializer::new().serialize(&h, &mut buf).unwrap();
        std::fs::write(dir.join(format!("{label}.hdr")), &buf).unwrap();
    };

    // Two pods emit three classes each.
    write_hdr("mixed-write-0",  &[100, 200],         &dir);
    write_hdr("mixed-write-1",  &[300, 400],         &dir);
    write_hdr("mixed-fanout-0", &[1000, 2000],       &dir);
    write_hdr("mixed-fanout-1", &[3000, 4000],       &dir);
    write_hdr("mixed-read-0",   &[10000, 20000],     &dir);
    write_hdr("mixed-read-1",   &[30000, 40000],     &dir);

    // Unfiltered merge includes all six histograms → 12 samples.
    let all = ds_bench::dist::merge_dir(Path::new(&dir)).unwrap();
    assert_eq!(all.len(), 12, "unfiltered merge should see all 12 samples");

    // Prefix-filtered: only write files → 4 samples (100,200,300,400).
    let write_only = ds_bench::dist::merge_dir_filtered(Path::new(&dir), Some("mixed-write")).unwrap();
    assert_eq!(write_only.len(), 4, "mixed-write prefix should merge only write histograms (4 samples)");

    // Prefix-filtered: only fanout files → 4 samples.
    let fanout_only = ds_bench::dist::merge_dir_filtered(Path::new(&dir), Some("mixed-fanout")).unwrap();
    assert_eq!(fanout_only.len(), 4, "mixed-fanout prefix should merge only fanout histograms (4 samples)");

    // Prefix-filtered: only read files → 4 samples.
    let read_only = ds_bench::dist::merge_dir_filtered(Path::new(&dir), Some("mixed-read")).unwrap();
    assert_eq!(read_only.len(), 4, "mixed-read prefix should merge only read histograms (4 samples)");

    // The three class counts should sum to the unfiltered total.
    assert_eq!(
        write_only.len() + fanout_only.len() + read_only.len(),
        all.len(),
        "per-class counts must sum to total"
    );
}

#[test]
fn emit_then_merge_roundtrips_exactly() {
    let dir = std::env::temp_dir().join("ds-bench-hdr-test");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    unsafe { std::env::set_var("DS_BENCH_HDR_OUT", &dir); }

    // two "pods" each record a known set of values
    let mut a = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    let mut b = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    for v in [10u64, 20, 30] { a.record(v).unwrap(); }
    for v in [40u64, 50, 60] { b.record(v).unwrap(); }
    ds_bench::dist::emit_hdr(&a, "pod-a");
    ds_bench::dist::emit_hdr(&b, "pod-b");

    // merge from the directory == a single histogram over all six values
    let merged = ds_bench::dist::merge_dir(&dir).unwrap();
    let mut expected = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    for v in [10u64, 20, 30, 40, 50, 60] { expected.record(v).unwrap(); }
    assert_eq!(merged.len(), expected.len());
    assert_eq!(merged.value_at_quantile(0.5), expected.value_at_quantile(0.5));
    assert_eq!(merged.max(), expected.max());
}

#[test]
fn merge_summary_reports_per_workload_headline() {
    use serde_json::json;
    // --- fan-out: ops/s is N/A (null), latency is the headline ---
    {
        let dir = std::env::temp_dir().join("ds-bench-headline-fanout");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // one hdr file so merged_count > 0
        let mut h = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        for v in [1000u64, 2000, 3000] { h.record(v).unwrap(); }
        {
            let mut buf = Vec::new();
            V2Serializer::new().serialize(&h, &mut buf).unwrap();
            std::fs::write(dir.join("fanout-0.hdr"), &buf).unwrap();
        }
        // two fan-out pods: no aggregate_ops_per_sec; events_received + duration_secs.
        std::fs::write(
            dir.join("pod-0.json"),
            serde_json::to_string(&json!({
                "scenario": "fanout", "events_received": 6000u64, "duration_secs": 30u64
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.join("pod-1.json"),
            serde_json::to_string(&json!({
                "scenario": "fanout", "events_received": 6000u64, "duration_secs": 30u64
            }))
            .unwrap(),
        )
        .unwrap();
        let s = ds_bench::dist::merge_summary(&dir, Some(&dir)).unwrap();
        assert_eq!(s.aggregate_ops_per_sec, None, "fanout ops/s must be null, not a misleading 0");
        // events_received_total = 12000 over 30s = 400 events/s
        assert_eq!(s.events_received_total, Some(12000));
        assert!((s.events_per_sec.unwrap() - 400.0).abs() < 1.0, "events_per_sec={:?}", s.events_per_sec);
        assert!(s.merged_count > 0);
    }
    // --- catch-up: headline is MB/s + bytes_received_total ---
    {
        let dir = std::env::temp_dir().join("ds-bench-headline-catchup");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut h = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        for v in [1000u64, 2000] { h.record(v).unwrap(); }
        {
            let mut buf = Vec::new();
            V2Serializer::new().serialize(&h, &mut buf).unwrap();
            std::fs::write(dir.join("catch-up-0.hdr"), &buf).unwrap();
        }
        std::fs::write(
            dir.join("pod-0.json"),
            serde_json::to_string(&json!({
                "scenario": "catch-up-stampede",
                "aggregate_mb_per_sec": 50.0, "bytes_received_total": 1000u64
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.join("pod-1.json"),
            serde_json::to_string(&json!({
                "scenario": "catch-up-stampede",
                "aggregate_mb_per_sec": 25.0, "bytes_received_total": 500u64
            }))
            .unwrap(),
        )
        .unwrap();
        let s = ds_bench::dist::merge_summary(&dir, Some(&dir)).unwrap();
        assert_eq!(s.aggregate_ops_per_sec, None, "catch-up has no ops/s headline");
        assert!((s.aggregate_mb_per_sec.unwrap() - 75.0).abs() < 1e-9);
        assert_eq!(s.bytes_received_total, Some(1500));
    }
    // --- multi-stream: headline is summed ops/s (backward compatible) ---
    {
        let dir = std::env::temp_dir().join("ds-bench-headline-multi");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut h = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        for v in [1000u64, 2000] { h.record(v).unwrap(); }
        {
            let mut buf = Vec::new();
            V2Serializer::new().serialize(&h, &mut buf).unwrap();
            std::fs::write(dir.join("multi-stream-0.hdr"), &buf).unwrap();
        }
        std::fs::write(
            dir.join("pod-0.json"),
            serde_json::to_string(&json!({
                "scenario": "multi-stream-write", "aggregate_ops_per_sec": 1000.0
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.join("pod-1.json"),
            serde_json::to_string(&json!({
                "scenario": "multi-stream-write", "aggregate_ops_per_sec": 2000.0
            }))
            .unwrap(),
        )
        .unwrap();
        let s = ds_bench::dist::merge_summary(&dir, Some(&dir)).unwrap();
        assert_eq!(s.aggregate_ops_per_sec, Some(3000.0));
        assert_eq!(s.aggregate_mb_per_sec, None);
        assert_eq!(s.events_per_sec, None);
    }
}

#[test]
fn merge_summary_multi_fanout_headline() {
    use serde_json::json;
    let dir = std::env::temp_dir().join("ds-bench-headline-multi-fanout");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut h = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    for v in [1000u64, 2000, 3000] { h.record(v).unwrap(); }
    {
        let mut buf = Vec::new();
        V2Serializer::new().serialize(&h, &mut buf).unwrap();
        std::fs::write(dir.join("multi-fanout-0.hdr"), &buf).unwrap();
    }
    // Two multi-fanout pods each report aggregate_events_per_sec.
    std::fs::write(
        dir.join("pod-0.json"),
        serde_json::to_string(&json!({
            "scenario": "multi-fanout", "aggregate_events_per_sec": 1234.0
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.join("pod-1.json"),
        serde_json::to_string(&json!({
            "scenario": "multi-fanout", "aggregate_events_per_sec": 766.0
        }))
        .unwrap(),
    )
    .unwrap();
    let s = ds_bench::dist::merge_summary(&dir, Some(&dir)).unwrap();
    assert_eq!(s.aggregate_ops_per_sec, None, "multi-fanout must not set ops/s");
    assert_eq!(s.events_per_sec, None, "multi-fanout must not set fanout events_per_sec");
    let aeps = s.aggregate_events_per_sec.expect("multi-fanout must set aggregate_events_per_sec");
    assert!((aeps - 2000.0).abs() < 1e-9, "aggregate_events_per_sec should be 2000.0, got {aeps}");
    assert!(s.merged_count > 0);
}

#[test]
fn hdr_merge_summary_matches_merged_histogram() {
    let dir = std::env::temp_dir().join("ds-bench-hdr-merge-test");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut a = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    for v in 1u64..=1000 { a.record(v * 1000).unwrap(); } // values in µs
    {
        use hdrhistogram::serialization::{Serializer, V2Serializer};
        let mut buf = Vec::new();
        V2Serializer::new().serialize(&a, &mut buf).unwrap();
        std::fs::write(dir.join("only.hdr"), &buf).unwrap();
    }
    let summary = ds_bench::dist::merge_summary(&dir, None).unwrap();
    assert_eq!(summary.merged_count, 1000);
    // p50 of 1..=1000 (×1000 µs) ≈ 500 ms, within HDR precision
    assert!((summary.p50_ms - 500.0).abs() < 5.0, "p50_ms={}", summary.p50_ms);
}
