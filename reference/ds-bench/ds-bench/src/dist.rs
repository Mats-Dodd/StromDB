use anyhow::{Context, Result};
use hdrhistogram::serialization::{Deserializer, Serializer, V2Serializer};
use hdrhistogram::Histogram;
use serde::Serialize;
use std::path::Path;

/// If DS_BENCH_HDR_OUT is set, serialize `hist` (HdrHistogram V2) to
/// `{DS_BENCH_HDR_OUT}/{label}.hdr`. When DS_BENCH_INSTANCE is also set and
/// non-empty the file is `{DS_BENCH_HDR_OUT}/{label}-{instance}.hdr` so that
/// multiple fleet pods writing to a shared PVC each produce a distinct file.
/// Additive: callers ignore failures so a missing/unwritable sink never affects
/// the measured run.
pub fn emit_hdr(hist: &Histogram<u64>, label: &str) {
    let Ok(dir) = std::env::var("DS_BENCH_HDR_OUT") else { return };
    let filename = match std::env::var("DS_BENCH_INSTANCE").ok().filter(|s| !s.is_empty()) {
        Some(instance) => format!("{label}-{instance}.hdr"),
        None => format!("{label}.hdr"),
    };
    let path = Path::new(&dir).join(filename);
    let mut buf = Vec::new();
    if V2Serializer::new().serialize(hist, &mut buf).is_ok() {
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::write(&path, &buf);
    }
}

/// Merge every `*.hdr` file in `dir` into one histogram (exact, lossless).
/// When `label_prefix` is `Some(prefix)`, only files whose name starts with
/// that prefix are included (e.g. `"mixed-write"` skips fanout/read files).
/// When `label_prefix` is `None` all `*.hdr` files are merged (original behaviour).
pub fn merge_dir(dir: &Path) -> Result<Histogram<u64>> {
    merge_dir_filtered(dir, None)
}

pub fn merge_dir_filtered(dir: &Path, label_prefix: Option<&str>) -> Result<Histogram<u64>> {
    let mut merged = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
        .context("alloc merged histogram")?;
    merged.auto(true);
    let mut de = Deserializer::new();
    for entry in std::fs::read_dir(dir).context("read hdr dir")? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("hdr") { continue; }
        // Apply optional prefix filter on the file stem (filename without extension).
        if let Some(prefix) = label_prefix {
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("");
            if !stem.starts_with(prefix) {
                continue;
            }
        }
        let bytes = std::fs::read(&path).with_context(|| format!("read {path:?}"))?;
        let h: Histogram<u64> = de
            .deserialize(&mut std::io::Cursor::new(bytes))
            .map_err(|e| anyhow::anyhow!("deserialize {path:?}: {e:?}"))?;
        merged.add(&h).map_err(|e| anyhow::anyhow!("merge {path:?}: {e:?}"))?;
    }
    Ok(merged)
}

/// Per-workload headline metrics, summed across every per-pod JSON in the
/// results dir. Each workload has a *different* right headline:
///   * multi-stream / mixed → summed `aggregate_ops_per_sec` (writes/s)
///   * catch-up             → summed `aggregate_mb_per_sec` + `bytes_received_total`
///   * fan-out              → ops/s is N/A (latency is the headline); we instead
///                            sum `events_received` → `events_per_sec` over duration
///
/// Fields are `Option` so the JSON omits / nulls metrics that don't apply to a
/// workload rather than emitting a misleading `0`. The workload is auto-detected
/// from each per-pod JSON's `scenario` field; no flag required.
#[derive(Serialize, Default)]
pub struct MergeSummary {
    pub merged_count: u64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub max_ms: f64,
    /// Summed writes/sec — multi-stream / mixed only. `None` (null/omitted) for
    /// fan-out and catch-up, where ops/s is not the headline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate_ops_per_sec: Option<f64>,
    /// Summed MB/sec — catch-up only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate_mb_per_sec: Option<f64>,
    /// Total bytes received — catch-up only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_received_total: Option<u64>,
    /// Total events received — fan-out only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events_received_total: Option<u64>,
    /// Events/sec (events_received_total / max per-pod duration) — fan-out only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events_per_sec: Option<f64>,
    /// Summed aggregate_events_per_sec — multi-fanout only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate_events_per_sec: Option<f64>,
    /// Summed bytes/sec — reads scenario only (cold-tier GB/s, splice MB/s).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_per_sec: Option<f64>,
    /// Summed backpressure (503/429) responses across pods — reads scenario only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backpressure_total: Option<u64>,
    /// Summed non-backpressure errors across pods — reads scenario only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub other_err_total: Option<u64>,
    /// Wall-clock span covered by the fleet's measure windows: max(end) − min(start)
    /// across pods, seconds. Only present when pods emitted window stamps.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measure_span_secs: Option<f64>,
    /// The largest single pod's measure window, seconds (the nominal duration).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measure_window_secs: Option<f64>,
    /// Whether the fleet's windows overlapped enough for the summed rates to be
    /// meaningful: span ≤ WINDOW_ALIGN_FACTOR × window. When false, the summed
    /// `aggregate_ops_per_sec` multiply-counts server capacity (pods measured at
    /// different wall times) and MUST NOT be used as a throughput reading.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub windows_aligned: Option<bool>,
    /// The fleet's CONCURRENT measure window (unix ms): [max(start), min(end)] —
    /// the interval when every pod was loading the server at once. Scope the
    /// server's CPU%/memory samples to this so "CPU at saturation" reflects the
    /// loaded window, not the whole cell (idle setup/warmup/upload) it was averaged
    /// over before. Present only when the pods actually overlapped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measure_overlap_start_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measure_overlap_end_unix_ms: Option<u64>,
    /// Number of per-pod result JSONs that contributed to this merge. A caller
    /// that knows the expected fleet size (PARALLELISM) compares the two: a
    /// short count means some pods never reported (Spot preemption / OOM), so
    /// the summed `aggregate_ops_per_sec` under-counts and the cell is suspect.
    /// `merged_count` (a histogram SAMPLE count) cannot serve this role.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pods_reported: Option<u64>,
}

/// Maximum fleet window span, as a multiple of the nominal per-pod window, for the
/// summed per-pod rates to still be considered one concurrent measurement. The
/// barrier normally aligns starts to within seconds, so a healthy run sits near
/// 1.0 (e.g. a 25 s window with pods starting ≤2 s apart → ~1.08). 1.25 keeps
/// comfortable headroom for that jitter while catching genuine misalignment:
/// at the boundary two windows overlap ≥75%, bounding the summed-rate overcount to
/// ~25%. (The old 2.0 admitted up to ~2× — two adjacent, barely-overlapping windows
/// summed as if concurrent.) NB: dividing summed per-pod TOTALS by the intersection
/// span would inflate MORE under stagger, not less — the correct lever is this
/// tolerance, not an overlap-window denominator.
const WINDOW_ALIGN_FACTOR: f64 = 1.25;

/// Accumulators built from scanning the per-pod JSONs in the results dir.
#[derive(Default)]
struct HeadlineAcc {
    saw_ops: bool,
    ops: f64,
    saw_catchup: bool,
    mb_per_sec: f64,
    bytes_received: u64,
    saw_fanout: bool,
    events_received: u64,
    max_duration_secs: f64,
    saw_multi_fanout: bool,
    multi_fanout_events_per_sec: f64,
    /// Summed bytes/sec from reads pods (bytes_per_sec field in per-pod JSON).
    saw_reads: bool,
    reads_bytes_per_sec: f64,
    /// Summed counts.{backpressure,other_err} from reads pods.
    reads_backpressure: u64,
    reads_other_err: u64,
    /// Fleet-wide measure-window bounds (unix ms) accumulated from per-pod
    /// measure_{start,end}_unix_ms stamps, plus the largest per-pod window.
    /// Summing per-pod rates is only valid when the windows overlap; these feed
    /// the windows_aligned verdict in the merged summary.
    win_min_start_ms: Option<u64>,
    win_max_end_ms: Option<u64>,
    win_max_window_ms: u64,
    /// Overlap-window bounds: max of per-pod starts, min of per-pod ends. Their
    /// interval is the window when ALL pods were measuring concurrently.
    win_max_start_ms: Option<u64>,
    win_min_end_ms: Option<u64>,
    /// Count of per-pod result JSONs scanned (one file per fleet pod). Feeds
    /// pods_reported so a caller can detect a partial fleet.
    pods_reported: u64,
}

fn scan_headlines(results_dir: Option<&Path>) -> HeadlineAcc {
    let mut acc = HeadlineAcc::default();
    let Some(dir) = results_dir else { return acc };
    let Ok(rd) = std::fs::read_dir(dir) else { return acc };
    for entry in rd.flatten() {
        let p = entry.path();
        if p.extension().and_then(|e| e.to_str()) != Some("json") { continue; }
        let Ok(txt) = std::fs::read_to_string(&p) else { continue };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) else { continue };
        // One result JSON per fleet pod: count contributors so a short fleet
        // (preempted / OOMed pod that never uploaded) is detectable downstream.
        acc.pods_reported += 1;
        // Window stamps are scenario-agnostic: accumulate them from any pod JSON
        // that carries them (older clients simply don't emit them).
        if let (Some(s), Some(e)) = (
            v.get("measure_start_unix_ms").and_then(|x| x.as_u64()),
            v.get("measure_end_unix_ms").and_then(|x| x.as_u64()),
        ) {
            acc.win_min_start_ms = Some(acc.win_min_start_ms.map_or(s, |m| m.min(s)));
            acc.win_max_end_ms = Some(acc.win_max_end_ms.map_or(e, |m| m.max(e)));
            acc.win_max_start_ms = Some(acc.win_max_start_ms.map_or(s, |m| m.max(s)));
            acc.win_min_end_ms = Some(acc.win_min_end_ms.map_or(e, |m| m.min(e)));
            acc.win_max_window_ms = acc.win_max_window_ms.max(e.saturating_sub(s));
        }
        let scenario = v.get("scenario").and_then(|x| x.as_str()).unwrap_or("");
        if scenario.starts_with("catch-up") || scenario.starts_with("bootstrap") {
            // bootstrap (reconnect/catch-up) reports bytes_received_total like catch-up
            // but no aggregate_mb_per_sec (unwrap_or 0.0); both surface bytes_received.
            acc.saw_catchup = true;
            acc.mb_per_sec += v.get("aggregate_mb_per_sec").and_then(|x| x.as_f64()).unwrap_or(0.0);
            acc.bytes_received += v.get("bytes_received_total").and_then(|x| x.as_u64()).unwrap_or(0);
        } else if scenario == "fanout" {
            acc.saw_fanout = true;
            acc.events_received += v.get("events_received").and_then(|x| x.as_u64()).unwrap_or(0);
            let d = v
                .get("duration_secs")
                .and_then(|x| x.as_f64())
                .or_else(|| v.get("elapsed_secs").and_then(|x| x.as_f64()))
                .unwrap_or(0.0);
            if d > acc.max_duration_secs {
                acc.max_duration_secs = d;
            }
        } else if scenario == "multi-fanout" {
            acc.saw_multi_fanout = true;
            acc.multi_fanout_events_per_sec += v
                .get("aggregate_events_per_sec")
                .and_then(|x| x.as_f64())
                .unwrap_or(0.0);
        } else if scenario == "reads" {
            // Reads: collect both ops/s (for the headline reads/s counter) and
            // bytes/s (for cold-tier GB/s and splice MB/s columns in the report).
            if let Some(ops) = v.get("aggregate_ops_per_sec").and_then(|x| x.as_f64()) {
                acc.saw_ops = true;
                acc.ops += ops;
            }
            if let Some(bps) = v.get("bytes_per_sec").and_then(|x| x.as_f64()) {
                acc.saw_reads = true;
                acc.reads_bytes_per_sec += bps;
            }
            if let Some(counts) = v.get("counts") {
                acc.reads_backpressure +=
                    counts.get("backpressure").and_then(|x| x.as_u64()).unwrap_or(0);
                acc.reads_other_err +=
                    counts.get("other_err").and_then(|x| x.as_u64()).unwrap_or(0);
            }
        } else {
            // multi-stream-write / mixed (and any future ops/s workload).
            if let Some(ops) = v.get("aggregate_ops_per_sec").and_then(|x| x.as_f64()) {
                acc.saw_ops = true;
                acc.ops += ops;
            }
        }
    }
    acc
}

pub fn merge_summary(hdr_dir: &Path, results_dir: Option<&Path>) -> Result<MergeSummary> {
    merge_summary_filtered(hdr_dir, results_dir, None)
}

pub fn merge_summary_filtered(
    hdr_dir: &Path,
    results_dir: Option<&Path>,
    label_prefix: Option<&str>,
) -> Result<MergeSummary> {
    let h = merge_dir_filtered(hdr_dir, label_prefix)?;
    let ms = |v: u64| (v as f64) / 1000.0;
    let acc = scan_headlines(results_dir);
    // Pick the headline(s) by which workload's per-pod JSONs we actually saw.
    // catch-up and fan-out take precedence (they never carry ops/s); ops/s is
    // multi-stream/mixed. A results dir holds one workload's pods at a time.
    let (aggregate_ops_per_sec, aggregate_mb_per_sec, bytes_received_total,
         events_received_total, events_per_sec, aggregate_events_per_sec,
         bytes_per_sec) = if acc.saw_catchup {
        (None, Some(acc.mb_per_sec), Some(acc.bytes_received), None, None, None, None)
    } else if acc.saw_fanout {
        let eps = if acc.max_duration_secs > 0.0 {
            Some(acc.events_received as f64 / acc.max_duration_secs)
        } else {
            None
        };
        (None, None, None, Some(acc.events_received), eps, None, None)
    } else if acc.saw_multi_fanout {
        (None, None, None, None, None, Some(acc.multi_fanout_events_per_sec), None)
    } else if acc.saw_ops {
        // reads scenario: also carry bytes_per_sec when available.
        let bps = if acc.saw_reads { Some(acc.reads_bytes_per_sec) } else { None };
        (Some(acc.ops), None, None, None, None, None, bps)
    } else {
        // No per-pod JSON (e.g. tests merging raw .hdr only): omit all headlines.
        (None, None, None, None, None, None, None)
    };
    let (backpressure_total, other_err_total) = if acc.saw_reads {
        (Some(acc.reads_backpressure), Some(acc.reads_other_err))
    } else {
        (None, None)
    };
    let (measure_span_secs, measure_window_secs, windows_aligned) =
        match (acc.win_min_start_ms, acc.win_max_end_ms) {
            (Some(s), Some(e)) if acc.win_max_window_ms > 0 => {
                let span = e.saturating_sub(s) as f64 / 1000.0;
                let window = acc.win_max_window_ms as f64 / 1000.0;
                (Some(span), Some(window), Some(span <= WINDOW_ALIGN_FACTOR * window))
            }
            _ => (None, None, None),
        };
    // Concurrent window [max(start), min(end)] — only when the pods actually
    // overlapped (min_end > max_start); a non-overlapping fleet has none.
    let (measure_overlap_start_unix_ms, measure_overlap_end_unix_ms) =
        match (acc.win_max_start_ms, acc.win_min_end_ms) {
            (Some(ms), Some(me)) if me > ms => (Some(ms), Some(me)),
            _ => (None, None),
        };
    Ok(MergeSummary {
        merged_count: h.len(),
        p50_ms: ms(h.value_at_quantile(0.5)),
        p90_ms: ms(h.value_at_quantile(0.9)),
        p99_ms: ms(h.value_at_quantile(0.99)),
        p999_ms: ms(h.value_at_quantile(0.999)),
        max_ms: ms(h.max()),
        aggregate_ops_per_sec,
        aggregate_mb_per_sec,
        bytes_received_total,
        events_received_total,
        events_per_sec,
        aggregate_events_per_sec,
        bytes_per_sec,
        backpressure_total,
        other_err_total,
        measure_span_secs,
        measure_window_secs,
        windows_aligned,
        measure_overlap_start_unix_ms,
        measure_overlap_end_unix_ms,
        pods_reported: if acc.pods_reported > 0 { Some(acc.pods_reported) } else { None },
    })
}

#[derive(clap::Args, Debug, Clone)]
pub struct HdrMergeArgs {
    /// Directory containing per-pod *.hdr files.
    #[arg(long)]
    pub hdr_dir: String,
    /// Optional directory of per-pod *.json results (sums aggregate_ops_per_sec).
    #[arg(long)]
    pub results_dir: Option<String>,
    /// When set, only merge *.hdr files whose filename starts with this prefix.
    /// Use e.g. "mixed-write", "mixed-fanout", "mixed-read" to get per-class
    /// percentiles for the mixed workload.  When unset, all *.hdr files are
    /// merged (original behaviour — multi-stream/fan-out/catch-up unaffected).
    #[arg(long)]
    pub label_prefix: Option<String>,
}

pub fn run_merge(args: HdrMergeArgs) -> Result<String> {
    let results = args.results_dir.as_ref().map(Path::new);
    let prefix = args.label_prefix.as_deref();
    let summary = merge_summary_filtered(Path::new(&args.hdr_dir), results, prefix)?;
    Ok(serde_json::to_string_pretty(&summary)?)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// A reads per-pod JSON that carries bytes_per_sec must contribute that value
    /// to the merged summary via scan_headlines.
    #[test]
    fn reads_bytes_per_sec_is_merged() {
        let dir = std::env::temp_dir()
            .join(format!("ds-bench-dist-test-{}-{}", std::process::id(), line!()));
        std::fs::create_dir_all(&dir).unwrap();

        // Write a minimal per-pod JSON for the reads scenario.
        let json = r#"{
            "scenario": "reads",
            "aggregate_ops_per_sec": 500.0,
            "bytes_per_sec": 1000.0,
            "elapsed_secs": 10.0
        }"#;
        let mut f = std::fs::File::create(dir.join("pod0.json")).unwrap();
        f.write_all(json.as_bytes()).unwrap();

        let acc = scan_headlines(Some(&dir));
        let _ = std::fs::remove_dir_all(&dir); // cleanup (best effort)
        assert!(acc.saw_reads, "expected saw_reads=true");
        assert!(
            (acc.reads_bytes_per_sec - 1000.0).abs() < 1e-6,
            "expected reads_bytes_per_sec≈1000, got {}",
            acc.reads_bytes_per_sec
        );
        assert!(
            (acc.ops - 500.0).abs() < 1e-6,
            "expected ops≈500, got {}",
            acc.ops
        );
    }

    fn write_pod_json(dir: &Path, name: &str, start_ms: u64, end_ms: u64, ops: f64) {
        let json = format!(
            r#"{{
                "scenario": "multi-stream-write",
                "aggregate_ops_per_sec": {ops},
                "elapsed_secs": 8.0,
                "measure_start_unix_ms": {start_ms},
                "measure_end_unix_ms": {end_ms}
            }}"#
        );
        std::fs::write(dir.join(name), json).unwrap();
    }

    /// Pods whose measure windows overlap (started within seconds of each other)
    /// → the merged summary reports the window span and windows_aligned=true.
    #[test]
    fn aligned_measure_windows_flagged_ok() {
        let dir = std::env::temp_dir()
            .join(format!("ds-bench-dist-test-{}-{}", std::process::id(), line!()));
        std::fs::create_dir_all(&dir).unwrap();
        // Two 8s windows starting 1s apart → span 9s ≤ 2×8s.
        write_pod_json(&dir, "pod0.json", 1_000_000, 1_008_000, 100.0);
        write_pod_json(&dir, "pod1.json", 1_001_000, 1_009_000, 100.0);

        let s = merge_summary_filtered(&dir, Some(&dir), None).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(s.windows_aligned, Some(true), "9s span over 8s windows is aligned");
        assert!((s.measure_span_secs.unwrap() - 9.0).abs() < 1e-6);
        assert!((s.measure_window_secs.unwrap() - 8.0).abs() < 1e-6);
    }

    /// A MARGINAL stagger — well under the old 2× tolerance but still materially
    /// non-concurrent — must now be flagged misaligned. Two 8 s windows spanning
    /// 12 s (ratio 1.5) overlap only ~50%, so summing their nominal rates assumes
    /// concurrency that isn't there (up to a ~1.5× overcount). The tightened factor
    /// (≤1.25) rejects it; the old 2.0 accepted it.
    #[test]
    fn marginally_staggered_windows_now_misaligned() {
        let dir = std::env::temp_dir()
            .join(format!("ds-bench-dist-test-{}-{}", std::process::id(), line!()));
        std::fs::create_dir_all(&dir).unwrap();
        // Two 8s windows starting 4s apart → span 12s = 1.5×8s.
        write_pod_json(&dir, "pod0.json", 1_000_000, 1_008_000, 100.0);
        write_pod_json(&dir, "pod1.json", 1_004_000, 1_012_000, 100.0);

        let s = merge_summary_filtered(&dir, Some(&dir), None).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(s.windows_aligned, Some(false), "12s span over 8s windows must be misaligned");
    }

    /// Pods whose measure windows do NOT overlap (staggered starts — the 500k-cell
    /// pathology: summed per-pod rates multiply-count the server's capacity)
    /// → windows_aligned=false so consumers can reject the cell.
    #[test]
    fn staggered_measure_windows_flagged_misaligned() {
        let dir = std::env::temp_dir()
            .join(format!("ds-bench-dist-test-{}-{}", std::process::id(), line!()));
        std::fs::create_dir_all(&dir).unwrap();
        // Two 8s windows starting 60s apart → span 68s > 2×8s.
        write_pod_json(&dir, "pod0.json", 1_000_000, 1_008_000, 100.0);
        write_pod_json(&dir, "pod1.json", 1_060_000, 1_068_000, 100.0);

        let s = merge_summary_filtered(&dir, Some(&dir), None).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(s.windows_aligned, Some(false), "68s span over 8s windows is misaligned");
        assert!((s.measure_span_secs.unwrap() - 68.0).abs() < 1e-6);
    }

    /// The merge exposes the fleet's CONCURRENT (overlap) measure window
    /// [max(start), min(end)] so a caller can scope the server's CPU%/mem samples
    /// to the interval when ALL pods were loading it (not the whole cell diluted
    /// by staggered setup/upload).
    #[test]
    fn exposes_concurrent_overlap_window() {
        let dir = std::env::temp_dir()
            .join(format!("ds-bench-dist-test-{}-{}", std::process::id(), line!()));
        std::fs::create_dir_all(&dir).unwrap();
        write_pod_json(&dir, "pod0.json", 1_000_000, 1_008_000, 100.0);
        write_pod_json(&dir, "pod1.json", 1_002_000, 1_010_000, 100.0);

        let s = merge_summary_filtered(&dir, Some(&dir), None).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        // overlap = [max_start=1_002_000, min_end=1_008_000]
        assert_eq!(s.measure_overlap_start_unix_ms, Some(1_002_000));
        assert_eq!(s.measure_overlap_end_unix_ms, Some(1_008_000));
    }

    /// The merge counts how many per-pod result JSONs contributed, so a caller
    /// that knows the expected fleet size (PARALLELISM) can detect a partial
    /// fleet (Spot preemption / OOM dropped a pod → summed throughput silently
    /// under-counts). merged_count is a SAMPLE count and cannot serve this.
    #[test]
    fn pods_reported_counts_contributing_pod_jsons() {
        let dir = std::env::temp_dir()
            .join(format!("ds-bench-dist-test-{}-{}", std::process::id(), line!()));
        std::fs::create_dir_all(&dir).unwrap();
        write_pod_json(&dir, "pod0.json", 1_000_000, 1_008_000, 100.0);
        write_pod_json(&dir, "pod1.json", 1_001_000, 1_009_000, 100.0);
        write_pod_json(&dir, "pod2.json", 1_002_000, 1_010_000, 100.0);

        let s = merge_summary_filtered(&dir, Some(&dir), None).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(s.pods_reported, Some(3), "three pod JSONs contributed");
    }

    /// Per-pod JSONs without window stamps (older client) → no alignment verdict,
    /// summary fields omitted (back-compat: never mis-flag old data).
    #[test]
    fn missing_window_stamps_yield_no_alignment_verdict() {
        let dir = std::env::temp_dir()
            .join(format!("ds-bench-dist-test-{}-{}", std::process::id(), line!()));
        std::fs::create_dir_all(&dir).unwrap();
        let json = r#"{"scenario": "multi-stream-write", "aggregate_ops_per_sec": 100.0, "elapsed_secs": 8.0}"#;
        std::fs::write(dir.join("pod0.json"), json).unwrap();

        let s = merge_summary_filtered(&dir, Some(&dir), None).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(s.windows_aligned, None);
        assert_eq!(s.measure_span_secs, None);
    }
}
