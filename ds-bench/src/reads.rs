use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use bytes::BytesMut;
use clap::Args;
use clap::ValueEnum;
use futures::StreamExt;
use hdrhistogram::Histogram;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::backend::ApiStyle;
use crate::backend::Backend;
use crate::common::Counts;
use crate::common::LatencySummary;
use crate::common::build_client;
use crate::common::fill_payload;
use crate::common::merge;
use crate::common::new_histogram;
use crate::common::record;
use crate::common::summarize;
use crate::sse_util::build_payload;
use crate::sse_util::extract_send_ns_maybe_b64;
use crate::sse_util::find_event_end;
use crate::sse_util::parse_sse_data;
use crate::sse_util::unix_nanos_now;

// ---------------------------------------------------------------------------
// Read-scalability workload, two modes:
//
//   catchup    — sustained hot catch-up reads of a resident stream: each reader
//                scans the whole seeded stream from the start, wraps, repeats.
//                Measures the resident/replay read path (sendfile bandwidth) at
//                a fixed cardinality, swept over connections.
//
//   long-poll  — a read is a LONG POLL that blocks at the tail waiting for new
//                data. A light per-stream writer appends timestamped records;
//                reader connections tail via `?offset=<tail>&live=long-poll` and
//                every reader of a stream receives every append (fan-out). We
//                measure per-record delivery latency (append→receive) and the
//                connection level at which delivery falls over.
//
//   sse        — like long-poll but over a held Server-Sent Events connection
//                (`?offset=now&live=sse`). Supported by BOTH Durable and Ursula,
//                so it gives an apples-to-apples live-read comparison. Same
//                writer + fan-out + delivery-latency model; the timestamp rides
//                in the payload as hex text (SSE-safe) via `sse_util`.
// ---------------------------------------------------------------------------

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReadMode {
    /// Re-scan resident data from the start in a hot loop.
    Catchup,
    /// Long-poll the tail, receiving live appends from the writer.
    LongPoll,
    /// Subscribe at the tail over SSE (works for Durable + Ursula).
    Sse,
}

#[derive(Args, Debug, Clone)]
pub struct ReadsArgs {
    /// Which read benchmark to run.
    #[arg(long, value_enum, default_value_t = ReadMode::Catchup)]
    pub mode: ReadMode,

    /// Target base URL(s). Comma-separated for round-robin across nodes.
    #[arg(long)]
    pub target: String,

    /// Backend API style.
    #[arg(long, value_enum, default_value_t = ApiStyle::Ursula)]
    pub api_style: ApiStyle,

    /// Bucket name (Ursula only — ignored by Durable).
    #[arg(long, default_value = "bench-reads")]
    pub bucket: String,

    /// Stream name prefix. Streams are `{stream}-{i}` for i in 0..streams.
    #[arg(long, default_value = "bench-reads-stream")]
    pub stream: String,

    /// Number of streams to read across. Readers are pinned to stream
    /// `reader_idx % streams` (the cardinality axis).
    #[arg(long, default_value_t = 1)]
    pub streams: usize,

    /// catchup: seeding record size + per-read latency unit.
    /// long-poll: size of each appended record (>= 8; first 8 bytes carry the
    /// write timestamp used for delivery latency).
    #[arg(long, default_value_t = 4096)]
    pub read_size_bytes: usize,

    /// Number of concurrent reader connections (the load axis).
    #[arg(long, default_value_t = 8)]
    pub connections: usize,

    /// Length of the measured window, in seconds.
    #[arg(long, default_value_t = 60)]
    pub duration_secs: u64,

    /// Warm-up seconds: run but uncounted.
    #[arg(long, default_value_t = 0)]
    pub warmup_secs: u64,

    /// Settle seconds: idle gap between warm-up and the measured window.
    #[arg(long, default_value_t = 0)]
    pub settle_secs: u64,

    /// catchup ONLY: bytes to pre-seed each stream with (idempotent).
    #[arg(long, default_value_t = 16_777_216)] // 16 MiB
    pub seed_bytes: u64,

    /// long-poll ONLY: appends per second PER STREAM by the writer. Low — it is
    /// the cadence that wakes long-polls; fan-out grows with `connections`.
    #[arg(long, default_value_t = 50.0)]
    pub append_rate_per_sec: f64,

    /// HTTP request timeout in seconds. For long-poll it must exceed the server
    /// long-poll timeout so an idle poll returns up-to-date rather than erroring.
    #[arg(long, default_value_t = 60)]
    pub request_timeout_secs: u64,
}

#[derive(Serialize)]
pub struct ReadsResult {
    pub scenario: &'static str,
    pub mode: ReadMode,
    pub api_style: ApiStyle,
    pub target: String,
    pub stream: String,
    pub streams: usize,
    pub read_size_bytes: usize,
    pub connections: usize,
    pub duration_secs: u64,
    pub warmup_secs: u64,
    pub settle_secs: u64,
    pub seed_bytes: u64,
    pub append_rate_per_sec: f64,
    pub elapsed_secs: f64,
    pub counts: Counts,
    pub bytes_read_total: u64,
    pub aggregate_ops_per_sec: f64,
    pub bytes_per_sec: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub latency_ms: LatencySummary,
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn read_url(b: &Backend, base_idx: usize, stream: &str, offset: &str) -> String {
    let base = b.base_for(base_idx);
    match b.kind {
        ApiStyle::Durable => format!("{base}/v1/stream/{stream}?offset={offset}"),
        ApiStyle::Ursula => format!("{base}/{}/{stream}?offset={offset}", b.bucket),
        ApiStyle::S2 => unreachable!("S2 is excluded from reads workload"),
    }
}

/// Pin a reader to a stream: reader `idx` reads `streams[idx % streams.len()]`.
fn stream_for(streams: &[String], idx: usize) -> &str {
    &streams[idx % streams.len()]
}

/// Offsets from a common base: warmup (uncounted) → settle (idle) → measure.
fn phase_offsets(
    warmup_secs: u64,
    settle_secs: u64,
    duration_secs: u64,
) -> (Duration, Duration, Duration) {
    let warmup_end = Duration::from_secs(warmup_secs);
    let measure_start = Duration::from_secs(warmup_secs + settle_secs);
    let deadline = Duration::from_secs(warmup_secs + settle_secs + duration_secs);
    (warmup_end, measure_start, deadline)
}

/// Guard the clock-domain assumption of the live-tail modes. Long-poll and SSE
/// stamp a send time in one process and subtract it from a receive time; that is
/// only meaningful when writer and readers share one wall clock — a single fleet
/// pod. Across pods the difference is cross-node clock skew, not delivery latency,
/// so refuse to produce a misleading number. Catch-up times each reader's own
/// replay locally (no cross-process timestamp) and is safe at any fleet size.
/// `shards` is DS_BENCH_SHARDS (0/unset ⇒ single pod).
fn assert_single_pod_for_live(mode: ReadMode, shards: usize) -> Result<()> {
    let is_live = matches!(mode, ReadMode::LongPoll | ReadMode::Sse);
    if is_live && shards > 1 {
        anyhow::bail!(
            "live-tail read mode ({mode:?}) requires a single fleet pod \
             (DS_BENCH_SHARDS={shards}): its delivery latency compares a writer \
             send-time to a reader receive-time on ONE process clock. Across pods \
             that measures cross-node clock skew, not latency. Run live reads with \
             pods=1, or use catch-up for multi-pod read scaling."
        );
    }
    Ok(())
}

pub async fn run(args: ReadsArgs) -> Result<ReadsResult> {
    if args.api_style == ApiStyle::S2 {
        anyhow::bail!(
            "reads workload is not supported for S2: its paginated JSON read \
             is not comparable to the Durable Streams read path"
        );
    }
    let shards: usize = std::env::var("DS_BENCH_SHARDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    assert_single_pod_for_live(args.mode, shards)?;
    match args.mode {
        ReadMode::Catchup => run_catchup(args).await,
        ReadMode::LongPoll => run_longpoll(args).await,
        ReadMode::Sse => run_sse(args).await,
    }
}

// ===========================================================================
// Mode: catchup — sustained hot re-scan of resident data
// ===========================================================================

/// One catch-up pass: read from `offset` until up-to-date; on up-to-date return
/// "-1" so the next pass restarts from the beginning (the hot-read loop).
async fn catch_up_once(
    b: &Backend,
    base_idx: usize,
    stream: &str,
    start_offset: &str,
) -> Result<(u64, String)> {
    let mut offset = start_offset.to_string();
    let mut total: u64 = 0;
    let mut guard: u32 = 0;
    loop {
        let url = read_url(b, base_idx, stream, &offset);
        let resp = b.client.get(&url).send().await.context("reads GET")?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("reads GET status {status}");
        }
        let up = resp
            .headers()
            .get("stream-up-to-date")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let next = resp
            .headers()
            .get("stream-next-offset")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        // Stream the body to count bytes without holding the whole resident stream
        // per reader (the documented catch-up OOM ceiling).
        total += crate::common::drain_len(resp).await.context("reads body")?;
        guard += 1;
        match next {
            Some(n) if !up && n != offset && guard < 100_000 => offset = n,
            _ => return Ok((total, "-1".to_string())),
        }
    }
}

/// Probe stream byte size via one GET from the start, reading the
/// `stream-next-offset` header. Returns 0 if absent/unparseable.
async fn probe_stream_size(b: &Backend, stream: &str) -> Result<u64> {
    let url = read_url(b, 0, stream, "-1");
    let resp = b.client.get(&url).send().await?;
    if !resp.status().is_success() {
        return Ok(0);
    }
    // Durable reports `<seq>_<bytes>`; Ursula a bare integer. Take the trailing
    // numeric run so both parse to total bytes written.
    let next_offset = resp
        .headers()
        .get("stream-next-offset")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.rsplit('_').next())
        .and_then(|s| s.trim_start_matches('0').parse::<u64>().ok().or(Some(0)))
        .unwrap_or(0);
    Ok(next_offset)
}

/// Ensure the stream holds at least `seed_bytes` of resident data.
async fn ensure_seeded(b: &Backend, stream: &str, seed_bytes: u64, record_bytes: usize) -> Result<()> {
    if seed_bytes == 0 {
        return Ok(());
    }
    let current_bytes = probe_stream_size(b, stream).await.unwrap_or(0);
    if current_bytes >= seed_bytes {
        tracing::info!(current_bytes, seed_bytes, "stream already seeded — skipping");
        return Ok(());
    }
    let need_bytes = seed_bytes.saturating_sub(current_bytes);
    let record_count = (need_bytes as usize).div_ceil(record_bytes.max(1));
    tracing::info!(current_bytes, seed_bytes, record_count, record_bytes, "seeding stream");

    let payload = Arc::new(fill_payload(record_bytes, 0xC0DE));
    let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
    let mut joins = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let b = b.clone();
        let stream = stream.to_string();
        let payload = payload.clone();
        joins.push(tokio::spawn(async move {
            let _permit = permit;
            b.append_request(0, &stream, &payload, None, "application/octet-stream")
                .send()
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false)
        }));
    }
    let mut failed = 0u64;
    for j in joins {
        if !j.await.unwrap_or(false) {
            failed += 1;
        }
    }
    if failed > 0 {
        tracing::warn!(failed, record_count, "some seed appends failed");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_reader_catchup(
    backend: Backend,
    base_idx: usize,
    stream: String,
    measure_start: Instant,
    deadline: Instant,
    ok: Arc<AtomicU64>,
    bp: Arc<AtomicU64>,
    err: Arc<AtomicU64>,
    bytes_total: Arc<AtomicU64>,
    hist: Arc<Mutex<Histogram<u64>>>,
) {
    let mut offset = "-1".to_string();
    let mut local = new_histogram();
    while Instant::now() < deadline {
        let started = Instant::now();
        match catch_up_once(&backend, base_idx, &stream, &offset).await {
            Ok((bytes, next_offset)) => {
                if Instant::now() >= measure_start {
                    ok.fetch_add(1, Ordering::Relaxed);
                    bytes_total.fetch_add(bytes, Ordering::Relaxed);
                    record(&mut local, started);
                }
                offset = next_offset;
            }
            Err(e) => {
                let msg = e.to_string();
                let counting = Instant::now() >= measure_start;
                if msg.contains("503") || msg.contains("429") {
                    if counting {
                        bp.fetch_add(1, Ordering::Relaxed);
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                } else if counting {
                    err.fetch_add(1, Ordering::Relaxed);
                }
                offset = "-1".to_string();
            }
        }
    }
    let mut h = hist.lock().await;
    merge(&mut h, &local);
}

async fn run_catchup(args: ReadsArgs) -> Result<ReadsResult> {
    let client = build_client(args.request_timeout_secs)?;
    let backend = Backend::new(args.api_style, &args.target, &args.bucket, "", client);

    let n_streams = args.streams.max(1);
    let streams: Vec<String> = (0..n_streams).map(|i| format!("{}-{}", args.stream, i)).collect();

    backend.ensure_namespace().await?;
    for s in &streams {
        backend.create_stream(s, "application/octet-stream").await?;
        ensure_seeded(&backend, s, args.seed_bytes, args.read_size_bytes).await?;
    }

    let ok = Arc::new(AtomicU64::new(0));
    let bp = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let bytes_total = Arc::new(AtomicU64::new(0));
    let hist = Arc::new(Mutex::new(new_histogram()));

    let base = Instant::now();
    let (_w, measure_off, deadline_off) =
        phase_offsets(args.warmup_secs, args.settle_secs, args.duration_secs);
    let measure_start = base + measure_off;
    let deadline = base + deadline_off;

    let mut workers = Vec::with_capacity(args.connections);
    for idx in 0..args.connections {
        let backend = backend.clone();
        let stream = stream_for(&streams, idx).to_string();
        let (ok, bp, err, bytes_total, hist) =
            (ok.clone(), bp.clone(), err.clone(), bytes_total.clone(), hist.clone());
        workers.push(tokio::spawn(async move {
            run_reader_catchup(backend, idx, stream, measure_start, deadline, ok, bp, err, bytes_total, hist).await
        }));
    }
    for w in workers {
        let _ = w.await;
    }

    finish(args, ReadMode::Catchup, &ok, &bp, &err, &bytes_total, &hist).await
}

// ===========================================================================
// Mode: long-poll — block at the tail for live appends (writer + readers)
// ===========================================================================

fn long_poll_url(b: &Backend, base_idx: usize, stream: &str, offset: &str) -> String {
    format!("{}&live=long-poll", read_url(b, base_idx, stream, offset))
}

/// Resolve a stream's current tail as a concrete byte offset (`offset=now`).
async fn tail_offset(b: &Backend, base_idx: usize, stream: &str) -> Result<String> {
    let url = read_url(b, base_idx, stream, "now");
    let resp = b.client.get(&url).send().await.context("tail probe")?;
    let next = resp
        .headers()
        .get("stream-next-offset")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let _ = resp.bytes().await;
    Ok(next.unwrap_or_else(|| "now".to_string()))
}

/// One long-poll: block at `offset` until new data or the server timeout.
/// Returns (delivered bytes, next offset). Empty bytes = idle timeout.
async fn long_poll_once(
    b: &Backend,
    base_idx: usize,
    stream: &str,
    offset: &str,
) -> Result<(Vec<u8>, String)> {
    let url = long_poll_url(b, base_idx, stream, offset);
    let resp = b.client.get(&url).send().await.context("long-poll GET")?;
    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!("long-poll status {status}");
    }
    let next = resp
        .headers()
        .get("stream-next-offset")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| offset.to_string());
    let body = resp.bytes().await.context("long-poll body")?.to_vec();
    Ok((body, next))
}

async fn run_writer(
    backend: Backend,
    stream: String,
    base: Instant,
    deadline: Instant,
    rate_per_sec: f64,
    record_bytes: usize,
) {
    let interval = Duration::from_secs_f64(1.0 / rate_per_sec.max(1e-3));
    let mut payload = fill_payload(record_bytes.max(8), 0xC0DE);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    while Instant::now() < deadline {
        ticker.tick().await;
        let ts = base.elapsed().as_nanos() as u64;
        payload[0..8].copy_from_slice(&ts.to_le_bytes());
        let _ = backend
            .append_request(0, &stream, &payload, None, "application/octet-stream")
            .send()
            .await;
    }
}

/// Decode per-record delivery latencies (µs) from a long-poll body whose records
/// are laid out at `step`-byte stride, each beginning with an 8-byte little-endian
/// send timestamp (ns, on the reader's monotonic `base` clock — valid only under
/// the pods=1 single-clock constraint). A record's timestamp is trusted only when
/// it is a plausible PAST instant (`0 < ts <= now_ns`); otherwise the 8-byte window
/// is misaligned framing or padding and the record is SKIPPED. The previous
/// fixed-stride walk floored an implausible (future) `ts` to ~0 µs via
/// `saturating_sub` and still counted it, biasing long-poll delivery latency toward
/// zero whenever the body wasn't raw concatenated payloads.
fn decode_longpoll_latencies(body: &[u8], step: usize, now_ns: u64) -> Vec<u64> {
    let step = step.max(8);
    let mut out = Vec::new();
    let mut i = 0;
    while i + 8 <= body.len() {
        let ts = u64::from_le_bytes(body[i..i + 8].try_into().unwrap());
        if ts != 0 && ts <= now_ns {
            out.push((now_ns - ts) / 1000);
        }
        i += step;
    }
    out
}

#[allow(clippy::too_many_arguments)]
async fn run_reader_longpoll(
    backend: Backend,
    base_idx: usize,
    stream: String,
    base: Instant,
    measure_start: Instant,
    deadline: Instant,
    record_bytes: usize,
    ok: Arc<AtomicU64>,
    bp: Arc<AtomicU64>,
    err: Arc<AtomicU64>,
    bytes_total: Arc<AtomicU64>,
    hist: Arc<Mutex<Histogram<u64>>>,
) {
    let step = record_bytes.max(8);
    let mut offset = tail_offset(&backend, base_idx, &stream)
        .await
        .unwrap_or_else(|_| "now".to_string());
    let mut local = new_histogram();

    while Instant::now() < deadline {
        match long_poll_once(&backend, base_idx, &stream, &offset).await {
            Ok((body, next)) => {
                if body.is_empty() {
                    // Either a genuine idle long-poll timeout, or a backend that
                    // ignores `live=long-poll` and returns up-to-date immediately
                    // (no long-poll support). The small sleep avoids a hot spin in
                    // the latter case; harmless for the rare durable idle timeout.
                    tokio::time::sleep(Duration::from_millis(5)).await;
                } else if Instant::now() >= measure_start {
                    let now_ns = base.elapsed().as_nanos() as u64;
                    // Only records with a plausible (past, non-padding) timestamp
                    // count as deliveries — misaligned framing must not register as
                    // ~0-latency events (see decode_longpoll_latencies).
                    for lat_us in decode_longpoll_latencies(&body, step, now_ns) {
                        let _ = local.record(lat_us.clamp(local.low(), local.high()));
                        ok.fetch_add(1, Ordering::Relaxed);
                        bytes_total.fetch_add(step as u64, Ordering::Relaxed);
                    }
                }
                offset = next;
            }
            Err(e) => {
                let msg = e.to_string();
                let counting = Instant::now() >= measure_start;
                if msg.contains("503") || msg.contains("429") {
                    if counting {
                        bp.fetch_add(1, Ordering::Relaxed);
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                } else if counting {
                    err.fetch_add(1, Ordering::Relaxed);
                }
                if let Ok(o) = tail_offset(&backend, base_idx, &stream).await {
                    offset = o;
                }
            }
        }
    }
    let mut h = hist.lock().await;
    merge(&mut h, &local);
}

async fn run_longpoll(args: ReadsArgs) -> Result<ReadsResult> {
    let client = build_client(args.request_timeout_secs)?;
    let backend = Backend::new(args.api_style, &args.target, &args.bucket, "", client);

    let n_streams = args.streams.max(1);
    let streams: Vec<String> = (0..n_streams).map(|i| format!("{}-{}", args.stream, i)).collect();

    backend.ensure_namespace().await?;
    for s in &streams {
        backend.create_stream(s, "application/octet-stream").await?;
    }

    let ok = Arc::new(AtomicU64::new(0));
    let bp = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let bytes_total = Arc::new(AtomicU64::new(0));
    let hist = Arc::new(Mutex::new(new_histogram()));

    let base = Instant::now();
    let (_w, measure_off, deadline_off) =
        phase_offsets(args.warmup_secs, args.settle_secs, args.duration_secs);
    let measure_start = base + measure_off;
    let deadline = base + deadline_off;
    let record_bytes = args.read_size_bytes.max(8);

    let mut writers = Vec::with_capacity(n_streams);
    for s in &streams {
        let backend = backend.clone();
        let stream = s.clone();
        let rate = args.append_rate_per_sec;
        writers.push(tokio::spawn(async move {
            run_writer(backend, stream, base, deadline, rate, record_bytes).await
        }));
    }

    let mut readers = Vec::with_capacity(args.connections);
    for idx in 0..args.connections {
        let backend = backend.clone();
        let stream = stream_for(&streams, idx).to_string();
        let (ok, bp, err, bytes_total, hist) =
            (ok.clone(), bp.clone(), err.clone(), bytes_total.clone(), hist.clone());
        readers.push(tokio::spawn(async move {
            run_reader_longpoll(backend, idx, stream, base, measure_start, deadline, record_bytes, ok, bp, err, bytes_total, hist).await
        }));
    }

    for r in readers {
        let _ = r.await;
    }
    for w in writers {
        w.abort();
        let _ = w.await;
    }

    finish(args, ReadMode::LongPoll, &ok, &bp, &err, &bytes_total, &hist).await
}

// ===========================================================================
// Mode: sse — subscribe at the tail over Server-Sent Events (writer + readers).
// Works for both Durable (`live=sse`) and Ursula (native SSE).
// ===========================================================================

async fn run_sse_writer(
    backend: Backend,
    stream: String,
    deadline: Instant,
    rate_per_sec: f64,
    record_bytes: usize,
) {
    let interval = Duration::from_secs_f64(1.0 / rate_per_sec.max(1e-3));
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut seq: u64 = 0;
    while Instant::now() < deadline {
        ticker.tick().await;
        // build_payload stamps unix-nanos as hex text (SSE-safe) at a fixed offset.
        let payload = build_payload(seq, record_bytes.max(48));
        let _ = backend
            .append_request(0, &stream, &payload, None, "text/plain")
            .send()
            .await;
        seq = seq.wrapping_add(1);
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_sse_reader(
    backend: Backend,
    base_idx: usize,
    stream: String,
    measure_start: Instant,
    deadline: Instant,
    record_bytes: usize,
    ok: Arc<AtomicU64>,
    err: Arc<AtomicU64>,
    bytes_total: Arc<AtomicU64>,
    hist: Arc<Mutex<Histogram<u64>>>,
) {
    let mut local = new_histogram();
    // Reconnect until the deadline: a dropped SSE stream must not kill the reader.
    while Instant::now() < deadline {
        let (url, headers) = backend.sse_url_for(base_idx, &stream);
        let resp = match backend.client.get(&url).headers(headers).send().await {
            Ok(r) if r.status().is_success() => r,
            _ => {
                if Instant::now() >= measure_start {
                    err.fetch_add(1, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let mut body = resp.bytes_stream();
        let mut buf = BytesMut::with_capacity(8192);
        loop {
            if Instant::now() >= deadline {
                break;
            }
            let to = deadline.saturating_duration_since(Instant::now()) + Duration::from_secs(1);
            let chunk = match tokio::time::timeout(to, body.next()).await {
                Ok(Some(Ok(c))) => c,
                Ok(Some(Err(_))) | Ok(None) => break, // stream ended → reconnect
                Err(_) => break,                       // deadline reached
            };
            buf.extend_from_slice(&chunk);
            while let Some(idx) = find_event_end(&buf) {
                let raw = buf.split_to(idx + 2).freeze();
                let Some(payload) = parse_sse_data(&raw) else { continue };
                // Control/keepalive events carry no timestamp → skipped here.
                let Some(sent_ns) = extract_send_ns_maybe_b64(&payload) else { continue };
                if Instant::now() < measure_start {
                    continue;
                }
                let now_ns = unix_nanos_now();
                let lat_us = (now_ns.saturating_sub(sent_ns) / 1000)
                    .min(u128::from(local.high())) as u64;
                let _ = local.record(lat_us.max(local.low()));
                ok.fetch_add(1, Ordering::Relaxed);
                bytes_total.fetch_add(record_bytes as u64, Ordering::Relaxed);
            }
        }
    }
    let mut h = hist.lock().await;
    merge(&mut h, &local);
}

async fn run_sse(args: ReadsArgs) -> Result<ReadsResult> {
    let client = build_client(args.request_timeout_secs)?;
    let backend = Backend::new(args.api_style, &args.target, &args.bucket, "", client);

    let n_streams = args.streams.max(1);
    let streams: Vec<String> = (0..n_streams).map(|i| format!("{}-{}", args.stream, i)).collect();

    backend.ensure_namespace().await?;
    for s in &streams {
        backend.create_stream(s, "text/plain").await?;
    }

    let ok = Arc::new(AtomicU64::new(0));
    let bp = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let bytes_total = Arc::new(AtomicU64::new(0));
    let hist = Arc::new(Mutex::new(new_histogram()));

    let base = Instant::now();
    let (_w, measure_off, deadline_off) =
        phase_offsets(args.warmup_secs, args.settle_secs, args.duration_secs);
    let measure_start = base + measure_off;
    let deadline = base + deadline_off;
    let record_bytes = args.read_size_bytes.max(48);

    let mut writers = Vec::with_capacity(n_streams);
    for s in &streams {
        let backend = backend.clone();
        let stream = s.clone();
        let rate = args.append_rate_per_sec;
        writers.push(tokio::spawn(async move {
            run_sse_writer(backend, stream, deadline, rate, record_bytes).await
        }));
    }

    let mut readers = Vec::with_capacity(args.connections);
    for idx in 0..args.connections {
        let backend = backend.clone();
        let stream = stream_for(&streams, idx).to_string();
        let (ok, err, bytes_total, hist) =
            (ok.clone(), err.clone(), bytes_total.clone(), hist.clone());
        readers.push(tokio::spawn(async move {
            run_sse_reader(backend, idx, stream, measure_start, deadline, record_bytes, ok, err, bytes_total, hist).await
        }));
    }

    for r in readers {
        let _ = r.await;
    }
    for w in writers {
        w.abort();
        let _ = w.await;
    }

    finish(args, ReadMode::Sse, &ok, &bp, &err, &bytes_total, &hist).await
}

// ---------------------------------------------------------------------------
// Shared result assembly
// ---------------------------------------------------------------------------

async fn finish(
    args: ReadsArgs,
    mode: ReadMode,
    ok: &AtomicU64,
    bp: &AtomicU64,
    err: &AtomicU64,
    bytes_total: &AtomicU64,
    hist: &Mutex<Histogram<u64>>,
) -> Result<ReadsResult> {
    let elapsed_secs = (args.duration_secs as f64).max(1e-9);
    let bytes_read_total = bytes_total.load(Ordering::Relaxed);
    let ok_count = ok.load(Ordering::Relaxed);
    let h = hist.lock().await;
    let latency = summarize(&h);
    crate::dist::emit_hdr(&h, &format!("reads-{}", std::process::id()));

    let result = ReadsResult {
        scenario: "reads",
        mode,
        api_style: args.api_style,
        target: args.target,
        stream: args.stream,
        streams: args.streams.max(1),
        read_size_bytes: args.read_size_bytes,
        connections: args.connections,
        duration_secs: args.duration_secs,
        warmup_secs: args.warmup_secs,
        settle_secs: args.settle_secs,
        seed_bytes: args.seed_bytes,
        append_rate_per_sec: args.append_rate_per_sec,
        elapsed_secs,
        counts: Counts {
            ok: ok_count,
            backpressure: bp.load(Ordering::Relaxed),
            other_err: err.load(Ordering::Relaxed),
        },
        bytes_read_total,
        aggregate_ops_per_sec: ok_count as f64 / elapsed_secs,
        bytes_per_sec: bytes_read_total as f64 / elapsed_secs,
        p50_ms: latency.p50_ms,
        p90_ms: latency.p90_ms,
        p99_ms: latency.p99_ms,
        p999_ms: latency.p999_ms,
        latency_ms: latency,
    };
    eprintln!("{}", serde_json::to_string_pretty(&result)?);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::assert_single_pod_for_live;
    use super::decode_longpoll_latencies;
    use super::phase_offsets;
    use super::stream_for;
    use super::ReadMode;
    use std::time::Duration;

    /// Live-tail modes (long-poll / sse) compare a writer send-time against a
    /// reader receive-time, valid only when both share one process wall clock —
    /// i.e. a single fleet pod. A multi-pod fleet would measure cross-node clock
    /// skew, not delivery latency, so those modes must refuse to run at shards>1.
    /// Catch-up measures each reader's own replay duration locally → pod-count-safe.
    #[test]
    fn live_modes_require_single_pod() {
        assert!(assert_single_pod_for_live(ReadMode::LongPoll, 2).is_err());
        assert!(assert_single_pod_for_live(ReadMode::Sse, 4).is_err());
        assert!(assert_single_pod_for_live(ReadMode::LongPoll, 1).is_ok());
        assert!(assert_single_pod_for_live(ReadMode::Sse, 1).is_ok());
        // catch-up is not clock-coupled across pods → allowed at any fleet size.
        assert!(assert_single_pod_for_live(ReadMode::Catchup, 8).is_ok());
        // defensive: 0 (unset/parse-fail) is treated as a single pod.
        assert!(assert_single_pod_for_live(ReadMode::Sse, 0).is_ok());
    }

    /// Well-formed body (records at `step` stride, each an 8-byte LE ns timestamp
    /// on the reader's clock) → one latency per record, now_ns − ts in µs.
    #[test]
    fn decode_longpoll_wellformed_records() {
        let now_ns = 5_000_000u64; // 5 ms
        let mut body = Vec::new();
        body.extend_from_slice(&1_000_000u64.to_le_bytes()); // 1 ms ago → 4000 µs
        body.extend_from_slice(&2_000_000u64.to_le_bytes()); // 2 ms ago → 3000 µs
        assert_eq!(decode_longpoll_latencies(&body, 8, now_ns), vec![4000, 3000]);
    }

    /// A misaligned/garbage 8-byte window yields an implausible timestamp
    /// (ts > now → a "future" send, or ts == 0 padding). Those records are
    /// SKIPPED, never floored to ~0 µs — flooring silently biased delivery
    /// latency toward zero when the body wasn't raw concatenated payloads.
    #[test]
    fn decode_longpoll_skips_implausible_timestamps() {
        let now_ns = 5_000_000u64;
        let mut body = Vec::new();
        body.extend_from_slice(&1_000_000u64.to_le_bytes()); // valid → 4000 µs
        body.extend_from_slice(&u64::MAX.to_le_bytes()); // future → skip (not 0/1)
        body.extend_from_slice(&0u64.to_le_bytes()); // padding → skip
        assert_eq!(decode_longpoll_latencies(&body, 8, now_ns), vec![4000]);
    }

    #[test]
    fn phase_offsets_stack_warmup_settle_measure() {
        let (warmup_end, measure_start, deadline) = phase_offsets(10, 5, 60);
        assert_eq!(warmup_end, Duration::from_secs(10));
        assert_eq!(measure_start, Duration::from_secs(15));
        assert_eq!(deadline, Duration::from_secs(75));
    }

    #[test]
    fn pins_reader_to_stream_modulo_n() {
        let streams = vec!["s-0".to_string(), "s-1".to_string(), "s-2".to_string()];
        assert_eq!(stream_for(&streams, 0), "s-0");
        assert_eq!(stream_for(&streams, 3), "s-0");
        assert_eq!(stream_for(&streams, 7), "s-1");
    }

    #[test]
    fn single_stream_pins_all_readers_to_it() {
        let streams = vec!["only-0".to_string()];
        assert_eq!(stream_for(&streams, 0), "only-0");
        assert_eq!(stream_for(&streams, 99), "only-0");
    }
}
