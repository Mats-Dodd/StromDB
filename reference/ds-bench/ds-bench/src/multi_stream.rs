use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use clap::Args;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use hdrhistogram::Histogram;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::backend::ApiStyle;
use crate::backend::Backend;
use crate::backend::Producer;
use crate::common::Counts;
use crate::common::LatencySummary;
use crate::common::build_client;
use crate::common::fill_payload;
use crate::common::fill_payload_text;
use crate::common::merge;
use crate::common::new_histogram;
use crate::common::record;
use crate::common::summarize;

#[derive(Args, Debug, Clone)]
pub struct MultiStreamArgs {
    /// Target base URL(s). Comma-separated for round-robin across nodes.
    #[arg(long)]
    pub target: String,

    /// Backend API style.
    #[arg(long, value_enum, default_value_t = ApiStyle::Ursula)]
    pub api_style: ApiStyle,

    /// Bucket name (Ursula only - ignored by Durable / S2).
    #[arg(long, default_value = "bench-multistream")]
    pub bucket: String,

    /// Basin name (S2 only).
    #[arg(long, default_value = "benchmark")]
    pub basin: String,

    /// Number of concurrent streams; one writer task per stream.
    #[arg(long, default_value_t = 1000)]
    pub streams: usize,

    /// Offered concurrency (number of connection-workers). 0 = legacy model: one
    /// in-flight append PER stream through an idempotent producer session
    /// (ordered, deduped — throughput becomes streams/latency; pods own disjoint
    /// pod-prefixed stream sets). >0 = bounded-concurrency pool model measuring
    /// RAW append throughput over a GLOBAL key domain [0, --streams): exactly
    /// N=connections worker tasks (each its own connection, one in-flight
    /// append) issue PLAIN appends — no producer headers, no seq, no server-side
    /// session/dedup state. The domain is partitioned DISJOINTLY: pod i of P
    /// (DS_BENCH_INSTANCE / DS_BENCH_SHARDS) owns [i·N/P, (i+1)·N/P), and each
    /// worker round-robins a disjoint sub-slice of the pod's slice — every key
    /// is covered evenly and no two pods (or workers) ever contend on the same
    /// stream's appender lock. Each pod creates its own slice in a setup phase
    /// BEFORE the fleet barrier, so the measure window contains appends only
    /// (a 404→create→retry fallback remains for robustness and is counted in
    /// `lazy_creates` — nonzero values mean setup didn't do its job).
    #[arg(long, default_value_t = 0)]
    pub connections: usize,

    /// Records per append request (pool model only). 1 = one record per POST. >1 =
    /// send a JSON array of N records in a single POST (the durable server flattens
    /// the array into N records under ONE appender-lock + ONE fsync), amortizing the
    /// per-request client/server overhead; throughput counts N records per
    /// successful POST. Cuts fleet vCPU per record/s — the main load-gen cost
    /// lever. Switches the body to application/json.
    #[arg(long, default_value_t = 1)]
    pub batch: usize,

    /// Wall-clock duration to drive load, in seconds.
    #[arg(long, default_value_t = 60)]
    pub duration_secs: u64,

    /// Payload size in bytes per append.
    #[arg(long, default_value_t = 256)]
    pub payload_bytes: usize,

    /// Target appends per second per stream. 0 = as fast as possible.
    #[arg(long, default_value_t = 0)]
    pub rate_per_stream: u64,

    /// Concurrent stream-creation calls during setup.
    #[arg(long, default_value_t = 256)]
    pub setup_concurrency: usize,

    /// HTTP request timeout in seconds.
    #[arg(long, default_value_t = 30)]
    pub request_timeout_secs: u64,

    /// Warm-up seconds: drive load (advancing the producer session, warming the
    /// server's caches/allocator/WAL) but DO NOT count these ops. 0 = disabled.
    #[arg(long, default_value_t = 0)]
    pub warmup_secs: u64,

    /// Settle/wait seconds: after warm-up, go idle so the create+warm-up burst
    /// quiesces before the measured window starts. 0 = disabled.
    #[arg(long, default_value_t = 0)]
    pub settle_secs: u64,
}

#[derive(Serialize)]
pub struct MultiStreamResult {
    pub scenario: &'static str,
    pub api_style: ApiStyle,
    pub target: String,
    pub bucket: String,
    pub basin: String,
    pub streams: usize,
    pub duration_secs: u64,
    pub payload_bytes: usize,
    pub rate_per_stream: u64,
    pub elapsed_secs: f64,
    pub counts: Counts,
    pub errors: Vec<ErrorCount>,
    pub aggregate_ops_per_sec: f64,
    pub per_stream_ops_per_sec_mean: f64,
    pub latency_ms: LatencySummary,
    /// Wall-clock bounds of this pod's measure window (unix ms). The fleet sum of
    /// `aggregate_ops_per_sec` only means something when every pod's window covers
    /// the same wall time — hdr-merge uses these stamps to verify the windows
    /// actually overlapped (staggered pod starts otherwise multiply-count the
    /// server's capacity: the 500k-stream 2.9M ops/s artifact).
    pub measure_start_unix_ms: u64,
    pub measure_end_unix_ms: u64,
    /// Successful appends across ALL phases (setup-retry, warmup, settle spillover,
    /// measure) — NOT rate-limited to the measure window. Lets a verifier compare
    /// the fleet's total client-observed appends against server-side truth
    /// (sum of per-stream record counts).
    pub ok_total_all_phases: u64,
    /// Pool model: streams created via the in-loop 404→create→retry fallback.
    /// Should be ~0 — the pod's slice is pre-created during setup, so a large
    /// value means creation leaked into the load phases (measurement suspect).
    pub lazy_creates: u64,
    /// Pool model: this pod's disjoint slice of the global key domain.
    pub pod_slice_lo: usize,
    pub pod_slice_hi: usize,
}

/// Per-pod stream-name prefix for the LEGACY model: its per-stream producer
/// sessions require pods to own disjoint stream sets, or pods collide on
/// producer identities and a multi-pod cell's real cardinality silently shrinks
/// to streams/pods. The indexed Job sets DS_BENCH_INSTANCE = pod ordinal;
/// single-process runs default to "0". The pool model does NOT use this: its
/// key domain is global by design (any pod writes any key).
fn instance_prefix() -> String {
    let inst = std::env::var("DS_BENCH_INSTANCE").unwrap_or_default();
    let inst = if inst.is_empty() { "0".to_string() } else { inst };
    format!("i{inst}-")
}

/// Global (pod-independent) stream name for the pool model's shared key domain.
fn stream_name_global(idx: usize) -> String {
    format!("s{idx:08}")
}

/// Proportional split of `[lo, hi)` into `parts` contiguous ranges; returns part
/// `i`. Ranges are disjoint, cover the input exactly, and differ in size by at
/// most 1 — the primitive behind both the pod-level and worker-level key-domain
/// partitioning (even coverage, no overlap).
fn split_range(lo: usize, hi: usize, parts: usize, i: usize) -> (usize, usize) {
    let n = hi - lo;
    let parts = parts.max(1);
    (lo + i * n / parts, lo + (i + 1) * n / parts)
}

/// This pod's disjoint slice of the global `[0, domain)` key space, from the
/// indexed-Job env (DS_BENCH_INSTANCE = pod ordinal, DS_BENCH_SHARDS = pod
/// count). Single-process runs (no env) default to instance 0 of 1 = the whole
/// domain.
fn pod_slice(domain: usize) -> (usize, usize) {
    let inst: usize = std::env::var("DS_BENCH_INSTANCE").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let shards: usize = std::env::var("DS_BENCH_SHARDS").ok().and_then(|s| s.parse().ok()).unwrap_or(1).max(1);
    let inst = inst.min(shards - 1);
    split_range(0, domain, shards, inst)
}

/// Planned wall-clock measure window, computed at phase setup: `Instant`-based
/// phase arithmetic mapped onto the wall clock. Drift between the monotonic and
/// wall clocks over a bench run is negligible for the overlap check this feeds.
fn wall_measure_window(warmup_secs: u64, settle_secs: u64, duration_secs: u64) -> (u64, u64) {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let start = now_ms + (warmup_secs + settle_secs) * 1000;
    (start, start + duration_secs * 1000)
}

#[derive(Clone, Debug, Serialize)]
pub struct ErrorCount {
    pub error: String,
    pub count: u64,
}

pub async fn run(args: MultiStreamArgs) -> Result<MultiStreamResult> {
    let client = build_client(args.request_timeout_secs)?;
    let backend = Backend::new(
        args.api_style,
        &args.target,
        &args.bucket,
        &args.basin,
        client,
    );

    tracing::info!(
        "creating namespace and streams: api={} streams={} targets={}",
        args.api_style.as_str(),
        args.streams,
        backend.bases.len()
    );
    backend.ensure_namespace().await?;

    if args.connections > 0 {
        // Pool model: pods own DISJOINT slices of the global [0, --streams)
        // domain. Setup creates this pod's slice up front (bounded by
        // --setup-concurrency) so the load phases contain appends only —
        // creating streams inside warmup/measure both distorts the measured
        // append path and (at high cardinality) can dominate the whole window.
        // The fleet barrier comes AFTER setup: pods signal ready only once
        // their slice exists, and the whole fleet starts measuring together.
        let (lo, hi) = pod_slice(args.streams.max(1));
        let content_type = if args.batch.max(1) > 1 { "application/json" } else { "application/octet-stream" };
        tracing::info!(
            "pool setup: creating pod slice [{lo}, {hi}) of {} global streams",
            args.streams
        );
        create_stream_range(&backend, lo, hi, args.setup_concurrency, content_type).await?;
        crate::barrier::sync_to_fleet_start().await;
        return run_pool(args, backend, lo, hi).await;
    }

    create_streams(&backend, args.streams, args.setup_concurrency, "application/octet-stream")
        .await?;

    // Fleet start barrier (no-op unless DS_BENCH_BARRIER_DIR is set): hold here —
    // AFTER setup, BEFORE any load phase — until every pod is ready and the
    // leader's go time arrives, so all measure windows cover the same wall time.
    crate::barrier::sync_to_fleet_start().await;

    // S2 embeds the body in a JSON string (from_utf8_lossy) — printable text keeps
    // the record length == payload_bytes; durable/ursula take raw octet bytes.
    let payload = Arc::new(if backend.kind == ApiStyle::S2 {
        fill_payload_text(args.payload_bytes, 0xC0FFEE)
    } else {
        fill_payload(args.payload_bytes, 0xC0FFEE)
    });
    let ok = Arc::new(AtomicU64::new(0));
    let ok_all = Arc::new(AtomicU64::new(0));
    let bp = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(Mutex::new(BTreeMap::<String, u64>::new()));
    let hist = Arc::new(Mutex::new(new_histogram()));

    // Three phases on ONE continuous producer session (seq advances throughout,
    // so no dedup collisions): warm-up (uncounted) → settle (idle) → measure (counted).
    let base = Instant::now();
    let warmup_end = base + Duration::from_secs(args.warmup_secs);
    let measure_start = warmup_end + Duration::from_secs(args.settle_secs);
    let deadline = measure_start + Duration::from_secs(args.duration_secs);
    let (measure_start_unix_ms, measure_end_unix_ms) =
        wall_measure_window(args.warmup_secs, args.settle_secs, args.duration_secs);

    let mut workers = Vec::with_capacity(args.streams);
    for idx in 0..args.streams {
        let backend = backend.clone();
        let stream = stream_name(idx);
        let payload = payload.clone();
        let ok = ok.clone();
        let ok_all = ok_all.clone();
        let bp = bp.clone();
        let err = err.clone();
        let errors = errors.clone();
        let hist = hist.clone();
        let rate = args.rate_per_stream;
        let producer_id = format!("bench-{idx}");
        workers.push(tokio::spawn(async move {
            run_writer(
                backend,
                idx,
                stream,
                payload,
                producer_id,
                rate,
                warmup_end,
                measure_start,
                deadline,
                ok,
                ok_all,
                bp,
                err,
                errors,
                hist,
            )
            .await
        }));
    }

    for w in workers {
        let _ = w.await;
    }

    let counts = Counts {
        ok: ok.load(Ordering::Relaxed),
        backpressure: bp.load(Ordering::Relaxed),
        other_err: err.load(Ordering::Relaxed),
    };
    let errors = errors
        .lock()
        .await
        .iter()
        .map(|(error, count)| ErrorCount {
            error: error.clone(),
            count: *count,
        })
        .collect();
    let h = hist.lock().await;
    let latency = summarize(&h);
    crate::dist::emit_hdr(&h, &format!("multi-stream-{}", std::process::id()));
    let elapsed_secs = args.duration_secs as f64; // throughput over the MEASURE window only
    let aggregate = counts.ok as f64 / elapsed_secs.max(1e-9);
    let per_stream_mean = aggregate / args.streams.max(1) as f64;

    Ok(MultiStreamResult {
        scenario: "multi-stream-write",
        api_style: args.api_style,
        target: args.target,
        bucket: args.bucket,
        basin: args.basin,
        streams: args.streams,
        duration_secs: args.duration_secs,
        payload_bytes: args.payload_bytes,
        rate_per_stream: args.rate_per_stream,
        elapsed_secs,
        counts,
        errors,
        aggregate_ops_per_sec: aggregate,
        per_stream_ops_per_sec_mean: per_stream_mean,
        latency_ms: latency,
        measure_start_unix_ms,
        measure_end_unix_ms,
        ok_total_all_phases: ok_all.load(Ordering::Relaxed),
        lazy_creates: 0,
        pod_slice_lo: 0,
        pod_slice_hi: args.streams,
    })
}

/// Bounded-concurrency pool model: exactly `connections` worker tasks, each
/// cycling plain appends round-robin over a disjoint sub-slice of this pod's
/// `[lo, hi)` slice of the global key domain, one in-flight append at a time.
/// Offered concurrency is `connections` (NOT `streams`), so the load the server
/// sees is controlled and the client's per-pod overhead/memory stays bounded
/// regardless of stream count. Round-robin over disjoint slices ⇒ every key is
/// hit evenly and no two workers/pods share a stream (no appender-lock
/// interference between load generators).
async fn run_pool(
    args: MultiStreamArgs,
    backend: Backend,
    lo: usize,
    hi: usize,
) -> Result<MultiStreamResult> {
    let n = args.streams.max(1);
    let c = args.connections.max(1);
    let batch = args.batch.max(1);
    // Precompute the request body once (constant across appends). batch>1 → a JSON
    // array of `batch` records (server flattens to N records under one lock/fsync).
    let (body, content_type, recs_per_post): (Arc<Vec<u8>>, &'static str, u64) = if batch > 1 {
        // High-entropy printable content (not "x".repeat(n)): an all-'x' record is
        // artificially compressible, so any server that compresses the WAL/body
        // would post a misleadingly high batch ceiling. Each of the N records is
        // seeded distinctly so the body is incompressible ACROSS records too (a
        // repeated identical record would still LZ-collapse). Each is payload_bytes.
        let per_rec = args.payload_bytes + 3; // quotes + comma
        let mut s = String::with_capacity(per_rec * batch + 2);
        s.push('[');
        for i in 0..batch {
            if i > 0 {
                s.push(',');
            }
            let rec_bytes = fill_payload_text(args.payload_bytes, 0xC0FFEE ^ i as u64);
            s.push('"');
            s.push_str(&String::from_utf8_lossy(&rec_bytes));
            s.push('"');
        }
        s.push(']');
        (Arc::new(s.into_bytes()), "application/json", batch as u64)
    } else {
        // S2 wraps the body in a JSON string via from_utf8_lossy — random bytes
        // would be re-encoded to a DIFFERENT length. Give S2 printable text of
        // exactly payload_bytes; durable/ursula take raw octet-stream bytes.
        let raw = if backend.kind == ApiStyle::S2 {
            fill_payload_text(args.payload_bytes, 0xC0FFEE)
        } else {
            fill_payload(args.payload_bytes, 0xC0FFEE)
        };
        (Arc::new(raw), "application/octet-stream", 1)
    };
    let ok = Arc::new(AtomicU64::new(0));
    let ok_all = Arc::new(AtomicU64::new(0));
    let lazy_creates = Arc::new(AtomicU64::new(0));
    let bp = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(Mutex::new(BTreeMap::<String, u64>::new()));
    let hist = Arc::new(Mutex::new(new_histogram()));

    let base = Instant::now();
    let warmup_end = base + Duration::from_secs(args.warmup_secs);
    let measure_start = warmup_end + Duration::from_secs(args.settle_secs);
    let deadline = measure_start + Duration::from_secs(args.duration_secs);
    let (measure_start_unix_ms, measure_end_unix_ms) =
        wall_measure_window(args.warmup_secs, args.settle_secs, args.duration_secs);

    tracing::info!(
        "pool model: connections={c} round-robin over pod slice [{lo}, {hi}) of {n} global streams, batch={batch}"
    );

    let mut workers = Vec::with_capacity(c);
    for w in 0..c {
        let backend = backend.clone();
        let body = body.clone();
        // Worker w owns a disjoint sub-slice of the pod's slice. When the pod
        // slice has fewer streams than workers (perpod < connections) the split
        // yields empty ranges — those workers fall back to cycling the whole pod
        // slice from a staggered start (intra-pod sharing; keep perpod ≥
        // connections in suites to avoid it).
        let (wlo, whi) = split_range(lo, hi, c, w);
        let (wlo, whi, phase) = if wlo == whi { (lo, hi, w) } else { (wlo, whi, 0) };
        let (ok, ok_all, lazy_creates, bp, err, errors, hist) = (
            ok.clone(), ok_all.clone(), lazy_creates.clone(), bp.clone(), err.clone(),
            errors.clone(), hist.clone(),
        );
        workers.push(tokio::spawn(async move {
            pool_worker(
                backend, wlo, whi, phase, body, content_type, recs_per_post, warmup_end,
                measure_start, deadline, ok, ok_all, lazy_creates, bp, err, errors, hist,
            )
            .await
        }));
    }
    for wk in workers {
        let _ = wk.await;
    }

    let counts = Counts {
        ok: ok.load(Ordering::Relaxed),
        backpressure: bp.load(Ordering::Relaxed),
        other_err: err.load(Ordering::Relaxed),
    };
    let errors = errors
        .lock()
        .await
        .iter()
        .map(|(error, count)| ErrorCount { error: error.clone(), count: *count })
        .collect();
    let h = hist.lock().await;
    let latency = summarize(&h);
    crate::dist::emit_hdr(&h, &format!("multi-stream-{}", std::process::id()));
    let elapsed_secs = args.duration_secs as f64;
    let aggregate = counts.ok as f64 / elapsed_secs.max(1e-9);
    let per_stream_mean = aggregate / n as f64;

    Ok(MultiStreamResult {
        scenario: "multi-stream-pool-write",
        api_style: args.api_style,
        target: args.target,
        bucket: args.bucket,
        basin: args.basin,
        streams: n,
        duration_secs: args.duration_secs,
        payload_bytes: args.payload_bytes,
        rate_per_stream: 0,
        elapsed_secs,
        counts,
        errors,
        aggregate_ops_per_sec: aggregate,
        per_stream_ops_per_sec_mean: per_stream_mean,
        latency_ms: latency,
        measure_start_unix_ms,
        measure_end_unix_ms,
        ok_total_all_phases: ok_all.load(Ordering::Relaxed),
        lazy_creates: lazy_creates.load(Ordering::Relaxed),
        pod_slice_lo: lo,
        pod_slice_hi: hi,
    })
}

#[allow(clippy::too_many_arguments)]
async fn pool_worker(
    backend: Backend,
    lo: usize,
    hi: usize,
    phase: usize,
    body: Arc<Vec<u8>>,
    content_type: &'static str,
    recs_per_post: u64,
    warmup_end: Instant,
    measure_start: Instant,
    deadline: Instant,
    ok: Arc<AtomicU64>,
    ok_all: Arc<AtomicU64>,
    lazy_creates: Arc<AtomicU64>,
    bp: Arc<AtomicU64>,
    err: Arc<AtomicU64>,
    errors: Arc<Mutex<BTreeMap<String, u64>>>,
    hist: Arc<Mutex<Histogram<u64>>>,
) {
    let span = hi.saturating_sub(lo).max(1);
    let mut rr = phase % span; // round-robin cursor within [lo, hi)
    let mut local = new_histogram();
    while Instant::now() < deadline {
        let now = Instant::now();
        if now >= warmup_end && now < measure_start {
            tokio::time::sleep(measure_start.saturating_duration_since(now)).await;
            continue;
        }
        let counting = now >= measure_start;
        // Round-robin over this worker's disjoint slice: even key coverage by
        // construction, and no other worker/pod ever touches these streams.
        let global = lo + rr;
        rr = (rr + 1) % span;
        let stream = stream_name_global(global);
        let started = Instant::now();
        // RAW throughput: plain append, no producer session/seq/dedup — the
        // measurement is the server's append path, not its idempotency layer.
        let resp = backend
            .append_request(global, &stream, &body, None, content_type)
            .send()
            .await;
        match resp {
            Ok(r) => {
                let status = r.status();
                if status.is_success() {
                    ok_all.fetch_add(recs_per_post, Ordering::Relaxed);
                    if counting {
                        ok.fetch_add(recs_per_post, Ordering::Relaxed);
                        record(&mut local, started);
                    }
                } else if status.as_u16() == 404 {
                    // Fallback only: setup pre-created the slice, so a 404 means
                    // lost server state or a setup gap. Create (tolerate-exists)
                    // and retry once; the recorded latency includes the create —
                    // the honest client-observed cost. lazy_creates makes any
                    // leak of creation into the load phases visible in the JSON.
                    lazy_creates.fetch_add(1, Ordering::Relaxed);
                    if backend.create_stream(&stream, content_type).await.is_ok() {
                        let retry = backend
                            .append_request(global, &stream, &body, None, content_type)
                            .send()
                            .await;
                        match retry {
                            Ok(r2) if r2.status().is_success() => {
                                ok_all.fetch_add(recs_per_post, Ordering::Relaxed);
                                if counting {
                                    ok.fetch_add(recs_per_post, Ordering::Relaxed);
                                    record(&mut local, started);
                                }
                            }
                            Ok(r2) => {
                                if counting {
                                    err.fetch_add(1, Ordering::Relaxed);
                                    record_error(
                                        &errors,
                                        format!("http_status_{}_after_create", r2.status().as_u16()),
                                    )
                                    .await;
                                }
                            }
                            Err(e) => {
                                if counting {
                                    err.fetch_add(1, Ordering::Relaxed);
                                    record_error(&errors, reqwest_error_chain(&e)).await;
                                }
                            }
                        }
                    } else if counting {
                        err.fetch_add(1, Ordering::Relaxed);
                        record_error(&errors, "create_failed".to_string()).await;
                    }
                } else if status.as_u16() == 503 || status.as_u16() == 429 {
                    if counting {
                        bp.fetch_add(1, Ordering::Relaxed);
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                } else if counting {
                    err.fetch_add(1, Ordering::Relaxed);
                    record_error(&errors, format!("http_status_{}", status.as_u16())).await;
                }
            }
            Err(e) => {
                if counting {
                    err.fetch_add(1, Ordering::Relaxed);
                    record_error(&errors, reqwest_error_chain(&e)).await;
                }
            }
        }
    }
    let mut h = hist.lock().await;
    merge(&mut h, &local);
}

#[allow(clippy::too_many_arguments)]
async fn run_writer(
    backend: Backend,
    base_idx: usize,
    stream: String,
    payload: Arc<Vec<u8>>,
    producer_id: String,
    rate_per_stream: u64,
    warmup_end: Instant,
    measure_start: Instant,
    deadline: Instant,
    ok: Arc<AtomicU64>,
    ok_all: Arc<AtomicU64>,
    bp: Arc<AtomicU64>,
    err: Arc<AtomicU64>,
    errors: Arc<Mutex<BTreeMap<String, u64>>>,
    hist: Arc<Mutex<Histogram<u64>>>,
) {
    let epoch: u64 = 0;
    let mut seq: u64 = 0;
    let interval = if rate_per_stream > 0 {
        Some(Duration::from_micros(1_000_000 / rate_per_stream.max(1)))
    } else {
        None
    };
    let mut next_at = Instant::now();
    let mut local = new_histogram();
    let use_producer = matches!(backend.kind, ApiStyle::Ursula | ApiStyle::Durable);
    while Instant::now() < deadline {
        let now = Instant::now();
        // SETTLE/WAIT phase: idle between warm-up and the measure window so the
        // create + warm-up burst quiesces before we start counting.
        if now >= warmup_end && now < measure_start {
            tokio::time::sleep(measure_start.saturating_duration_since(now)).await;
            continue;
        }
        // Count only in the measure window; warm-up still appends (advancing seq +
        // warming the server) but is not counted.
        let counting = now >= measure_start;
        if let Some(iv) = interval {
            let now = Instant::now();
            if now < next_at {
                tokio::time::sleep(next_at - now).await;
            }
            next_at += iv;
        }
        let started = Instant::now();
        let producer = if use_producer {
            Some(Producer {
                id: &producer_id,
                epoch,
                seq,
            })
        } else {
            None
        };
        let resp = backend
            .append_request(
                base_idx,
                &stream,
                &payload,
                producer,
                "application/octet-stream",
            )
            .send()
            .await;
        match resp {
            Ok(r) => {
                let status = r.status();
                if status.is_success() {
                    ok_all.fetch_add(1, Ordering::Relaxed);
                    if counting {
                        ok.fetch_add(1, Ordering::Relaxed);
                        record(&mut local, started);
                    }
                    seq += 1; // advance in BOTH warm-up and measure → one continuous session
                } else if status.as_u16() == 503 || status.as_u16() == 429 {
                    if counting {
                        bp.fetch_add(1, Ordering::Relaxed);
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                } else if counting {
                    err.fetch_add(1, Ordering::Relaxed);
                    record_error(&errors, format!("http_status_{}", status.as_u16())).await;
                }
            }
            Err(e) => {
                if counting {
                    err.fetch_add(1, Ordering::Relaxed);
                    record_error(&errors, reqwest_error_chain(&e)).await;
                }
            }
        }
    }
    let mut h = hist.lock().await;
    merge(&mut h, &local);
}

async fn record_error(errors: &Mutex<BTreeMap<String, u64>>, error: String) {
    let mut errors = errors.lock().await;
    *errors.entry(error).or_default() += 1;
}

fn reqwest_error_chain(error: &reqwest::Error) -> String {
    let mut parts = Vec::new();
    let mut source = error.source();
    while let Some(err) = source {
        parts.push(err.to_string());
        source = err.source();
    }
    if parts.is_empty() {
        error.to_string()
    } else {
        parts.join(" | caused by: ")
    }
}

async fn create_streams(
    backend: &Backend,
    count: usize,
    concurrency: usize,
    content_type: &'static str,
) -> Result<()> {
    create_streams_named(backend, 0, count, concurrency, content_type, stream_name).await
}

/// Pool-model setup: create the GLOBAL-named streams of this pod's disjoint
/// slice `[lo, hi)`, `concurrency` creates in flight. Create tolerates
/// already-exists, so re-runs (ladder rungs against un-reset state) are cheap.
async fn create_stream_range(
    backend: &Backend,
    lo: usize,
    hi: usize,
    concurrency: usize,
    content_type: &'static str,
) -> Result<()> {
    create_streams_named(backend, lo, hi, concurrency, content_type, stream_name_global).await
}

async fn create_streams_named(
    backend: &Backend,
    lo: usize,
    hi: usize,
    concurrency: usize,
    content_type: &'static str,
    name: fn(usize) -> String,
) -> Result<()> {
    let mut pending: FuturesUnordered<_> = FuturesUnordered::new();
    let mut next = lo;
    let max = concurrency.max(1);
    let push_one = |i: usize, pending: &mut FuturesUnordered<_>| {
        let backend = backend.clone();
        let stream = name(i);
        pending.push(tokio::spawn(async move {
            backend
                .create_stream(&stream, content_type)
                .await
        }));
    };
    while next < hi && pending.len() < max {
        push_one(next, &mut pending);
        next += 1;
    }
    while let Some(joined) = pending.next().await {
        joined??;
        if next < hi {
            push_one(next, &mut pending);
            next += 1;
        }
    }
    Ok(())
}

fn stream_name(idx: usize) -> String {
    // Pod-namespaced: pods own disjoint stream sets (see instance_prefix).
    use std::sync::OnceLock;
    static PREFIX: OnceLock<String> = OnceLock::new();
    let prefix = PREFIX.get_or_init(instance_prefix);
    format!("{prefix}s{idx:08}")
}

#[cfg(test)]
mod tests {
    use super::split_range;

    /// Pod-level partition: the P slices of [0, N) are disjoint, contiguous,
    /// cover the whole domain exactly, and are balanced to within one key —
    /// the properties that give even key-space use with zero cross-pod
    /// conflicts.
    #[test]
    fn pod_slices_are_disjoint_covering_and_balanced() {
        for (n, p) in [(10, 3), (500_000, 7), (100, 100), (5, 8), (1, 1)] {
            let mut covered = 0usize;
            let mut prev_hi = 0usize;
            let (mut min_span, mut max_span) = (usize::MAX, 0usize);
            for i in 0..p {
                let (lo, hi) = split_range(0, n, p, i);
                assert_eq!(lo, prev_hi, "slices must be contiguous (n={n} p={p} i={i})");
                prev_hi = hi;
                covered += hi - lo;
                min_span = min_span.min(hi - lo);
                max_span = max_span.max(hi - lo);
            }
            assert_eq!(prev_hi, n, "last slice must end at the domain (n={n} p={p})");
            assert_eq!(covered, n, "slices must cover the domain exactly");
            assert!(max_span - min_span <= 1, "balanced to within 1 (n={n} p={p})");
        }
    }

    /// Worker-level partition nests inside the pod slice: sub-slices are
    /// disjoint and cover exactly the pod's [lo, hi) — no two workers of a pod
    /// share a stream when the slice has at least one key per worker.
    #[test]
    fn worker_slices_nest_inside_pod_slice() {
        let (lo, hi) = split_range(0, 100_000, 7, 3); // an arbitrary pod slice
        let c = 256;
        let mut prev = lo;
        for w in 0..c {
            let (wlo, whi) = split_range(lo, hi, c, w);
            assert_eq!(wlo, prev);
            prev = whi;
        }
        assert_eq!(prev, hi);
    }
}
