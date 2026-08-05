use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use anyhow::Result;
use clap::Args;
use serde::Serialize;
use tokio::sync::Barrier;
use tokio::sync::Mutex;

use crate::backend::ApiStyle;
use crate::backend::Backend;
use crate::common::Counts;
use crate::common::LatencySummary;
use crate::common::build_client;
use crate::common::fill_payload;
use crate::common::new_histogram;
use crate::common::record_micros;
use crate::common::summarize;

#[derive(Args, Debug, Clone)]
pub struct CatchUpArgs {
    #[arg(long)]
    pub target: String,

    #[arg(long, value_enum, default_value_t = ApiStyle::Ursula)]
    pub api_style: ApiStyle,

    #[arg(long, default_value = "bench-catchup")]
    pub bucket: String,

    #[arg(long, default_value = "benchmark")]
    pub basin: String,

    #[arg(long, default_value = "doc")]
    pub stream: String,

    #[arg(long, default_value_t = 200)]
    pub clients: usize,

    #[arg(long, default_value_t = 2000)]
    pub pre_events: usize,

    #[arg(long, default_value_t = 1024)]
    pub event_bytes: usize,

    #[arg(long, default_value_t = 32)]
    pub setup_concurrency: usize,

    #[arg(long, default_value_t = 120)]
    pub request_timeout_secs: u64,
}

#[derive(Serialize)]
pub struct CatchUpResult {
    pub scenario: &'static str,
    pub api_style: ApiStyle,
    pub target: String,
    pub bucket: String,
    pub stream: String,
    pub clients: usize,
    pub pre_events: usize,
    pub event_bytes: usize,
    pub pre_bytes_total: u64,
    pub setup_elapsed_secs: f64,
    pub stampede_elapsed_secs: f64,
    pub counts: Counts,
    pub bytes_received_total: u64,
    pub aggregate_mb_per_sec: f64,
    pub latency_ms: LatencySummary,
}

fn catch_up_url(b: &Backend, base_idx: usize, stream: &str, offset: &str) -> String {
    let base = b.base_for(base_idx);
    match b.kind {
        ApiStyle::Durable => format!("{base}/v1/stream/{stream}?offset={offset}"),
        ApiStyle::Ursula => format!("{base}/{}/{stream}?offset={offset}", b.bucket),
        ApiStyle::S2 => unreachable!("S2 is excluded from catch-up workload"),
    }
}

fn catch_up_start(b: &Backend) -> &'static str {
    // durable & ursula (DS-protocol) accept -1 = start
    match b.kind {
        ApiStyle::Durable | ApiStyle::Ursula => "-1",
        ApiStyle::S2 => unreachable!("S2 is excluded from catch-up workload"),
    }
}

/// Loop catch-up GETs, feeding back stream-next-offset, until stream-up-to-date.
/// Returns total bytes read. Offset is opaque (format-agnostic across servers).
pub(crate) async fn catch_up_read_all(b: &Backend, base_idx: usize, stream: &str) -> anyhow::Result<u64> {
    use anyhow::Context;
    let mut offset = catch_up_start(b).to_string();
    let mut total: u64 = 0;
    let mut guard: u32 = 0;
    loop {
        let url = catch_up_url(b, base_idx, stream, &offset);
        let resp = b.client.get(&url).send().await.context("catch-up GET")?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("catch-up status {status}");
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
        // Stream the body to count bytes without holding the whole (up to
        // full-stream) response resident — the per-reader OOM at high fan-out.
        total += crate::common::drain_len(resp).await.context("catch-up body")?;
        guard += 1;
        match next {
            Some(n) if !up && n != offset && guard < 100_000 => offset = n,
            _ => break, // up-to-date, no next, offset stalled, or guard hit
        }
    }
    Ok(total)
}

pub async fn run(args: CatchUpArgs) -> Result<CatchUpResult> {
    if args.api_style == ApiStyle::S2 {
        anyhow::bail!(
            "catch-up workload is not supported for S2 Lite: its paginated, \
             JSON-enveloped seq_num read is not comparable to the Durable Streams \
             full-replay loop"
        );
    }
    let client = build_client(args.request_timeout_secs)?;
    let backend = Backend::new(
        args.api_style,
        &args.target,
        &args.bucket,
        &args.basin,
        client,
    );

    backend.ensure_namespace().await?;
    let _ = backend.delete_stream(&args.stream).await;
    backend
        .create_stream(&args.stream, "application/octet-stream")
        .await?;

    // SETUP: append pre_events payloads
    let setup_start = Instant::now();
    let payload = Arc::new(fill_payload(args.event_bytes, 0xBEEF));
    let pending = Arc::new(tokio::sync::Semaphore::new(args.setup_concurrency.max(1)));
    let pre_bytes_total = (args.pre_events as u64) * (args.event_bytes as u64);
    let setup_ok = Arc::new(AtomicU64::new(0));

    let mut joins = Vec::with_capacity(args.pre_events);
    for _seq in 0..args.pre_events {
        let permit = pending.clone().acquire_owned().await.unwrap();
        let backend = backend.clone();
        let stream = args.stream.clone();
        let payload = payload.clone();
        let setup_ok = setup_ok.clone();
        joins.push(tokio::spawn(async move {
            let _permit = permit;
            // Use None producer: concurrent setup appends are incompatible with
            // producer-seq (server enforces strict ordering and returns 409 on gaps).
            // Matches bootstrap.rs pattern.
            if backend
                .append_request(0, &stream, &payload, None, "application/octet-stream")
                .send()
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false)
            {
                setup_ok.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for j in joins {
        let _ = j.await;
    }
    let setup_elapsed = setup_start.elapsed();

    let landed = setup_ok.load(Ordering::Relaxed);
    if landed < args.pre_events as u64 {
        anyhow::bail!(
            "setup failed: only {landed}/{} appends succeeded — \
             check that the target is reachable and the stream was created",
            args.pre_events
        );
    }

    // STAMPEDE: clients all read from the start simultaneously
    let barrier = Arc::new(Barrier::new(args.clients));
    let ok = Arc::new(AtomicU64::new(0));
    let bp = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let bytes_total = Arc::new(AtomicU64::new(0));
    let hist = Arc::new(Mutex::new(new_histogram()));

    let mut handles = Vec::with_capacity(args.clients);
    for idx in 0..args.clients {
        let backend = backend.clone();
        let stream = args.stream.clone();
        let barrier = barrier.clone();
        let ok = ok.clone();
        let bp = bp.clone();
        let err = err.clone();
        let bytes_total = bytes_total.clone();
        let hist = hist.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let t = Instant::now();
            let result = catch_up_read_all(&backend, idx, &stream).await;
            match result {
                Ok(bytes) => {
                    // Measure elapsed BEFORE contending for the shared histogram
                    // lock: in the stampede all clients finish near-simultaneously
                    // and queue on this Mutex, and folding that lock-wait into the
                    // sample inflated the per-client catch-up tail (not server time).
                    let dur = t.elapsed();
                    ok.fetch_add(1, Ordering::Relaxed);
                    bytes_total.fetch_add(bytes, Ordering::Relaxed);
                    let mut h = hist.lock().await;
                    record_micros(&mut h, dur);
                }
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("503") || msg.contains("429") {
                        bp.fetch_add(1, Ordering::Relaxed);
                    } else {
                        err.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    let stampede_start = Instant::now();
    for h in handles {
        let _ = h.await;
    }
    let stampede_elapsed = stampede_start.elapsed();

    let h = hist.lock().await;
    let latency = summarize(&h);
    crate::dist::emit_hdr(&h, &format!("catch-up-{}", std::process::id()));

    let bytes_received_total = bytes_total.load(Ordering::Relaxed);
    let stampede_elapsed_secs = stampede_elapsed.as_secs_f64();
    let aggregate_mb_per_sec =
        bytes_received_total as f64 / stampede_elapsed_secs / 1_048_576.0;

    Ok(CatchUpResult {
        scenario: "catch-up-stampede",
        api_style: args.api_style,
        target: args.target,
        bucket: args.bucket,
        stream: args.stream,
        clients: args.clients,
        pre_events: args.pre_events,
        event_bytes: args.event_bytes,
        pre_bytes_total,
        setup_elapsed_secs: setup_elapsed.as_secs_f64(),
        stampede_elapsed_secs,
        counts: Counts {
            ok: ok.load(Ordering::Relaxed),
            backpressure: bp.load(Ordering::Relaxed),
            other_err: err.load(Ordering::Relaxed),
        },
        bytes_received_total,
        aggregate_mb_per_sec,
        latency_ms: latency,
    })
}
