//! Steady-rate, long-duration workload swept over N streams.
//!
//! Each stream is driven at exactly `rate_per_stream` ops/sec using a
//! `tokio::time::interval` — the whole point is a *controlled*, stable offered
//! load rather than maximum throughput.  A snapshot task periodically prints a
//! latency-over-time series so we can see whether latency is stable over the
//! full `duration_secs` window.
//!
//! At the end, a mergeable HDR file is emitted (like multi_stream) and a JSON
//! summary is printed.

use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;
use clap::Args;
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
use crate::common::merge;
use crate::common::new_histogram;
use crate::common::record_micros;
use crate::common::scheduled_send;
use crate::common::summarize;
use crate::multi_stream::ErrorCount;

/// Args for the `sustained` subcommand.
#[derive(Args, Debug, Clone)]
pub struct SustainedArgs {
    /// Target base URL(s). Comma-separated for round-robin across nodes.
    #[arg(long)]
    pub target: String,

    /// Backend API style.
    #[arg(long, value_enum, default_value_t = ApiStyle::Ursula)]
    pub api_style: ApiStyle,

    /// Bucket name (Ursula only — ignored by Durable / S2).
    #[arg(long, default_value = "bench-sustained")]
    pub bucket: String,

    /// Basin name (S2 only).
    #[arg(long, default_value = "benchmark")]
    pub basin: String,

    /// Number of concurrent streams; one writer task per stream.
    #[arg(long, default_value_t = 10)]
    pub streams: usize,

    /// Steady offered load per stream in ops/sec.  Must be > 0.
    #[arg(long, default_value_t = 10)]
    pub rate_per_stream: u64,

    /// Total wall-clock duration in seconds (long — e.g. 300).
    #[arg(long, default_value_t = 300)]
    pub duration_secs: u64,

    /// Payload size in bytes per append.
    #[arg(long, default_value_t = 256)]
    pub payload_bytes: usize,

    /// How often (secs) to print a point-in-time snapshot line.
    #[arg(long, default_value_t = 5)]
    pub snapshot_secs: u64,

    /// Concurrent stream-creation calls during setup.
    #[arg(long, default_value_t = 64)]
    pub setup_concurrency: usize,

    /// HTTP request timeout in seconds.
    #[arg(long, default_value_t = 30)]
    pub request_timeout_secs: u64,
}

/// Final summary emitted as JSON to stdout.
#[derive(Serialize)]
pub struct SustainedResult {
    pub scenario: &'static str,
    pub api_style: ApiStyle,
    pub target: String,
    pub streams: usize,
    pub duration_secs: u64,
    pub rate_per_stream: u64,
    pub payload_bytes: usize,
    pub elapsed_secs: f64,
    pub counts: Counts,
    pub errors: Vec<ErrorCount>,
    pub aggregate_ops_per_sec: f64,
    pub latency_ms: LatencySummary,
}

pub async fn run(args: SustainedArgs) -> Result<SustainedResult> {
    anyhow::ensure!(args.rate_per_stream > 0, "--rate-per-stream must be > 0");

    let client = build_client(args.request_timeout_secs)?;
    let backend = Backend::new(
        args.api_style,
        &args.target,
        &args.bucket,
        &args.basin,
        client,
    );

    tracing::info!(
        "sustained: creating namespace and streams: api={} streams={} targets={}",
        args.api_style.as_str(),
        args.streams,
        backend.bases.len()
    );
    backend.ensure_namespace().await?;
    create_streams(&backend, args.streams, args.setup_concurrency).await?;

    let payload = Arc::new(fill_payload(args.payload_bytes, 0xDECAFBAD));
    let ok = Arc::new(AtomicU64::new(0));
    let bp = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(Mutex::new(BTreeMap::<String, u64>::new()));
    let hist = Arc::new(Mutex::new(new_histogram()));

    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);
    let start = Instant::now();

    // Spawn per-stream writer tasks.
    let mut workers = Vec::with_capacity(args.streams);
    for idx in 0..args.streams {
        let backend = backend.clone();
        let stream = stream_name(idx);
        let payload = payload.clone();
        let ok = ok.clone();
        let bp = bp.clone();
        let err = err.clone();
        let errors = errors.clone();
        let hist = hist.clone();
        let rate = args.rate_per_stream;
        let producer_id = format!("bench-sustained-{idx}");
        workers.push(tokio::spawn(async move {
            run_writer(
                backend,
                idx,
                stream,
                payload,
                producer_id,
                rate,
                deadline,
                ok,
                bp,
                err,
                errors,
                hist,
            )
            .await
        }));
    }

    // Snapshot task: every `snapshot_secs` print a point-in-time JSON line.
    let snapshot_hist = hist.clone();
    let snapshot_ok = ok.clone();
    let snapshot_interval = args.snapshot_secs;
    let snapshot_start = start;
    let snapshot_task = tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(Duration::from_secs(snapshot_interval.max(1)));
        interval.tick().await; // discard first immediate tick
        loop {
            interval.tick().await;
            let elapsed = snapshot_start.elapsed();
            let elapsed_s = elapsed.as_secs_f64();
            let ops = snapshot_ok.load(Ordering::Relaxed);
            let h = snapshot_hist.lock().await;
            if !h.is_empty() {
                let p50 = h.value_at_quantile(0.5) as f64 / 1000.0;
                let p99 = h.value_at_quantile(0.99) as f64 / 1000.0;
                // Print to stderr to keep stdout clean for the final JSON.
                eprintln!(
                    r#"{{"elapsed_s":{elapsed_s:.1},"ops":{ops},"p50_ms":{p50:.3},"p99_ms":{p99:.3}}}"#
                );
            }
        }
    });

    // Wait for all writers to finish, then cancel the snapshot task.
    for w in workers {
        let _ = w.await;
    }
    snapshot_task.abort();

    let elapsed = start.elapsed();
    let counts = Counts {
        ok: ok.load(Ordering::Relaxed),
        backpressure: bp.load(Ordering::Relaxed),
        other_err: err.load(Ordering::Relaxed),
    };
    let errors_vec = errors
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
    crate::dist::emit_hdr(&h, &format!("sustained-{}", std::process::id()));
    let elapsed_secs = elapsed.as_secs_f64();
    let aggregate = counts.ok as f64 / elapsed_secs.max(1e-9);

    Ok(SustainedResult {
        scenario: "sustained-write",
        api_style: args.api_style,
        target: args.target,
        streams: args.streams,
        duration_secs: args.duration_secs,
        rate_per_stream: args.rate_per_stream,
        payload_bytes: args.payload_bytes,
        elapsed_secs,
        counts,
        errors: errors_vec,
        aggregate_ops_per_sec: aggregate,
        latency_ms: latency,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_writer(
    backend: Backend,
    base_idx: usize,
    stream: String,
    payload: Arc<Vec<u8>>,
    producer_id: String,
    rate_per_stream: u64,
    deadline: Instant,
    ok: Arc<AtomicU64>,
    bp: Arc<AtomicU64>,
    err: Arc<AtomicU64>,
    errors: Arc<Mutex<BTreeMap<String, u64>>>,
    hist: Arc<Mutex<Histogram<u64>>>,
) {
    let epoch: u64 = 0;
    let mut seq: u64 = 0;
    let mut n: u64 = 0;
    // Open-loop pacing: the n-th record has a fixed DUE instant (base + n/rate). We
    // sleep until it, then measure latency from that scheduled time — NOT from when
    // the loop got around to issuing the request. Under `interval` + Delay the loop
    // silently fell behind when the server slowed and the overdue requests were
    // never timed (coordinated omission), so a degrading server read as
    // latency-stable — the exact property this workload claims to test.
    let base = Instant::now();
    let mut local = new_histogram();
    let use_producer = matches!(backend.kind, ApiStyle::Ursula | ApiStyle::Durable);
    loop {
        let scheduled = scheduled_send(base, n, rate_per_stream);
        n += 1;
        let now = Instant::now();
        if now < scheduled {
            tokio::time::sleep(scheduled - now).await;
        }
        if Instant::now() >= deadline {
            break;
        }
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
                    ok.fetch_add(1, Ordering::Relaxed);
                    // Latency from the SCHEDULED time: overdue records (server
                    // behind the offered rate) record their full queueing delay.
                    record_micros(&mut local, scheduled.elapsed());
                    seq += 1;
                } else if status.as_u16() == 503 || status.as_u16() == 429 {
                    bp.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                } else {
                    err.fetch_add(1, Ordering::Relaxed);
                    record_error(&errors, format!("http_status_{}", status.as_u16())).await;
                }
            }
            Err(e) => {
                err.fetch_add(1, Ordering::Relaxed);
                record_error(&errors, reqwest_error_chain(&e)).await;
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
    while let Some(e) = source {
        parts.push(e.to_string());
        source = e.source();
    }
    if parts.is_empty() {
        error.to_string()
    } else {
        parts.join(" | caused by: ")
    }
}

async fn create_streams(backend: &Backend, count: usize, concurrency: usize) -> Result<()> {
    use futures::stream::FuturesUnordered;
    use futures::stream::StreamExt;

    let mut pending: FuturesUnordered<_> = FuturesUnordered::new();
    let mut next = 0usize;
    let max = concurrency.max(1);
    let push_one = |i: usize, pending: &mut FuturesUnordered<_>| {
        let backend = backend.clone();
        let stream = stream_name(i);
        pending.push(tokio::spawn(async move {
            backend
                .create_stream(&stream, "application/octet-stream")
                .await
        }));
    };
    while next < count && pending.len() < max {
        push_one(next, &mut pending);
        next += 1;
    }
    while let Some(joined) = pending.next().await {
        joined??;
        if next < count {
            push_one(next, &mut pending);
            next += 1;
        }
    }
    Ok(())
}

fn stream_name(idx: usize) -> String {
    format!("sustained-{:08}", idx)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    /// Verify that the steady-rate limiter fires ~R times in 1 second.
    ///
    /// We use `tokio::time::pause()` so the test runs instantly without real
    /// wall-clock delay.  We advance time by exactly 1 s and count ticks.
    #[tokio::test(start_paused = true)]
    async fn rate_limiter_fires_r_times_per_second() {
        const RATE: u64 = 20; // 20 ops/sec
        let interval_duration = Duration::from_secs_f64(1.0 / RATE as f64);
        let mut ticker = tokio::time::interval(interval_duration);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // Advance 1 second worth of virtual time, counting ticks.
        let start = tokio::time::Instant::now();
        let horizon = start + Duration::from_secs(1);

        let mut ticks: u64 = 0;
        loop {
            // Advance time just enough for the next tick to fire.
            tokio::time::advance(interval_duration).await;
            ticker.tick().await;
            ticks += 1;
            if tokio::time::Instant::now() >= horizon {
                break;
            }
        }

        // Allow ±1 tick tolerance for rounding.
        assert!(
            ticks >= RATE - 1 && ticks <= RATE + 1,
            "expected ~{RATE} ticks in 1s, got {ticks}"
        );
    }
}
