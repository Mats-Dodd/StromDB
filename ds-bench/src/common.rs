use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use hdrhistogram::Histogram;
use reqwest::Client;
use serde::Serialize;

pub fn build_client(timeout_secs: u64) -> Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        // Keep ALL idle connections alive — at high connection counts a small
        // idle pool forces TCP re-handshakes (churn) that throttle the load
        // generator before the server is saturated. Paired with the raised
        // NOFILE limit, this lets one pod sustain many thousands of connections.
        .pool_max_idle_per_host(usize::MAX)
        .tcp_nodelay(true)
        .build()
        .context("build reqwest client")
}

pub fn new_histogram() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("hist bounds")
}

pub fn record(hist: &mut Histogram<u64>, started_at: Instant) {
    record_micros(hist, started_at.elapsed());
}

/// Record a PRE-MEASURED duration. Callers that must acquire a shared lock before
/// touching the histogram measure `elapsed()` FIRST and pass it here, so lock-wait
/// (histogram-mutex contention under a client stampede) is never folded into the
/// sample — that inflated the catch-up tail latency (see catch_up.rs).
pub fn record_micros(hist: &mut Histogram<u64>, dur: Duration) {
    let us = dur.as_micros().min(u64::MAX as u128) as u64;
    let us = us.min(hist.high());
    let _ = hist.record(us.max(hist.low()));
}

#[derive(Default, Clone, Debug, Serialize)]
pub struct LatencySummary {
    pub count: u64,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub max_ms: f64,
}

pub fn summarize(hist: &Histogram<u64>) -> LatencySummary {
    if hist.is_empty() {
        return LatencySummary::default();
    }
    let to_ms = |v: u64| (v as f64) / 1000.0;
    LatencySummary {
        count: hist.len(),
        mean_ms: hist.mean() / 1000.0,
        p50_ms: to_ms(hist.value_at_quantile(0.5)),
        p90_ms: to_ms(hist.value_at_quantile(0.9)),
        p99_ms: to_ms(hist.value_at_quantile(0.99)),
        p999_ms: to_ms(hist.value_at_quantile(0.999)),
        max_ms: to_ms(hist.max()),
    }
}

pub fn merge(target: &mut Histogram<u64>, other: &Histogram<u64>) {
    target.add(other).expect("histogram bounds match");
}

/// Stream a response body to its end and return only its total byte length,
/// WITHOUT materializing the whole body. Catch-up / replay readers need only the
/// size, but `resp.bytes()` held the entire response (up to a full resident stream)
/// in memory per reader — at high fan-out that OOMs the client (the documented
/// catch-up ceiling). Streaming keeps peak memory at ~one chunk per reader.
pub async fn drain_len(resp: reqwest::Response) -> Result<u64> {
    use futures::stream::StreamExt;
    let mut stream = resp.bytes_stream();
    let mut n: u64 = 0;
    while let Some(chunk) = stream.next().await {
        n += chunk.context("stream body chunk")?.len() as u64;
    }
    Ok(n)
}

pub fn fill_payload(size: usize, seed: u64) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    let mut state = seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1);
    for chunk in buf.chunks_mut(8) {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let bytes = state.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    buf
}

/// The instant the `n`-th record is DUE in an open-loop paced writer: `base + n/rate`
/// seconds. A paced writer must measure latency from this scheduled time, not from
/// when it actually got around to issuing the request — otherwise a server that
/// stalls simply makes the loop fall behind and the requests that *should* have
/// fired are never timed (coordinated omission), so a degrading server reads as
/// latency-stable. `rate_per_stream` 0 is treated as 1/s.
pub fn scheduled_send(base: Instant, n: u64, rate_per_stream: u64) -> Instant {
    base + Duration::from_secs_f64(n as f64 / rate_per_stream.max(1) as f64)
}

/// High-entropy printable-ASCII payload of exactly `size` bytes, for records that
/// travel inside a JSON string (batched appends, and the S2 record body). Unlike a
/// run of one byte (`"x".repeat(n)`, artificially compressible) or raw random bytes
/// mangled by `from_utf8_lossy` (length-changing), every byte here is in a
/// JSON-safe printable set (0x20–0x7e minus `"` and `\`), so the record is
/// incompressible AND its byte length is exactly `size`. Deterministic in `seed`.
pub fn fill_payload_text(size: usize, seed: u64) -> Vec<u8> {
    // 90 JSON-string-safe printable ASCII bytes (0x20..=0x7e minus '"' and '\').
    const ALPHABET: &[u8] = b" !#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`abcdefghijklmnopqrstuvwxyz{|}~";
    let mut buf = Vec::with_capacity(size);
    let mut state = seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1);
    for _ in 0..size {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let idx = (state >> 33) as usize % ALPHABET.len();
        buf.push(ALPHABET[idx]);
    }
    buf
}

#[derive(Clone, Debug, Serialize)]
pub struct Counts {
    pub ok: u64,
    pub backpressure: u64,
    pub other_err: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// scheduled_send returns base + n/rate seconds — the instant the n-th record
    /// was DUE in an open-loop paced writer. Measuring latency from this (not from
    /// when the loop actually issued the request) is the coordinated-omission fix:
    /// a server that falls behind makes overdue requests show large latency instead
    /// of the loop silently slowing and hiding the stall.
    #[test]
    fn scheduled_send_is_base_plus_n_over_rate() {
        let base = Instant::now();
        assert_eq!(scheduled_send(base, 0, 100), base);
        assert_eq!(scheduled_send(base, 100, 100), base + Duration::from_secs(1));
        assert_eq!(scheduled_send(base, 50, 100), base + Duration::from_millis(500));
        // rate 0 is treated as 1/s (no divide-by-zero), not a panic.
        assert_eq!(scheduled_send(base, 2, 0), base + Duration::from_secs(2));
    }

    /// fill_payload_text produces exactly `size` JSON-safe printable bytes with
    /// real entropy (not a run of one byte), so a batched/JSON record is neither
    /// artificially compressible (the all-'x' batch-body confound) nor length-
    /// mangled by utf8_lossy (the S2 confound). Deterministic in `seed`.
    #[test]
    fn fill_payload_text_is_sized_json_safe_and_high_entropy() {
        let n = 256;
        let a = fill_payload_text(n, 0xABCD);
        assert_eq!(a.len(), n, "exact requested length");
        for &b in &a {
            assert!((0x20..=0x7e).contains(&b), "printable ASCII, got {b:#x}");
            assert!(b != b'"' && b != b'\\', "JSON-string-safe (no quote/backslash)");
        }
        let distinct: std::collections::HashSet<u8> = a.iter().copied().collect();
        assert!(distinct.len() > 8, "high entropy, got {} distinct bytes", distinct.len());
        assert_eq!(a, fill_payload_text(n, 0xABCD), "deterministic in seed");
        assert_ne!(a, fill_payload_text(n, 0x1234), "varies with seed");
    }

    /// record_micros stores the PRE-MEASURED duration verbatim, so a caller can
    /// measure elapsed() before taking a shared lock and never fold lock-wait
    /// into the sample (the catch-up mutex-contention tail-inflation bug).
    #[test]
    fn record_micros_uses_the_given_duration_not_now() {
        let mut h = new_histogram();
        record_micros(&mut h, Duration::from_millis(5));
        // 5 ms = 5000 µs (histogram unit); within HDR's 3-sig-fig precision.
        let v = h.value_at_quantile(0.5);
        assert!((4990..=5010).contains(&v), "expected ~5000us, got {v}");
    }
}
