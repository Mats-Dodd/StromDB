use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use clap::Args;
use clap::ValueEnum;
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
use crate::common::record;
use crate::common::summarize;

/// Body encoding for each append.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[clap(rename_all = "kebab-case")]
pub enum BodyMode {
    /// Raw bytes; content-type application/octet-stream.
    Binary,
    /// Single JSON object sized to --payload-bytes; content-type application/json.
    JsonSingle,
    /// JSON array of --array-records objects each ~(payload_bytes/N) bytes.
    JsonArray,
}

impl BodyMode {
    pub fn as_str(self) -> &'static str {
        match self {
            BodyMode::Binary => "binary",
            BodyMode::JsonSingle => "json-single",
            BodyMode::JsonArray => "json-array",
        }
    }
}

#[derive(Args, Debug, Clone)]
pub struct AppendArgs {
    /// Target base URL(s). Comma-separated for round-robin across nodes.
    #[arg(long)]
    pub target: String,

    /// Backend API style.
    #[arg(long, value_enum, default_value_t = ApiStyle::Ursula)]
    pub api_style: ApiStyle,

    /// Bucket name (Ursula only — ignored by Durable / S2).
    #[arg(long, default_value = "bench-append")]
    pub bucket: String,

    /// Basin name (S2 only).
    #[arg(long, default_value = "benchmark")]
    pub basin: String,

    /// Stream name to append to.
    #[arg(long, default_value = "append-bench")]
    pub stream: String,

    /// Number of concurrent appender tasks all writing to the same stream.
    #[arg(long, default_value_t = 8)]
    pub connections: usize,

    /// Payload size in bytes per append.
    #[arg(long, default_value_t = 256)]
    pub payload_bytes: usize,

    /// Wall-clock duration to drive load, in seconds.
    #[arg(long, default_value_t = 60)]
    pub duration_secs: u64,

    /// Body encoding mode: binary (raw bytes), json-single (one JSON object),
    /// or json-array (array of --array-records JSON objects).
    #[arg(long, value_enum, default_value_t = BodyMode::Binary)]
    pub body_mode: BodyMode,

    /// Number of records per JSON array (json-array mode only).
    #[arg(long, default_value_t = 10)]
    pub array_records: usize,

    /// HTTP request timeout in seconds.
    #[arg(long, default_value_t = 30)]
    pub request_timeout_secs: u64,
}

#[derive(Serialize)]
pub struct AppendResult {
    pub scenario: &'static str,
    pub api_style: ApiStyle,
    pub target: String,
    pub bucket: String,
    pub stream: String,
    pub connections: usize,
    pub payload_bytes: usize,
    pub duration_secs: u64,
    pub body_mode: &'static str,
    pub array_records: usize,
    pub elapsed_secs: f64,
    pub counts: Counts,
    pub errors: Vec<ErrorCount>,
    pub aggregate_ops_per_sec: f64,
    /// Only populated for json-array mode: appends/s × array_records.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records_per_sec: Option<f64>,
    pub latency_ms: LatencySummary,
}

#[derive(Clone, Debug, Serialize)]
pub struct ErrorCount {
    pub error: String,
    pub count: u64,
}

// ---------------------------------------------------------------------------
// Body construction
// ---------------------------------------------------------------------------

/// Build the (content_type, body_bytes) pair for a single append call.
pub fn build_body(mode: BodyMode, payload_bytes: usize, array_records: usize) -> (&'static str, Vec<u8>) {
    match mode {
        BodyMode::Binary => {
            let body = fill_payload(payload_bytes, 0xDEAD_BEEF);
            ("application/octet-stream", body)
        }
        BodyMode::JsonSingle => {
            // Build a JSON object whose total encoded size is ~payload_bytes.
            // {"d":"<filler>"} where filler is base64-encoded random bytes.
            // Overhead of `{"d":""}` is 7 bytes; base64 expands 3 bytes → 4 chars.
            let overhead = 7usize; // {"d":""}
            let filler_chars = payload_bytes.saturating_sub(overhead);
            // filler_chars base64 chars ≈ filler_chars * 3 / 4 raw bytes needed.
            let raw_bytes = (filler_chars * 3 + 3) / 4;
            let raw = fill_payload(raw_bytes.max(1), 0xFEED_CAFE);
            let b64 = B64.encode(&raw);
            let json = format!("{{\"d\":\"{}\"}}", &b64[..b64.len().min(filler_chars)]);
            ("application/json", json.into_bytes())
        }
        BodyMode::JsonArray => {
            // Array of array_records objects each ~payload_bytes/N bytes.
            let n = array_records.max(1);
            let per_record = payload_bytes / n;
            let overhead_per = 7usize; // {"d":""}  (plus comma, brackets ≈ handled below)
            let filler_chars = per_record.saturating_sub(overhead_per);
            let raw_bytes = (filler_chars * 3 + 3) / 4;
            let raw_bytes = raw_bytes.max(1);
            let mut array_str = String::with_capacity(payload_bytes + 32);
            array_str.push('[');
            for i in 0..n {
                let seed = (0xBEEF_CAFE_u64).wrapping_add(i as u64);
                let raw = fill_payload(raw_bytes, seed);
                let b64 = B64.encode(&raw);
                if i > 0 {
                    array_str.push(',');
                }
                array_str.push_str("{\"d\":\"");
                array_str.push_str(&b64[..b64.len().min(filler_chars)]);
                array_str.push_str("\"}");
            }
            array_str.push(']');
            ("application/json", array_str.into_bytes())
        }
    }
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

pub async fn run(args: AppendArgs) -> Result<AppendResult> {
    let client = build_client(args.request_timeout_secs)?;
    let backend = Backend::new(
        args.api_style,
        &args.target,
        &args.bucket,
        &args.basin,
        client,
    );

    tracing::info!(
        "ensure namespace and stream: api={} stream={} connections={}",
        args.api_style.as_str(),
        args.stream,
        args.connections,
    );
    backend.ensure_namespace().await?;
    backend
        .create_stream(&args.stream, "application/octet-stream")
        .await?;

    let ok = Arc::new(AtomicU64::new(0));
    let bp = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let errors: Arc<Mutex<BTreeMap<String, u64>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let hist = Arc::new(Mutex::new(new_histogram()));

    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);
    let start = Instant::now();

    // Include the pod instance index (DS_BENCH_INSTANCE = $JOB_COMPLETION_INDEX,
    // set by the fleet Job) so that parallel pods appending to the same stream
    // do not collide on (producer_id, seq) — duplicates get deduped/409'd by the
    // server and inflate the reported aggregate_ops_per_sec ~N×.
    let instance = std::env::var("DS_BENCH_INSTANCE")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "0".to_string());

    let mut workers = Vec::with_capacity(args.connections);
    for idx in 0..args.connections {
        let backend = backend.clone();
        let stream = args.stream.clone();
        let ok = ok.clone();
        let bp = bp.clone();
        let err = err.clone();
        let errors = errors.clone();
        let hist = hist.clone();
        let mode = args.body_mode;
        let payload_bytes = args.payload_bytes;
        let array_records = args.array_records;
        let producer_id = format!("bench-append-{instance}-{idx}");
        workers.push(tokio::spawn(async move {
            run_appender(
                backend,
                idx,
                stream,
                producer_id,
                mode,
                payload_bytes,
                array_records,
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

    for w in workers {
        let _ = w.await;
    }

    let elapsed = start.elapsed();
    let counts = Counts {
        ok: ok.load(Ordering::Relaxed),
        backpressure: bp.load(Ordering::Relaxed),
        other_err: err.load(Ordering::Relaxed),
    };
    let errors: Vec<ErrorCount> = errors
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
    crate::dist::emit_hdr(&h, &format!("append-{}", std::process::id()));
    let elapsed_secs = elapsed.as_secs_f64();
    let aggregate = counts.ok as f64 / elapsed_secs.max(1e-9);
    let records_per_sec = if args.body_mode == BodyMode::JsonArray {
        Some(aggregate * args.array_records as f64)
    } else {
        None
    };

    Ok(AppendResult {
        scenario: "append",
        api_style: args.api_style,
        target: args.target,
        bucket: args.bucket,
        stream: args.stream,
        connections: args.connections,
        payload_bytes: args.payload_bytes,
        duration_secs: args.duration_secs,
        body_mode: args.body_mode.as_str(),
        array_records: args.array_records,
        elapsed_secs,
        counts,
        errors,
        aggregate_ops_per_sec: aggregate,
        records_per_sec,
        latency_ms: latency,
    })
}

// ---------------------------------------------------------------------------
// Per-connection appender loop
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_appender(
    backend: Backend,
    base_idx: usize,
    stream: String,
    producer_id: String,
    mode: BodyMode,
    payload_bytes: usize,
    array_records: usize,
    deadline: Instant,
    ok: Arc<AtomicU64>,
    bp: Arc<AtomicU64>,
    err: Arc<AtomicU64>,
    errors: Arc<Mutex<BTreeMap<String, u64>>>,
    hist: Arc<Mutex<Histogram<u64>>>,
) {
    let epoch: u64 = 0;
    let mut seq: u64 = 0;
    let mut local = new_histogram();
    let use_producer = matches!(backend.kind, ApiStyle::Ursula | ApiStyle::Durable);

    while Instant::now() < deadline {
        let (content_type, body) = build_body(mode, payload_bytes, array_records);
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
            .append_request(base_idx, &stream, &body, producer, content_type)
            .send()
            .await;
        match resp {
            Ok(r) => {
                let status = r.status();
                if status.is_success() {
                    ok.fetch_add(1, Ordering::Relaxed);
                    record(&mut local, started);
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

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_array_builds_valid_array_of_n_elements() {
        let n = 10;
        let (_ct, body) = build_body(BodyMode::JsonArray, 256, n);
        let parsed: serde_json::Value =
            serde_json::from_slice(&body).expect("valid JSON");
        let arr = parsed.as_array().expect("JSON value is an array");
        assert_eq!(arr.len(), n, "expected {n} elements, got {}", arr.len());
        // Each element should be an object with key "d".
        for (i, elem) in arr.iter().enumerate() {
            assert!(
                elem.is_object(),
                "element {i} is not an object"
            );
            assert!(
                elem.get("d").is_some(),
                "element {i} missing key 'd'"
            );
        }
    }

    #[test]
    fn binary_body_has_correct_size() {
        let (ct, body) = build_body(BodyMode::Binary, 128, 1);
        assert_eq!(ct, "application/octet-stream");
        assert_eq!(body.len(), 128);
    }

    #[test]
    fn json_single_is_valid_json() {
        let (ct, body) = build_body(BodyMode::JsonSingle, 256, 1);
        assert_eq!(ct, "application/json");
        let v: serde_json::Value =
            serde_json::from_slice(&body).expect("valid JSON");
        assert!(v.is_object());
        assert!(v.get("d").is_some());
    }
}
