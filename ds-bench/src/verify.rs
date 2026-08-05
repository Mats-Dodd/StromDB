//! Server-side ground truth for the write workload: HEAD every stream in the
//! global key domain and sum `stream-next-offset` (bytes). With a fixed payload
//! size at batch=1 the sum divides exactly into a record count, which must
//! equal the fleet's summed client-observed successes (`ok_total_all_phases`
//! across per-pod JSONs). Any mismatch means the harness is not reporting
//! accurate numbers — the check the fleet coordination work exists to pass.

use anyhow::Result;
use clap::Args;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use serde::Serialize;

use crate::backend::ApiStyle;
use crate::backend::Backend;
use crate::common::build_client;

#[derive(Args, Debug, Clone)]
pub struct VerifyOffsetsArgs {
    /// Target base URL(s). Comma-separated for round-robin across nodes.
    #[arg(long)]
    pub target: String,

    /// Backend API style (durable only — offsets are read via HEAD).
    #[arg(long, value_enum, default_value_t = ApiStyle::Durable)]
    pub api_style: ApiStyle,

    /// Global stream-domain size: verifies streams s00000000..s<N-1>.
    #[arg(long)]
    pub streams: usize,

    /// Payload bytes per append (batch=1): offsets divide by this into records.
    #[arg(long, default_value_t = 256)]
    pub payload_bytes: usize,

    /// Concurrent HEAD requests.
    #[arg(long, default_value_t = 256)]
    pub concurrency: usize,

    /// HTTP request timeout in seconds.
    #[arg(long, default_value_t = 30)]
    pub request_timeout_secs: u64,
}

#[derive(Serialize)]
pub struct VerifyOffsetsResult {
    pub scenario: &'static str,
    pub streams: usize,
    /// Streams that answered HEAD 200.
    pub streams_found: usize,
    /// Streams answering 404 (never created — a coverage gap if setup ran).
    pub streams_missing: usize,
    /// HEAD failures other than 404 (transport errors, 5xx).
    pub head_errors: usize,
    /// Σ stream-next-offset over all found streams (bytes appended, server truth).
    pub total_bytes: u64,
    /// total_bytes / payload_bytes — the server-side record count. Exact only
    /// when every append carried the same payload_bytes at batch=1.
    pub total_records: u64,
    /// True when total_bytes divides payload_bytes exactly.
    pub bytes_divide_exactly: bool,
    /// Streams with offset 0 among the found ones (created but never written —
    /// uneven key-space use if large).
    pub streams_empty: usize,
}

fn stream_name_global(idx: usize) -> String {
    format!("s{idx:08}")
}

pub async fn run(args: VerifyOffsetsArgs) -> Result<VerifyOffsetsResult> {
    let client = build_client(args.request_timeout_secs)?;
    let backend = Backend::new(args.api_style, &args.target, "bench-multistream", "benchmark", client);

    let mut pending: FuturesUnordered<_> = FuturesUnordered::new();
    let mut next = 0usize;
    let max = args.concurrency.max(1);
    let (mut found, mut missing, mut errors, mut empty) = (0usize, 0usize, 0usize, 0usize);
    let mut total_bytes: u64 = 0;

    let head_one = |i: usize| {
        let backend = backend.clone();
        async move {
            let base = backend.base_for(i).to_string();
            let stream = stream_name_global(i);
            let url = match backend.kind {
                ApiStyle::Durable => format!("{base}/v1/stream/{stream}"),
                ApiStyle::Ursula => format!("{base}/{}/{stream}", backend.bucket),
                ApiStyle::S2 => format!("{base}/v1/streams/{stream}"),
            };
            let resp = backend.client.head(&url).send().await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let off = r
                        .headers()
                        .get("stream-next-offset")
                        .and_then(|v| v.to_str().ok())
                        // durable formats offsets as "<epoch>_<bytes>" or plain bytes;
                        // take the numeric tail either way.
                        .and_then(|s| s.rsplit('_').next().map(str::to_string))
                        .and_then(|s| s.trim().parse::<u64>().ok());
                    match off {
                        Some(b) => Ok(Some(b)),
                        None => Err(()),
                    }
                }
                Ok(r) if r.status().as_u16() == 404 => Ok(None),
                _ => Err(()),
            }
        }
    };

    while next < args.streams && pending.len() < max {
        pending.push(head_one(next));
        next += 1;
    }
    while let Some(res) = pending.next().await {
        match res {
            Ok(Some(bytes)) => {
                found += 1;
                if bytes == 0 {
                    empty += 1;
                }
                total_bytes += bytes;
            }
            Ok(None) => missing += 1,
            Err(()) => errors += 1,
        }
        if next < args.streams {
            pending.push(head_one(next));
            next += 1;
        }
    }

    let pb = args.payload_bytes.max(1) as u64;
    Ok(VerifyOffsetsResult {
        scenario: "verify-offsets",
        streams: args.streams,
        streams_found: found,
        streams_missing: missing,
        head_errors: errors,
        total_bytes,
        total_records: total_bytes / pb,
        bytes_divide_exactly: total_bytes % pb == 0,
        streams_empty: empty,
    })
}
