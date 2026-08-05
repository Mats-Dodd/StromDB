//! Shared SSE / timestamped-payload helpers.
//!
//! These were duplicated verbatim between `mixed.rs` and `fanout.rs`. `mixed.rs`
//! now uses this module. `fanout.rs` is a forked file kept byte-identical to
//! upstream (except its Task-1 emit line) and so KEEPS its own private copies —
//! that residual duplication is intentional and acceptable under the
//! verbatim-fanout constraint.

use base64::Engine;
use std::time::{SystemTime, UNIX_EPOCH};

/// Current UNIX time in nanoseconds (0 on the impossible pre-epoch case).
pub fn unix_nanos_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Build a payload with an embedded ns-precision send timestamp.
/// Layout: [16 hex chars seq][32 hex chars send_ns][' '][filler]
/// Minimum size: 49 bytes.
pub fn build_payload(seq: u64, size: usize) -> Vec<u8> {
    let now_ns = unix_nanos_now();
    let mut head = String::with_capacity(64);
    head.push_str(&format!("{seq:016x}"));
    head.push_str(&format!("{now_ns:032x}"));
    head.push(' ');
    let mut buf = Vec::with_capacity(size);
    let head_bytes = head.as_bytes();
    let take = head_bytes.len().min(size);
    buf.extend_from_slice(&head_bytes[..take]);
    if size > take {
        buf.resize(size, b'.');
    }
    buf
}

/// Scan for the first run of >=48 consecutive ASCII hex digits and parse
/// bytes 16..48 as a u128 nanos timestamp.
pub fn extract_send_ns(payload: &[u8]) -> Option<u128> {
    let mut run_start: Option<usize> = None;
    for (i, b) in payload.iter().enumerate() {
        if b.is_ascii_hexdigit() {
            let start = match run_start {
                Some(s) => s,
                None => {
                    run_start = Some(i);
                    i
                }
            };
            if i + 1 - start >= 48 {
                let s = std::str::from_utf8(&payload[start + 16..start + 48]).ok()?;
                return u128::from_str_radix(s, 16).ok();
            }
        } else {
            run_start = None;
        }
    }
    None
}

/// Try to extract the send_ns from an SSE payload that may be:
/// - Raw ASCII hex (Ursula/Durable raw text): scan for 48 consecutive hex digits directly.
/// - Base64-encoded bytes (Durable Streams SSE): base64-decode first, then scan.
pub fn extract_send_ns_maybe_b64(payload: &[u8]) -> Option<u128> {
    // Try direct scan first (handles Ursula raw text and already-decoded data).
    if let Some(ns) = extract_send_ns(payload) {
        return Some(ns);
    }
    // Attempt base64 decode — the server may have base64-encoded the binary payload.
    // Strip whitespace that may appear at ends of base64 lines.
    let trimmed: Vec<u8> = payload.iter().copied().filter(|b| !b.is_ascii_whitespace()).collect();
    if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(&trimmed) {
        return extract_send_ns(&decoded);
    }
    None
}

pub fn find_event_end(buf: &[u8]) -> Option<usize> {
    let mut prev = b'\0';
    for (i, b) in buf.iter().enumerate() {
        if prev == b'\n' && *b == b'\n' {
            return Some(i - 1);
        }
        prev = *b;
    }
    None
}

pub fn parse_sse_data(raw: &[u8]) -> Option<Vec<u8>> {
    let mut payload = Vec::new();
    for line in raw.split(|b| *b == b'\n') {
        if line.starts_with(b"data:") {
            let rest = &line[5..];
            let rest = if rest.starts_with(b" ") {
                &rest[1..]
            } else {
                rest
            };
            if !payload.is_empty() {
                payload.push(b'\n');
            }
            payload.extend_from_slice(rest);
        }
    }
    if payload.is_empty() {
        None
    } else {
        Some(payload)
    }
}
