//! Fleet start barrier: align every pod's measure window to one wall-clock start.
//!
//! Summed per-pod rates only measure the server when all pods load it at the same
//! time. Without a barrier, per-pod setup (stream creation) plus Kubernetes
//! scheduling waves stagger the measure windows across minutes at high pod counts,
//! and the fleet sum multiply-counts server capacity (the 500k-stream 2.9M-ops/s
//! artifact — see `dist.rs`'s `windows_aligned`, which detects what this prevents).
//!
//! The client side is deliberately file-based — the pod's wrapper script owns the
//! MinIO round-trip (it already has `mc` and credentials for result upload):
//!
//!   1. after setup, the client writes `$DS_BENCH_BARRIER_DIR/ready`
//!   2. the wrapper uploads it as `…/<run>/barrier/ready-<pod-index>` and polls
//!      MinIO for `…/<run>/barrier/go`, downloading it to `$DS_BENCH_BARRIER_DIR/go`
//!   3. the harness host (leader) publishes `go` — a unix-ms start time a few
//!      seconds in the future — once every pod's ready marker is up
//!   4. the client parses `go` and sleeps until that instant, then starts its
//!      warmup/settle/measure phases
//!
//! Unset `DS_BENCH_BARRIER_DIR` ⇒ no-op (local runs, workloads without a fleet).
//! A timeout ⇒ proceed unaligned rather than fail the pod: the merge-side
//! `windows_aligned` check is the arbiter of whether the cell still counts.

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const POLL_INTERVAL: Duration = Duration::from_millis(250);
const DEFAULT_TIMEOUT_SECS: u64 = 600;

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Signal readiness and wait for the fleet-wide go time, then sleep until it.
/// Driven entirely by env (`DS_BENCH_BARRIER_DIR`, `DS_BENCH_BARRIER_TIMEOUT_SECS`);
/// returns without doing anything when no barrier is configured.
pub async fn sync_to_fleet_start() {
    let Some(dir) = std::env::var_os("DS_BENCH_BARRIER_DIR").filter(|d| !d.is_empty()) else {
        return;
    };
    let timeout = std::env::var("DS_BENCH_BARRIER_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_TIMEOUT_SECS);
    sync_in_dir(Path::new(&dir), Duration::from_secs(timeout)).await;
}

/// Testable core: write `ready`, poll for `go` (unix-ms start time) until
/// `timeout`, sleep until the go time. Timeout or unparseable `go` ⇒ log and
/// proceed (the merged `windows_aligned` verdict decides the cell's fate).
pub async fn sync_in_dir(dir: &Path, timeout: Duration) {
    let _ = std::fs::create_dir_all(dir);
    if let Err(e) = std::fs::write(dir.join("ready"), b"ready\n") {
        tracing::warn!("barrier: cannot write ready marker in {dir:?}: {e}; proceeding unaligned");
        return;
    }
    tracing::info!("barrier: ready written, waiting for go (timeout {}s)", timeout.as_secs());

    let deadline = std::time::Instant::now() + timeout;
    let go_path = dir.join("go");
    loop {
        if let Ok(text) = std::fs::read_to_string(&go_path) {
            let Ok(start_ms) = text.trim().parse::<u64>() else {
                tracing::warn!("barrier: unparseable go file {text:?}; proceeding unaligned");
                return;
            };
            let now = now_unix_ms();
            let wait = start_ms.saturating_sub(now);
            tracing::info!("barrier: go received (start in {wait} ms)");
            if wait > 0 {
                tokio::time::sleep(Duration::from_millis(wait)).await;
            }
            return;
        }
        if std::time::Instant::now() >= deadline {
            tracing::warn!(
                "barrier: no go after {}s; proceeding unaligned (windows_aligned will judge)",
                timeout.as_secs()
            );
            return;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("ds-bench-barrier-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// ready is written immediately; when go carries a future start time the
    /// client returns only after that instant.
    #[tokio::test]
    async fn waits_until_go_time() {
        let dir = tmp_dir("go");
        let go_at = now_unix_ms() + 600; // 600ms in the future
        std::fs::write(dir.join("go"), format!("{go_at}\n")).unwrap();

        sync_in_dir(&dir, Duration::from_secs(5)).await;

        assert!(dir.join("ready").exists(), "ready marker must be written");
        assert!(now_unix_ms() >= go_at, "must not return before the go time");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// go appearing late (after polling started) is picked up.
    #[tokio::test]
    async fn picks_up_late_go() {
        let dir = tmp_dir("late");
        let go_path = dir.join("go");
        let writer = {
            let go_path = go_path.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(400)).await;
                std::fs::write(&go_path, format!("{}\n", now_unix_ms())).unwrap();
            })
        };
        let t0 = std::time::Instant::now();
        sync_in_dir(&dir, Duration::from_secs(5)).await;
        assert!(t0.elapsed() >= Duration::from_millis(400), "must wait for go to appear");
        writer.await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// No go within the timeout ⇒ proceed (unaligned) rather than hang or panic.
    #[tokio::test]
    async fn times_out_and_proceeds() {
        let dir = tmp_dir("timeout");
        let t0 = std::time::Instant::now();
        sync_in_dir(&dir, Duration::from_millis(600)).await;
        let dt = t0.elapsed();
        assert!(dt >= Duration::from_millis(600) && dt < Duration::from_secs(5));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A past go time returns immediately (no underflow / no sleep).
    #[tokio::test]
    async fn past_go_time_returns_immediately() {
        let dir = tmp_dir("past");
        std::fs::write(dir.join("go"), format!("{}\n", now_unix_ms() - 10_000)).unwrap();
        let t0 = std::time::Instant::now();
        sync_in_dir(&dir, Duration::from_secs(5)).await;
        assert!(t0.elapsed() < Duration::from_millis(300));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
