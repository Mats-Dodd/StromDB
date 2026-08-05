#!/usr/bin/env python3
"""Pure saturation classifier + throughput reader for the saturation walk."""
import argparse, json, re

def classify(prev_thr, thr, cpu_pct, cores, cpu_frac=0.90, plateau_frac=0.10):
    if cpu_pct >= cpu_frac * cores * 100.0:
        return "cpu"
    if prev_thr > 0 and (thr - prev_thr) / prev_thr < plateau_frac:
        return "plateau"
    return "headroom"

def _last_json_object(text):
    """Extract the last parseable JSON object from coordinator output, which is a
    `mc cp` download log followed by the hdr-merge JSON — the JSON is usually
    PRETTY-PRINTED across multiple lines, so a per-line scan misses it. Mirrors
    render_common.load_merged's strategy."""
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    for o in reversed(re.findall(r"\{.*?\}", text, re.S)):
        try:
            obj = json.loads(o)
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue
    i = text.find("{")
    if i >= 0:
        try:
            obj = json.loads(text[i:])
            if isinstance(obj, dict):
                return obj
        except Exception:
            return None
    return None

def extract_merged(path):
    """merged.json → its last JSON object (multi-line/pretty-printed safe), else None."""
    try:
        with open(path) as f:
            text = f.read()
    except FileNotFoundError:
        return None
    obj = _last_json_object(text)
    return obj if isinstance(obj, dict) else None

def windows_aligned(obj):
    """hdr-merge's verdict on whether the fleet's per-pod measure windows overlapped.
    Absent field (older client / no stamps) → True (back-compat: never reject old data)."""
    return bool(obj.get("windows_aligned", True)) if isinstance(obj, dict) else True

def fleet_complete(obj, expect_pods):
    """True when the merge is NOT a partial fleet: every expected pod reported.

    `expect_pods` is PARALLELISM (the rung's pod count). A merge whose
    `pods_reported` is below it means some pods never uploaded (Spot preemption /
    OOM), so the SUMMED throughput under-counts the server. Absent field (older
    client) or no expectation → True (back-compat: never reject on missing data)."""
    if not expect_pods or not isinstance(obj, dict):
        return True
    reported = obj.get("pods_reported")
    return reported is None or reported >= expect_pods


def extract_throughput(path, expect_pods=None):
    """merged.json → aggregate_ops_per_sec or _events_per_sec from its last JSON
    object. Missing file / no object → 0.0.

    A merged summary with windows_aligned=false ALSO reads as 0.0: the fleet sum
    multiply-counted server capacity (pods measured at different wall times — the
    500k-stream 2.9M-ops/s artifact), so no throughput reading exists for the rung.
    Likewise a partial fleet (pods_reported < expect_pods) reads as 0.0: the sum
    UNDER-counts the server, so the rung has no valid reading. Both route the
    walker's step_decision to "error", recording the cell as invalid instead of
    pinning a fantasy (or deflated) number."""
    obj = extract_merged(path)
    if not isinstance(obj, dict):
        return 0.0
    if not windows_aligned(obj):
        import sys
        print(f"saturation: rejecting {path}: windows_aligned=false "
              f"(span {obj.get('measure_span_secs')}s vs window {obj.get('measure_window_secs')}s)",
              file=sys.stderr)
        return 0.0
    if not fleet_complete(obj, expect_pods):
        import sys
        print(f"saturation: rejecting {path}: partial fleet "
              f"(pods_reported {obj.get('pods_reported')} < expected {expect_pods}) — "
              f"summed throughput under-counts the server",
              file=sys.stderr)
        return 0.0
    for k in ("aggregate_ops_per_sec", "aggregate_events_per_sec"):
        if k in obj:
            return float(obj[k])
    return 0.0

def cap_ladder(ladder, stream_count):
    """Cap each ladder rung at the stream_count and dedup (preserving order).

    A rung with more pods than streams would force streams/pod up to 1 and
    over-provision the cell (total streams = pods × ceil(streams/pods) > the
    intended count) — e.g. n=1 at 2 pods drives 2 streams. Capping pods ≤
    stream_count keeps the cell at its labelled cardinality; dedup avoids a
    repeated pod count (which would read as a false 0%-gain plateau)."""
    out = []
    for p in ladder:
        c = min(p, stream_count)
        if c >= 1 and c not in out:
            out.append(c)
    return out

def plateau_pin(walk, plateau_pct, patience=1):
    """Noise-robust plateau detection over the walk so far.

    `walk` is the ladder in order: [[pods, thr, ...], ...]. Returns the [pods, thr]
    to pin when the server has plateaued — `patience` CONSECUTIVE rung-to-rung gains
    at or below `plateau_pct` percent — else None. The pinned rung is where the
    climb stopped: the rung immediately before that run of small gains.

    patience=1 reproduces the legacy single-shot rule exactly. patience>=2 needs the
    small gain to repeat, so a single unlucky-low rung (measurement noise) no longer
    triggers a false plateau and an under-reported ceiling. A negative `plateau_pct`
    (the -100 sentinel) makes every gain exceed the threshold → never pins (full
    ladder). thr<=0 rungs are handled by the caller (recorded as an error), not here.
    """
    patience = max(1, patience)
    if len(walk) < patience + 1:
        return None
    thrs = [w[1] for w in walk]
    thresh = plateau_pct / 100.0
    for i in range(len(thrs) - patience, len(thrs)):
        prev, cur = thrs[i - 1], thrs[i]
        if prev <= 0:
            return None
        if (cur - prev) / prev > thresh:
            return None  # a gain in the window still exceeds the threshold
    pin = walk[len(walk) - patience - 1]
    return [pin[0], pin[1]]


def step_decision(prev_thr, thr, plateau_pct):
    """Decide the walker's next move from consecutive ladder-rung throughputs.

    "error"    -> throughput collapsed to ~0 (choke/thrash); do not pin.
    "continue" -> first rung, or gain still above plateau threshold.
    "plateau"  -> gain at or below plateau threshold; server saturated.
    """
    if thr <= 0:
        return "error"
    if prev_thr <= 0:
        return "continue"
    gain = (thr - prev_thr) / prev_thr
    return "continue" if gain > plateau_pct / 100.0 else "plateau"

def extract_latency(path):
    """merged.json → (p50_ms, p99_ms) of the rung's fleet-merged histogram, or
    (None, None). Per-rung latency lets the walk distinguish the pre-saturation
    knee (latency ≈ service time) from the plateau rungs (latency = queueing —
    reporting THOSE as 'the latency' was the 100k-wal 64 ms mistake)."""
    obj = extract_merged(path)
    if not isinstance(obj, dict):
        return (None, None)
    return obj.get("p50_ms"), obj.get("p99_ms")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--merged", required=True)
    p.add_argument("--prev-thr", type=float, default=0.0)
    p.add_argument("--cpu", type=float, required=True)
    p.add_argument("--cores", type=float, required=True)
    p.add_argument("--expect-pods", type=int, default=None,
                   help="fleet size (PARALLELISM): reject the rung if fewer pods reported")
    a = p.parse_args()
    thr = extract_throughput(a.merged, expect_pods=a.expect_pods)
    # Third field: 1 = fleet measure windows overlapped (or no stamps: old data),
    # 0 = misaligned (thr is forced to 0 above). Lets the walker record the rung
    # as misaligned_windows rather than a generic error. Fourth/fifth: the rung's
    # merged p50/p99 ms ("None" when absent). Consumers reading only the first
    # two fields are unaffected.
    aligned = 1 if windows_aligned(extract_merged(a.merged)) else 0
    p50, p99 = extract_latency(a.merged)
    print(f"{classify(a.prev_thr, thr, a.cpu, a.cores)} {thr} {aligned} {p50} {p99}")

if __name__ == "__main__":
    main()
