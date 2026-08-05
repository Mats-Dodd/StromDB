#!/usr/bin/env python3
"""Compare the cardinality-cliff SHAPE between a local kind run and the remote
corrected campaign: normalized throughput vs stream count per config, plus the
knee p50 trend. Local absolute numbers differ (laptop disk/CPU); what must match
for local iteration to be trustworthy is the RELATIVE decline."""
import json, os, sys

REMOTE = {  # 2026-07-08 corrected campaign (plateau thr, knee p50 ms)
    ("wal", "4vCPU"):    {100_000: (47_000, 4.5), 500_000: (31_000, 6.7)},
    ("wal", "8vCPU"):    {100_000: (43_000, 5.0), 500_000: (29_000, 6.9)},
    ("memory", "4vCPU"): {100_000: (315_000, 1.2), 500_000: (226_000, 1.1)},
    ("memory", "8vCPU"): {100_000: (526_000, 1.2), 500_000: (323_000, 1.3)},
}

def knee(walk, frac=0.8):
    ranked = [w for w in (walk or []) if len(w) >= 4 and w[2] is not None]
    if not ranked:
        return (None, None)
    peak = max(w[1] for w in ranked)
    under = [w for w in ranked if w[1] <= frac * peak]
    pick = max(under, key=lambda w: w[1]) if under else min(ranked, key=lambda w: w[0])
    return (pick[1], pick[2])

def series(path):
    if not os.path.exists(path):
        return {}
    cells = json.load(open(path))["cells"]
    out = {}
    for k, c in cells.items():
        if c.get("status") != "ok":
            continue
        kt, kp = knee(c.get("walk"))
        out[int(k)] = (c["throughput"], kt, kp)
    return dict(sorted(out.items()))

def show(name, s):
    if not s:
        print(f"  {name}: (no data)")
        return
    base_sc = min(s)
    base = s[base_sc][0]
    print(f"  {name}  (normalized to n={base_sc})")
    for sc, (thr, kt, kp) in s.items():
        rel = thr / base * 100
        lat = f"knee p50 {kp:.1f}ms" if kp is not None else ""
        print(f"    n={sc:<8} thr={thr/1000:7.1f}k  rel={rel:6.1f}%  {lat}")

print("== LOCAL (kind) ==")
for label in ("wal", "memory"):
    show(f"local {label} 2vCPU", series(f"results/write-cliff-local/{label}/cells.json"))
show("local wal 4vCPU", series("results/write-cliff-local-cpu4/wal-cpu4/cells.json"))

print("\n== REMOTE (corrected campaign, for shape comparison) ==")
for (label, pin), pts in REMOTE.items():
    base = pts[100_000][0]
    line = "  ".join(f"n={sc}: {thr/1000:.0f}k ({thr/base*100:.0f}%) p50 {p50}ms" for sc, (thr, p50) in pts.items())
    print(f"  remote {label} {pin}:  {line}")
