"""Aggregate per-mode cells.json into aggregate.csv/json + a markdown skeleton.
Deterministic; no cluster needed."""
import sys, os, json, csv
sys.path.insert(0, os.path.dirname(__file__))
from suite import Suite
import cells as cells_mod


def knee_of(walk, frac=0.8):
    """The pre-saturation 'knee' rung of a walk: the LARGEST rung whose
    throughput is ≤ `frac` × the walk's max (fallback: the smallest rung).
    Only rungs that carry latency ([pods, thr, p50, p99]) qualify.

    Rationale: a closed-loop fleet driven past the ceiling measures pure
    QUEUEING latency (in-flight ÷ ceiling, Little's law) — it says nothing
    about the server's service time. Latency must be quoted from below the
    knee; the pinned rung's latency is reported separately AS saturation
    queueing (verified manually 2026-07-08: wal@100k curl p50 1.0 ms unloaded,
    5 ms at 74% load, 66 ms at 4096 in-flight — all the same server)."""
    ranked = [w for w in (walk or []) if len(w) >= 4 and w[2] is not None]
    if not ranked:
        return None
    peak = max(w[1] for w in ranked)
    under = [w for w in ranked if w[1] <= frac * peak]
    pick = max(under, key=lambda w: w[1]) if under else min(ranked, key=lambda w: w[0])
    return {"pods": pick[0], "throughput": pick[1], "p50": pick[2], "p99": pick[3]}


def headline_throughput(cell):
    """The throughput to quote for a cell. A SATURATED cell uses its confirmed
    (replicated) plateau throughput. A NOT-saturated cell (ladder_exhausted, e.g.
    plateau_pct=-100 full sweeps) must NOT quote its stored top-rung value — that is
    the over-saturation asymptote (closed-loop queueing, "not the number we report",
    AGENTS.md §7). Quote the walk MAX instead: the highest rate the server
    DEMONSTRABLY reached, an honest † lower bound on the ceiling."""
    if cell.get("saturated"):
        return cell.get("throughput")
    thrs = [w[1] for w in (cell.get("walk") or [])
            if len(w) >= 2 and isinstance(w[1], (int, float))]
    return max(thrs) if thrs else cell.get("throughput")


def build(suite_path, results_root):
    s = Suite.load(suite_path)
    rows = []
    # One result series per LABEL (mode × server-config variant) — variants like
    # wal vs wal-tailcache land in adjacent columns. `mode` field holds the label.
    for label in s.labels():
        p = os.path.join(results_root, label, "cells.json")
        if not os.path.exists(p):
            continue
        for c in cells_mod.all_cells(p):
            knee = knee_of(c.get("walk")) or {}
            rows.append({"mode": label, "stream_count": c["stream_count"],
                         "pods": c.get("pinned_pods"), "throughput": headline_throughput(c),
                         "p50": c.get("p50"), "p99": c.get("p99"),
                         "knee_pods": knee.get("pods"), "knee_throughput": knee.get("throughput"),
                         "knee_p50": knee.get("p50"), "knee_p99": knee.get("p99"),
                         "pod_mem_mb": c.get("pod_mem_mb"), "pod_mem_p50_mb": c.get("pod_mem_p50_mb"),
                         "saturated": c.get("saturated"),
                         "status": c.get("status"), "reason": c.get("reason"),
                         "walk": c.get("walk")})
    rows.sort(key=lambda r: (r["stream_count"], r["mode"]))
    return rows, _markdown(s, rows)


def suite_status(suite_path, results_root):
    """Overall completion state of a suite's run, for auto-teardown decisions:
      "complete"   — every (label × stream_count) cell present and status "ok".
      "errors"     — all cells present but at least one status "error" (keep the
                     cluster for investigation / resume).
      "incomplete" — a cell is missing (run didn't finish; keep the cluster)."""
    s = Suite.load(suite_path)
    # Catch-up cells are keyed by `pre_events` (not `stream_count`) and live in a
    # separate store, so they need the catchup_cells reader + key.
    if s.workload == "reads":
        import reads_cells
        saw_error = False
        for label in s.labels():
            p = os.path.join(results_root, label, "cells.json")
            by_sc = {c["stream_count"]: c for c in reads_cells.all_cells(p)} if os.path.exists(p) else {}
            for sc in s.stream_counts:
                c = by_sc.get(sc)
                if c is None or not c.get("complete"):
                    return "incomplete"
                if any(cc.get("status") == "error" for cc in c["connections"].values()):
                    saw_error = True
        return "errors" if saw_error else "complete"
    if s.workload == "mixed":
        # Mixed cells mirror reads: a per-level map + `complete`, keyed by
        # stream_count; an errored level makes the suite "errors" (resumable).
        import mixed_cells
        saw_error = False
        for label in s.labels():
            p = os.path.join(results_root, label, "cells.json")
            by_sc = {c["stream_count"]: c for c in mixed_cells.all_cells(p)} if os.path.exists(p) else {}
            for sc in s.stream_counts:
                c = by_sc.get(sc)
                if c is None or not c.get("complete"):
                    return "incomplete"
                if any(cc.get("status") == "error" for cc in c["levels"].values()):
                    saw_error = True
        return "errors" if saw_error else "complete"
    if s.workload == "catchup":
        import catchup_cells as cu_cells
        key = "pre_events"
        reader = cu_cells.all_cells
    else:
        key = "stream_count"
        reader = cells_mod.all_cells
    saw_error = False
    for label in s.labels():
        p = os.path.join(results_root, label, "cells.json")
        by_sc = {c[key]: c for c in reader(p)} if os.path.exists(p) else {}
        for sc in s.stream_counts:
            c = by_sc.get(sc)
            if c is None:
                return "incomplete"
            if c.get("status") == "error":
                saw_error = True
    return "errors" if saw_error else "complete"


def _cell_str(r):
    if r["status"] == "error":
        return f"ERROR ({r['reason']})"
    n = r["throughput"]
    mark = "" if r["saturated"] else "†"   # † = not saturated (lower bound)
    return f"{n/1000:.0f}k{mark}"


def _markdown(s, rows):
    labels = s.labels()
    out = [f"# {s.name} — write-throughput report", ""]
    out += ["## Throughput at saturation (ops/s)", ""]
    header = "| streams | " + " | ".join(labels) + " |"
    out += [header, "|" + "---|" * (len(labels) + 1)]
    by = {(r["mode"], r["stream_count"]): r for r in rows}
    for sc in s.stream_counts:
        cells_row = [_cell_str(by[(m, sc)]) if (m, sc) in by else "—" for m in labels]
        out.append(f"| {sc} | " + " | ".join(cells_row) + " |")
    out += ["", "† = not saturated (ladder exhausted) — treat as a lower bound.", ""]

    # Peak pod working-set memory (anon + active page cache) at saturation. Uniform
    # across implementations, so it captures a resident cache (shows as memory) vs an
    # OS-paging design (data in page cache, also charged to the pod) on equal terms.
    if any(r.get("pod_mem_mb") for r in rows):
        out += ["## Pod memory at saturation — peak / p50 (MiB)", ""]
        out += [header, "|" + "---|" * (len(labels) + 1)]
        for sc in s.stream_counts:
            mrow = []
            for m in labels:
                r = by.get((m, sc))
                if r and r.get("pod_mem_mb"):
                    pk = r["pod_mem_mb"]; md = r.get("pod_mem_p50_mb")
                    mrow.append(f"{pk:.0f} / {md:.0f}" if md is not None else f"{pk:.0f}")
                else:
                    mrow.append("—")
            out.append(f"| {sc} | " + " | ".join(mrow) + " |")
        out += ["", "_Pod working set = cgroup `memory.current − inactive_file` (anon + active page "
                "cache), sampled each second at the pinned rung. **peak** = high-water (catches bursts "
                "like an in-RAM Raft log filling); **p50** = median (what the server steadily holds "
                "resident). peak ≈ p50 ⇒ steadily resident; peak ≫ p50 ⇒ transient spikes._", ""]

    # Latency belongs BELOW the knee: at plateau rungs a closed-loop fleet only
    # measures its own queueing (in-flight ÷ ceiling). Quote service latency
    # from the ≤80%-of-peak rung; the pinned rung's numbers are labelled as
    # saturation queueing.
    if any(r.get("knee_p50") is not None for r in rows):
        out += ["## Latency (ms, p50 / p99)", ""]
        out += ["| streams | " + " | ".join(f"{m} @≤80% load | {m} @saturation" for m in labels) + " |",
                "|" + "---|" * (2 * len(labels) + 1)]
        for sc in s.stream_counts:
            lrow = []
            for m in labels:
                r = by.get((m, sc))
                if r and r.get("knee_p50") is not None:
                    lrow.append(f"{r['knee_p50']:.1f} / {r['knee_p99']:.1f} ({r['knee_throughput']/1000:.0f}k @{r['knee_pods']}p)")
                else:
                    lrow.append("—")
                if r and r.get("p50") is not None:
                    lrow.append(f"{r['p50']:.1f} / {r['p99']:.1f}")
                else:
                    lrow.append("—")
            out.append(f"| {sc} | " + " | ".join(lrow) + " |")
        out += ["", "_@≤80% load = the largest ladder rung at ≤80% of peak throughput — the server's "
                "service latency with headroom. @saturation = the pinned plateau rung, where a "
                "closed-loop fleet measures its own queueing (≈ in-flight ÷ ceiling by Little's law), "
                "NOT the server's per-request cost. Compare against the unloaded single-request "
                "baseline (~1 ms for wal) before reading anything into large saturation values._", ""]

    out += ["## Saturation walks (pods → ops/s, p50 ms)", ""]
    for r in rows:
        walk = " → ".join(
            f"{w[0]}:{w[1]/1000:.0f}k" + (f"@{w[2]:.1f}ms" if len(w) >= 4 and w[2] is not None else "")
            for w in (r["walk"] or []))
        out.append(f"- **{r['mode']} {r['stream_count']}**: {walk}  (pinned {r['pods']}, {r['reason']})")
    out += ["", "## Findings", "", "_TODO: written by hand on top of the generated data._", ""]
    out += ["## Caveats", "", "_Single-node best-case; not 3-node Raft. Throughput is a saturation ceiling per the ladder._", ""]
    return "\n".join(out)


def main():
    suite_path = sys.argv[1]
    s = Suite.load(suite_path)
    root = os.path.join("results", s.name)
    rows, md = build(suite_path, root)
    os.makedirs(root, exist_ok=True)
    with open(os.path.join(root, "aggregate.json"), "w") as f:
        json.dump(rows, f, indent=2)
    with open(os.path.join(root, "aggregate.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["mode", "stream_count", "pods", "throughput", "p50", "p99", "knee_pods", "knee_throughput", "knee_p50", "knee_p99", "pod_mem_mb", "pod_mem_p50_mb", "saturated", "status", "reason"])
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in w.fieldnames})
    with open(os.path.join(root, "report.md"), "w") as f:
        f.write(md)
    print(f"wrote {root}/aggregate.csv, aggregate.json, report.md")


if __name__ == "__main__":
    main()
