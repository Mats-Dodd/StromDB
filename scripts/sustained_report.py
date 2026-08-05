"""Report for the SUSTAINED workload: per stream-count × config, the achieved
throughput, tail latency, and server-memory stability (RSS peak/drift) + CPU.
Reads each label's sustained cells.json (already collected locally). No cluster.

Usage: scripts/sustained_report.py <suite.json>  ->  results/<suite>/report.md
       suite_status(suite, results_root) -> complete|errors|incomplete (for teardown)
"""
import sys, os, csv
sys.path.insert(0, os.path.dirname(__file__))
from suite import Suite
import sustained_cells


def build(suite_path, results_root):
    s = Suite.load(suite_path)
    rows = []
    for label in s.labels():
        cells = {c["stream_count"]: c for c in sustained_cells.all_cells(
            os.path.join(results_root, label, "cells.json"))}
        for sc in s.stream_counts:
            c = cells.get(sc)
            if c is None:
                rows.append({"label": label, "stream_count": sc, "status": "missing"})
            else:
                r = dict(c); r["label"] = label
                rows.append(r)
    return s, rows


def suite_status(suite_path, results_root):
    _, rows = build(suite_path, results_root)
    if any(r.get("status") == "missing" for r in rows):
        return "incomplete"
    if any(r.get("status") == "error" for r in rows):
        return "errors"
    return "complete"


def _f(x, d="—"):
    return f"{x:g}" if isinstance(x, (int, float)) else d


def markdown(s, rows):
    labels = s.labels()
    su = s.sustained
    scs = s.stream_counts
    by = {(r["label"], r["stream_count"]): r for r in rows}
    out = [f"# Sustained load — {s.name}", ""]
    out += [f"_{su.get('rate_per_stream', 10)} ops/s per stream, held {su.get('duration_secs', 90)}s; "
            f"{su.get('payload_bytes', 256)} B payload, {su.get('pods', 1)} client pod(s). "
            f"Measures stability, not peak._", ""]

    out += ["## Throughput + tail latency", ""]
    out += ["| streams | " + " | ".join(f"{l} ops/s | p50 | p99" for l in labels) + " |",
            "|" + "---|" * (1 + 3 * len(labels))]
    for sc in scs:
        cells = []
        for l in labels:
            r = by.get((l, sc))
            if not r or r.get("status") in ("missing", None):
                cells += ["—", "—", "—"]
            elif r.get("status") == "error":
                cells += [f"ERR({r.get('reason')})", "—", "—"]
            else:
                cells += [f"{r['throughput']/1000:.1f}k", _f(r.get("p50")), _f(r.get("p99"))]
        out.append(f"| {sc} | " + " | ".join(cells) + " |")
    out += [""]

    out += ["## Server memory stability (RSS, MiB) + CPU", ""]
    out += ["| streams | " + " | ".join(f"{l} peak | drift | cpu% | stable" for l in labels) + " |",
            "|" + "---|" * (1 + 4 * len(labels))]
    for sc in scs:
        cells = []
        for l in labels:
            r = by.get((l, sc))
            if not r or r.get("status") != "ok":
                cells += ["—", "—", "—", "—"]
            else:
                cells += [_f(r.get("rss_peak_mb")), _f(r.get("rss_drift_mb")),
                          _f(r.get("cpu_mean")), "✅" if r.get("stable") else "⚠️"]
        out.append(f"| {sc} | " + " | ".join(cells) + " |")
    out += ["", "_drift = RSS(end) − RSS(start); ~0 = no leak/growth over the window._", ""]

    errs = [r for r in rows if r.get("status") == "error"]
    out += ["## Errors / caveats", ""]
    out += ([f"- **{r['label']} n={r['stream_count']}**: {r.get('reason')} (no measurement)." for r in errs]
            or ["- None."])
    out += [""]
    return "\n".join(out)


def main():
    suite_path = sys.argv[1]
    s = Suite.load(suite_path)
    results_root = os.path.join("results", s.name)
    s, rows = build(suite_path, results_root)
    os.makedirs(results_root, exist_ok=True)
    fields = ["label", "stream_count", "throughput", "p50", "p99", "p999",
              "rss_peak_mb", "rss_drift_mb", "cpu_mean", "stable", "status", "reason"]
    with open(os.path.join(results_root, "aggregate.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fields})
    md = markdown(s, rows)
    with open(os.path.join(results_root, "report.md"), "w") as f:
        f.write(md)
    print(md)


if __name__ == "__main__":
    main()
