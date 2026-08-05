"""Report for the CATCH-UP / reconnect workload — Ursula's methodology
(https://ursula.tonbo.io/benchmark): N clients each reconnect to their own
pre-populated stream; each system uses its native replay path (ursula /bootstrap
snapshot+tail, durable & s2 full-log). Reports per-client catch-up p99 (lower is
better) + response body size, per stream length (pre_events) × system.

Usage: scripts/catchup_report.py <suite.json> [<suite2.json> ...]
       (pass catchup-durable + catchup-ursula + catchup-s2 for the 3-way compare)
"""
import sys, os, glob, csv
sys.path.insert(0, os.path.dirname(__file__))
from suite import Suite
import catchup_cells


def collect(suite_paths):
    rows, pre_set, labels = [], set(), []
    for sp in suite_paths:
        s = Suite.load(sp)
        rr = os.path.join("results", s.name)
        for label in s.labels():
            if label not in labels:
                labels.append(label)
            cells = {c["pre_events"]: c for c in catchup_cells.all_cells(
                os.path.join(rr, label, "cells.json"))}
            for pe in s.stream_counts:
                pre_set.add(pe)
                c = cells.get(pe)
                rows.append({**(c or {"pre_events": pe, "status": "missing"}), "label": label})
    return labels, sorted(pre_set), rows


def _g(x):
    return f"{x:g}" if isinstance(x, (int, float)) else "—"


def markdown(labels, pres, rows):
    by = {(r["label"], r["pre_events"]): r for r in rows}

    def col(pe, l, field):
        r = by.get((l, pe))
        if not r or r.get("status") in ("missing", None):
            return "—"
        if r.get("status") == "error":
            return f"ERR({r.get('reason')})"
        return _g(r.get(field))

    out = ["# Catch-up / reconnect — durable vs ursula vs s2", ""]
    out += ["_Reproduces Ursula's methodology (ursula.tonbo.io/benchmark): each client "
            "reconnects to its OWN pre-populated stream and catches up via that system's "
            "native path — **ursula** `GET /bootstrap` (snapshot+tail), **durable** `offset=-1` "
            "and **s2** `/records` (full log). Equal hardware (1 server node each — Ursula's "
            "published run gave ursula 3 nodes vs 1 for DS/S2)._", ""]

    out += ["## Per-client catch-up p99 latency (ms) — lower is faster", ""]
    out += ["| pre_events (stream) | " + " | ".join(labels) + " |", "|" + "---|" * (len(labels) + 1)]
    for pe in pres:
        out.append(f"| {pe} | " + " | ".join(col(pe, l, "p99") for l in labels) + " |")
    out += ["", "## p50 latency (ms)", ""]
    out += ["| pre_events | " + " | ".join(labels) + " |", "|" + "---|" * (len(labels) + 1)]
    for pe in pres:
        out.append(f"| {pe} | " + " | ".join(col(pe, l, "p50") for l in labels) + " |")
    out += ["", "## Aggregate replay throughput (MiB/s) — total bytes served / stampede time", ""]
    out += ["| pre_events | " + " | ".join(labels) + " |", "|" + "---|" * (len(labels) + 1)]
    for pe in pres:
        out.append(f"| {pe} | " + " | ".join(col(pe, l, "mb_per_sec") for l in labels) + " |")
    out += ["", "## Response body per client (KiB) — smaller = less to transfer", ""]
    out += ["| pre_events | " + " | ".join(labels) + " |", "|" + "---|" * (len(labels) + 1)]
    for pe in pres:
        out.append(f"| {pe} | " + " | ".join(col(pe, l, "body_kb") for l in labels) + " |")
    out += [""]

    errs = [r for r in rows if r.get("status") == "error"]
    out += ["## Errors / caveats", ""]
    out += ([f"- **{r['label']} pre_events={r['pre_events']}**: {r.get('reason')}." for r in errs] or ["- None."])
    if rows:
        r0 = next((r for r in rows if r.get("status") == "ok"), rows[0])
        out += ["", f"_clients={r0.get('clients','?')} (each own stream), event_bytes={r0.get('event_bytes','?')}, "
                f"snapshot_bytes={r0.get('snapshot_bytes','?')} (ursula only)._"]
    return "\n".join(out)


def main():
    paths = sys.argv[1:] or sorted(glob.glob("suites/catchup-*.json"))
    labels, pres, rows = collect(paths)
    out_dir = "results/catchup"
    os.makedirs(out_dir, exist_ok=True)
    fields = ["label", "pre_events", "clients", "event_bytes", "snapshot_bytes",
              "p50", "p99", "body_kb", "mb_per_sec", "bytes_received_total", "status", "reason"]
    with open(os.path.join(out_dir, "aggregate.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fields})
    md = markdown(labels, pres, rows)
    with open(os.path.join(out_dir, "report.md"), "w") as f:
        f.write(md)
    print(md)


if __name__ == "__main__":
    main()
