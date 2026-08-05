"""Render the MIXED interference grid: per server-config label and stream_count,
one row per sweep level showing every class side by side — write ops/s + p50/p99,
catch-up read ops/s + p50/p99, delivery (fan-out) events/s + p50/p99 — so the
interference direction under test (readers→writes or writes→delivery) reads
straight down a column. Deterministic; no cluster needed."""
import sys, os, json, csv
sys.path.insert(0, os.path.dirname(__file__))
from suite import Suite
import mixed_cells


def build(suite_path, results_root):
    s = Suite.load(suite_path)
    levels = s.mixed.get("levels", [])
    axis = s.mixed.get("sweep", "readers")
    rows = []
    by = {}  # (label, sc, level) -> sub-cell
    for label in s.labels():
        p = os.path.join(results_root, label, "cells.json")
        if not os.path.exists(p):
            continue
        for c in mixed_cells.all_cells(p):
            for sub in c["levels"].values():
                by[(label, c["stream_count"], sub["level"])] = sub
                rows.append({
                    "mode": label, "stream_count": c["stream_count"],
                    "sweep": axis, "level": sub["level"],
                    "readers": sub.get("readers"),
                    "subscribers": sub.get("subscribers"),
                    "writer_rate": sub.get("writer_rate"),
                    "read_rate": sub.get("read_rate"),
                    "write_ops_per_sec": sub.get("write_ops_per_sec"),
                    "write_p50": sub.get("write_p50"), "write_p99": sub.get("write_p99"),
                    "read_ops_per_sec": sub.get("read_ops_per_sec"),
                    "read_mib_per_sec": sub.get("read_mib_per_sec"),
                    "read_p50": sub.get("read_p50"), "read_p99": sub.get("read_p99"),
                    "events_per_sec": sub.get("events_per_sec"),
                    "delivery_p50": sub.get("delivery_p50"),
                    "delivery_p99": sub.get("delivery_p99"),
                    "write_bp": sub.get("write_bp"), "write_err": sub.get("write_err"),
                    "read_bp": sub.get("read_bp"), "read_err": sub.get("read_err"),
                    "status": sub.get("status"), "reason": sub.get("reason"),
                })
    rows.sort(key=lambda r: (r["mode"], r["stream_count"], r["level"]))
    return rows, _markdown(s, axis, levels, by)


def _n(x, fmt="{:.0f}"):
    return fmt.format(x) if isinstance(x, (int, float)) else "—"


def _lat(p50, p99):
    if not isinstance(p50, (int, float)) and not isinstance(p99, (int, float)):
        return "—"
    return f"{_n(p50, '{:.1f}')}/{_n(p99, '{:.1f}')}"


def _level_name(axis, level):
    # writer_rate 0 = unthrottled writers (`mixed --writer-rate 0` hot loop).
    if axis == "writer_rate" and level == 0:
        return "max"
    return str(level)


def _markdown(s, axis, levels, by):
    labels = s.labels()
    axis_col = {"readers": "readers", "writer_rate": "rate/writer"}.get(axis, axis)
    out = [f"# {s.name} — mixed read/write interference report", ""]
    out += [f"Sweep axis: **{axis}**. Latency cells are p50/p99 ms. "
            "‡ = backpressure (503/429) observed in that class.", ""]
    for label in labels:
        for sc in s.stream_counts:
            out += [f"## {label} — {sc} streams", ""]
            out += [f"| {axis_col} | write ops/s | write ms | read ops/s | read MiB/s | read ms "
                    "| deliv rec/s | deliv ms | status |",
                    "|---|---|---|---|---|---|---|---|---|"]
            for lv in levels:
                sub = by.get((label, sc, lv))
                if sub is None:
                    out.append(f"| {_level_name(axis, lv)} | — | — | — | — | — | — | — | absent |")
                    continue
                wmark = "‡" if (sub.get("write_bp") or 0) > 0 else ""
                rmark = "‡" if (sub.get("read_bp") or 0) > 0 else ""
                out.append(
                    f"| {_level_name(axis, lv)} "
                    f"| {_n(sub.get('write_ops_per_sec'))}{wmark} "
                    f"| {_lat(sub.get('write_p50'), sub.get('write_p99'))} "
                    f"| {_n(sub.get('read_ops_per_sec'))}{rmark} "
                    f"| {_n(sub.get('read_mib_per_sec'), '{:.1f}')} "
                    f"| {_lat(sub.get('read_p50'), sub.get('read_p99'))} "
                    f"| {_n(sub.get('events_per_sec'))} "
                    f"| {_lat(sub.get('delivery_p50'), sub.get('delivery_p99'))} "
                    f"| {sub.get('status')} |")
            out += [""]
    out += ["## Findings", "", "_TODO: written by hand on top of the generated data._", ""]
    return "\n".join(out)


def main():
    suite_path = sys.argv[1]
    s = Suite.load(suite_path)
    root = os.path.join("results", s.name)
    rows, md = build(suite_path, root)
    os.makedirs(root, exist_ok=True)
    with open(os.path.join(root, "aggregate.json"), "w") as f:
        json.dump(rows, f, indent=2)
    fields = ["mode", "stream_count", "sweep", "level", "readers", "subscribers",
              "writer_rate", "read_rate", "write_ops_per_sec", "write_p50", "write_p99",
              "read_ops_per_sec", "read_mib_per_sec", "read_p50", "read_p99",
              "events_per_sec", "delivery_p50", "delivery_p99", "write_bp", "write_err",
              "read_bp", "read_err", "status", "reason"]
    with open(os.path.join(root, "aggregate.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in fields})
    with open(os.path.join(root, "report.md"), "w") as f:
        f.write(md)
    print(f"wrote {root}/aggregate.csv, aggregate.json, report.md")


if __name__ == "__main__":
    main()
