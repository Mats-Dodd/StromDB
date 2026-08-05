"""Render the READS scalability grid: per server-config label, a
stream_count × connections table of read throughput (MiB/s) + p99 latency, with
the peak-throughput connection level flagged per cardinality and overload cells
(backpressure > 0) marked. Deterministic; no cluster needed."""
import sys, os, json, csv
sys.path.insert(0, os.path.dirname(__file__))
from suite import Suite
import reads_cells

MIB = 1024.0 * 1024.0


def peak_throughput(cell):
    """(connections, bytes_per_sec) of the highest-throughput sub-cell in a cell."""
    best = None
    for sub in cell["connections"].values():
        bps = sub.get("bytes_per_sec") or 0
        if best is None or bps > best[1]:
            best = (sub["connections"], bps)
    return best if best is not None else (None, 0)


def build(suite_path, results_root):
    s = Suite.load(suite_path)
    conns = s.reads.get("connection_levels", [])
    rows = []
    by = {}  # (label, sc, conn) -> sub-cell
    for label in s.labels():
        p = os.path.join(results_root, label, "cells.json")
        if not os.path.exists(p):
            continue
        for c in reads_cells.all_cells(p):
            peak_conn, peak_bps = peak_throughput(c)
            for sub in c["connections"].values():
                by[(label, c["stream_count"], sub["connections"])] = sub
                rows.append({
                    "mode": label, "stream_count": c["stream_count"],
                    "connections": sub["connections"],
                    "ops_per_sec": sub.get("ops_per_sec"),
                    "bytes_per_sec": sub.get("bytes_per_sec"),
                    "mib_per_sec": round((sub.get("bytes_per_sec") or 0) / MIB, 1),
                    "p50": sub.get("p50"), "p99": sub.get("p99"),
                    "backpressure": sub.get("backpressure"),
                    "other_err": sub.get("other_err"),
                    "status": sub.get("status"),
                    "is_peak": sub["connections"] == peak_conn,
                })
    rows.sort(key=lambda r: (r["mode"], r["stream_count"], r["connections"]))
    return rows, _markdown(s, conns, by)


def _cell_str(sub):
    if sub is None:
        return "—"
    if sub.get("status") == "error":
        return f"ERR({sub.get('other_err', 0)})"
    mib = (sub.get("bytes_per_sec") or 0) / MIB
    p99 = sub.get("p99")
    mark = "‡" if (sub.get("backpressure") or 0) > 0 else ""  # ‡ = backpressure seen
    p99s = f"{p99:.0f}" if p99 is not None else "?"
    return f"{mib:.0f}MiB/s@{p99s}ms{mark}"


def _markdown(s, conns, by):
    labels = s.labels()
    out = [f"# {s.name} — read-scalability report", ""]
    out += ["Each cell: aggregate read throughput (MiB/s) @ p99 latency (ms). "
            "‡ = backpressure (503/429) observed at this load.", ""]
    for label in labels:
        out += [f"## {label} — throughput @ p99 over stream_count × connections", ""]
        header = "| streams | " + " | ".join(str(c) for c in conns) + " |"
        out += [header, "|" + "---|" * (len(conns) + 1)]
        for sc in s.stream_counts:
            cells_row = [_cell_str(by.get((label, sc, c))) for c in conns]
            out.append(f"| {sc} | " + " | ".join(cells_row) + " |")
        out += [""]
        # Peak read throughput per cardinality (the plateau point).
        out += ["Peak read throughput per cardinality:"]
        for sc in s.stream_counts:
            subs = [by.get((label, sc, c)) for c in conns if by.get((label, sc, c))]
            if not subs:
                out.append(f"- streams={sc}: —"); continue
            best = max(subs, key=lambda x: x.get("bytes_per_sec") or 0)
            out.append(f"- streams={sc}: {(best.get('bytes_per_sec') or 0)/MIB:.0f} MiB/s "
                       f"at {best['connections']} connections")
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
    fields = ["mode", "stream_count", "connections", "ops_per_sec", "bytes_per_sec",
              "mib_per_sec", "p50", "p99", "backpressure", "other_err", "status", "is_peak"]
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
