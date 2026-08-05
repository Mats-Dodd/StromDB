"""SSE delivery-latency report from gke-bench run dirs. Reads each SSE cell's
merged.json (richer than summary.tsv — has the full percentile set) and reports
**p50** (median) as the headline, plus p90/p99/p999/max. SSE is writer-paced, so
latency (not throughput) is the metric.

Usage: python3 sse_report.py <run-dir-or-merged.json>... [out_basename]
  run-dir: a results/bench/bench-* dir (globs its */rep*/merged.json).
Writes results/<out>.md + results/<out>.csv (default out=sse-comparison)."""
import sys, os, csv, re, glob, json, statistics
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from podmem import _ws_samples   # per-tick pod_ws_bytes reader (col 5 of samples.csv)

LABEL = {
    ("durable", "walnew"): "wal (cache off)",
    ("durable", "walnew-cache"): "wal (cache on)",
    ("ursula", "memory"): "ursula in-memory",
    ("ursula", "disk"): "ursula disk",
    ("s2", "_"): "s2",
}
ORDER = ["wal (cache off)", "wal (cache on)", "ursula in-memory", "ursula disk", "s2"]
# cell dir looks like:  <sys>-<var>-sse-m1t<T>-r<rep>
CELL_RE = re.compile(r"^(?P<sys>[a-z0-9]+)-(?P<var>.+)-sse-m\d+t(?P<subs>\d+)-r\d+$")


def _last_merged_obj(path):
    t = open(path).read()
    for o in reversed(re.findall(r"\{.*?\}", t, re.S)):
        try:
            d = json.loads(o)
            if isinstance(d, dict) and "p50_ms" in d:
                return d
        except Exception:
            pass
    return None


def parse(paths):
    files = []
    for p in paths:
        if os.path.isdir(p):
            files += glob.glob(os.path.join(p, "*", "rep*", "merged.json"))
            files += glob.glob(os.path.join(p, "*", "merged.json"))
        elif p.endswith("merged.json"):
            files.append(p)
    rows, seen = [], set()
    for f in files:
        cell = os.path.basename(os.path.dirname(os.path.dirname(f)))
        m = CELL_RE.match(cell)
        if not m:
            continue
        d = _last_merged_obj(f)
        if not d:
            continue
        label = LABEL.get((m["sys"], m["var"]), f'{m["sys"]}:{m["var"]}')
        subs = int(m["subs"])
        key = (label, subs)
        if key in seen:
            rows = [x for x in rows if (x["config"], x["subs"]) != key]
        seen.add(key)
        # Pod working-set memory during this subscriber-count cell (samples.csv sits
        # next to merged.json). Lets us see whether the server's fan-out buffer is
        # SHARED (memory flat as subscribers grow) or PER-SUBSCRIBER (memory grows).
        ws = _ws_samples(os.path.join(os.path.dirname(f), "samples.csv"))
        pod_peak = round(max(ws) / 1048576) if ws else None
        pod_p50 = round(statistics.median(ws) / 1048576) if ws else None
        rows.append({"config": label, "subs": subs,
                     "p50_ms": d.get("p50_ms"), "p90_ms": d.get("p90_ms"),
                     "p99_ms": d.get("p99_ms"), "p999_ms": d.get("p999_ms"),
                     "max_ms": d.get("max_ms"),
                     "pod_mem_mb": pod_peak, "pod_mem_p50_mb": pod_p50,
                     "events_per_sec": round(d.get("aggregate_events_per_sec", 0), 1)})
    return rows


def markdown(rows):
    subs_vals = sorted({r["subs"] for r in rows})
    configs = [c for c in ORDER if any(r["config"] == c for r in rows)]
    configs += [c for c in sorted({r["config"] for r in rows}) if c not in configs]
    by = {(r["config"], r["subs"]): r for r in rows}
    out = ["# SSE Fan-out — delivery latency", "",
           "1 stream, 1 writer @ 50 ev/s, swept total subscribers; one well-provisioned",
           "client pod (single wall clock). Writer-paced → metric is delivery latency.", ""]
    out += ["## Median (p50, ms)", "",
            "| config \\ subscribers | " + " | ".join(str(s) for s in subs_vals) + " |",
            "|" + "---|" * (len(subs_vals) + 1)]
    for c in configs:
        cells = [(f'{by[(c,s)]["p50_ms"]}' if (c, s) in by and by[(c, s)]["p50_ms"] is not None else "—")
                 for s in subs_vals]
        out.append(f"| {c} | " + " | ".join(cells) + " |")
    out += ["", "## Full spread (p50 / p99 / max, ms)", "",
            "| config | subs | p50 | p90 | p99 | p999 | max |", "|---|---|---|---|---|---|---|"]
    for c in configs:
        for s in subs_vals:
            r = by.get((c, s))
            if r:
                out.append(f"| {c} | {s} | {r['p50_ms']} | {r['p90_ms']} | {r['p99_ms']} | {r['p999_ms']} | {r['max_ms']} |")
    # Pod memory vs subscriber count — the shared-vs-per-subscriber buffer test.
    if any(r.get("pod_mem_mb") for r in rows):
        out += ["", "## Pod memory vs subscribers — peak / p50 (MiB)", "",
                "| config \\ subscribers | " + " | ".join(str(s) for s in subs_vals) + " |",
                "|" + "---|" * (len(subs_vals) + 1)]
        for c in configs:
            cells = []
            for s in subs_vals:
                r = by.get((c, s))
                if r and r.get("pod_mem_mb") is not None:
                    pk, md = r["pod_mem_mb"], r.get("pod_mem_p50_mb")
                    cells.append(f"{pk} / {md}" if md is not None else f"{pk}")
                else:
                    cells.append("—")
            out.append(f"| {c} | " + " | ".join(cells) + " |")
        out += ["", "_Pod working set (cgroup `memory.current − inactive_file`) during each "
                "subscriber-count cell. **Flat across the row ⇒ a shared fan-out buffer** (one "
                "resident tail served to all subscribers); growth ⇒ per-subscriber buffering._", ""]
    out += ["", "_p50 = median; lower is better. — = not measured._", ""]
    return "\n".join(out)


def main():
    paths = [a for a in sys.argv[1:] if os.path.exists(a)]
    nonpaths = [a for a in sys.argv[1:] if not os.path.exists(a)]
    out = nonpaths[0] if nonpaths else "sse-comparison"
    if not paths:
        print("usage: sse_report.py <run-dir-or-merged.json>... [out_basename]"); sys.exit(2)
    rows = parse(paths)
    os.makedirs("results", exist_ok=True)
    with open(f"results/{out}.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["config", "subs", "p50_ms", "p90_ms", "p99_ms", "p999_ms", "max_ms", "pod_mem_mb", "pod_mem_p50_mb", "events_per_sec"])
        w.writeheader()
        for r in sorted(rows, key=lambda r: (r["config"], r["subs"])):
            w.writerow(r)
    with open(f"results/{out}.md", "w") as f:
        f.write(markdown(rows))
    print(f"wrote results/{out}.md, results/{out}.csv ({len(rows)} sse cells)")


if __name__ == "__main__":
    main()
