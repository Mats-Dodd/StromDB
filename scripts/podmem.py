"""Backfill peak + p50 pod working-set memory into write-throughput cells from the
saved per-tick samples.csv.

The live walk records pod_mem_mb as the high-water across the whole ladder. This
recomputes it from the PINNED rung's samples only (the saturation point), and adds
pod_mem_p50_mb (the median = typical/steady-state memory under saturating load).
Peak captures bursts (e.g. an in-RAM Raft log filling during a write storm); p50
captures what the server steadily holds resident. Idempotent; safe to re-run.

Usage: scripts/podmem.py results/<suite> [results/<suite> ...]
       scripts/podmem.py            # defaults to every results/run-* dir
"""
import sys, os, glob, json, statistics


def _ws_samples(csv_path):
    out = []
    try:
        with open(csv_path) as f:
            for ln in f.read().splitlines()[1:]:
                p = ln.split(",")
                if len(p) >= 5 and p[4].strip().lstrip("-").isdigit():
                    out.append(int(p[4]))
    except OSError:
        pass
    return out


def backfill_suite(suite_root):
    n = 0
    for cellsf in glob.glob(os.path.join(suite_root, "*", "cells.json")):
        samples_root = os.path.join(os.path.dirname(cellsf), "cells")
        data = json.load(open(cellsf))
        for sc, c in data["cells"].items():
            pp = c.get("pinned_pods")
            if pp is None:
                continue
            ws = []
            for sf in glob.glob(os.path.join(samples_root, "*", f"n{sc}", f"p{pp}-r*", "samples.csv")):
                ws += _ws_samples(sf)
            if not ws:
                continue
            c["pod_mem_mb"] = round(max(ws) / 1048576)            # peak (saturation high-water)
            c["pod_mem_p50_mb"] = round(statistics.median(ws) / 1048576)  # typical/steady
            n += 1
        tmp = cellsf + ".tmp"
        json.dump(data, open(tmp, "w"), indent=2)
        os.replace(tmp, cellsf)
    return n


def main():
    roots = sys.argv[1:] or sorted(glob.glob("results/run-*"))
    total = 0
    for r in roots:
        k = backfill_suite(r)
        total += k
        print(f"  {r}: {k} cells updated (peak + p50)")
    print(f"done — {total} cells")


if __name__ == "__main__":
    main()
