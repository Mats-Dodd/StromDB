"""Per-label results-and-state store for the SUSTAINED workload. One JSON file
per label; one entry per stream_count. Like cells.py but the cell records a
single fixed-rate, long-duration measurement (no pod ladder / no plateau) plus
server-memory stability (RSS peak/drift) and CPU steadiness."""
import json, os


def _load(path):
    if not os.path.exists(path):
        return {"cells": {}}
    with open(path) as f:
        return json.load(f)


def _save(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def record(path, stream_count, *, image_digest, pods, rate_per_stream, duration_secs,
           throughput, p50, p99, p999, rss_peak_mb, rss_drift_mb, cpu_mean, stable,
           status, reason):
    data = _load(path)
    data["cells"][str(stream_count)] = {
        "stream_count": stream_count,
        "image_digest": image_digest,
        "pods": pods,
        "rate_per_stream": rate_per_stream,   # offered ops/s per stream (held steady)
        "duration_secs": duration_secs,
        "throughput": throughput,             # aggregate ops/s actually achieved
        "p50": p50, "p99": p99, "p999": p999, # append latency (ms) over the window
        "rss_peak_mb": rss_peak_mb,           # server peak RSS (MiB)
        "rss_drift_mb": rss_drift_mb,         # server RSS end-start (MiB); ~0 = stable
        "cpu_mean": cpu_mean,                 # mean server CPU% over the window
        "stable": stable,                     # bool: latency/RSS held steady
        "status": status,                     # "ok" | "error"
        "reason": reason,                     # "complete" | "creation_choke" | ...
    }
    _save(path, data)


def status_of(path, stream_count, image_digest):
    cell = _load(path)["cells"].get(str(stream_count))
    if cell is None or cell.get("image_digest") != image_digest:
        return "absent"
    if cell.get("status") == "error":
        return "error"
    if cell.get("status") == "ok":
        return "done"          # single-shot cell already measured cleanly -> skip
    return "absent"


def all_cells(path):
    return list(_load(path)["cells"].values())
