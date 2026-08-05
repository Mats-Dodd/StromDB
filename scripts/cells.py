"""Per-mode results-and-state store. One JSON file per mode; one entry per
stream_count. Replaces pins.json -- this file is both the resume state and the
report source."""
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


def record(path, stream_count, *, image_digest, walk, pinned_pods, throughput,
           p50, p99, saturated, status, reason, pod_mem_mb=None):
    data = _load(path)
    data["cells"][str(stream_count)] = {
        "stream_count": stream_count,
        "image_digest": image_digest,
        "walk": walk,                # [[pods, throughput], ...]
        "pinned_pods": pinned_pods,
        "throughput": throughput,
        "p50": p50,                  # median latency (ms) at the pinned point
        "p99": p99,                  # tail latency (ms) at the pinned point
        "pod_mem_mb": pod_mem_mb,    # peak pod working-set memory (MiB) during the cell
        "saturated": saturated,
        "status": status,            # "ok" | "error"
        "reason": reason,            # "plateau" | "cpu" | "ladder_exhausted" | "creation_choke" | ...
    }
    _save(path, data)


def status_of(path, stream_count, image_digest):
    cell = _load(path)["cells"].get(str(stream_count))
    if cell is None or cell.get("image_digest") != image_digest:
        return "absent"
    if cell.get("status") == "error":
        return "error"
    if cell.get("saturated"):
        return "saturated"
    return "resume"


def all_cells(path):
    return list(_load(path)["cells"].values())
