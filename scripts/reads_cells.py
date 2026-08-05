"""Per-label results-and-state store for the READS scalability workload. One cell
per stream_count, holding a per-connection-level result map plus a `complete` flag
set once every configured connection level has been measured. Metric per sub-cell:
aggregate ops/s + bytes/s (read throughput) and p50/p99 latency, plus backpressure
(503/429) and other-error counts so overload cells are visible."""
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


def _cell(data, stream_count, image_digest):
    key = str(stream_count)
    cell = data["cells"].get(key)
    # A digest change invalidates the whole cell (new server image → re-measure).
    if cell is None or cell.get("image_digest") != image_digest:
        cell = {"stream_count": stream_count, "image_digest": image_digest,
                "complete": False, "connections": {}}
        data["cells"][key] = cell
    return cell


def record(path, stream_count, connections, *, image_digest, ops_per_sec,
           bytes_per_sec, p50, p99, backpressure, other_err, status, reason):
    data = _load(path)
    cell = _cell(data, stream_count, image_digest)
    cell["connections"][str(connections)] = {
        "connections": connections,
        "ops_per_sec": ops_per_sec,
        "bytes_per_sec": bytes_per_sec,
        "p50": p50, "p99": p99,
        "backpressure": backpressure,
        "other_err": other_err,
        "status": status,            # "ok" | "error"
        "reason": reason,
    }
    _save(path, data)


def conn_status(path, stream_count, connections, image_digest):
    cell = _load(path)["cells"].get(str(stream_count))
    if cell is None or cell.get("image_digest") != image_digest:
        return "absent"
    sub = cell["connections"].get(str(connections))
    return "done" if sub is not None and sub.get("status") == "ok" else "absent"


def mark_complete(path, stream_count, image_digest):
    data = _load(path)
    cell = data["cells"].get(str(stream_count))
    if cell is not None and cell.get("image_digest") == image_digest:
        cell["complete"] = True
        _save(path, data)


def status_of(path, stream_count, image_digest):
    cell = _load(path)["cells"].get(str(stream_count))
    if cell is None or cell.get("image_digest") != image_digest:
        return "absent"
    if not cell.get("complete"):
        return "absent"
    # A completed cell whose sweep left an errored connection level must NOT be
    # treated as "done" — the runner skips only on "done", so returning "error"
    # here lets a resume RE-RUN the cell and retry the failed levels (conn_status
    # returns "absent" for the errored sub-cells, "done" for the ok ones). This
    # mirrors catchup/sustained, which re-run error cells rather than stranding them.
    if any(sub.get("status") == "error" for sub in cell.get("connections", {}).values()):
        return "error"
    return "done"


def all_cells(path):
    return list(_load(path)["cells"].values())
