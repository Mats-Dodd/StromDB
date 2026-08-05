"""Per-label results-and-state store for the MIXED read/write workload. One cell
per stream_count, holding a per-sweep-level result map plus a `complete` flag set
once every configured level has been measured. A level is one mixed run (writers +
catch-up readers + SSE subscribers over shared streams); its metrics carry all
three classes so the report can show interference: write ops/s + p50/p99, read
ops/s + p50/p99, delivery (fan-out) p50/p99 + events/s, and per-class error counts.

The coordinator's merged.json for a mixed cell is a per-class composite (see
lib-mixed.sh): {"write": <MergeSummary>, "fanout": ..., "read": ..., "pods":
[<per-pod MixedResult>...]} — preceded by mc chatter lines, so parsing starts at
the '{"write":' marker."""
import json, os

MARKER = '{"write":'


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
                "complete": False, "levels": {}}
        data["cells"][key] = cell
    return cell


def parse_merged(text):
    """Extract the composite JSON from coordinator stdout (which begins with mc
    alias/cp chatter). Returns the dict or None when the marker is absent (e.g.
    the coordinator's no_client_results_uploaded error doc)."""
    i = text.find(MARKER)
    if i < 0:
        return None
    try:
        return json.loads(text[i:])
    except ValueError:
        return None


def _pos(x):
    """A percentile from an EMPTY merged histogram is 0.0 — treat as absent."""
    return x if isinstance(x, (int, float)) and x > 0 else None


def metrics_from_merged(doc):
    """Flatten the per-class composite into the metrics dict a level stores.
    Rates for read/fanout come from the per-pod MixedResults (summed counts over
    the drive window — barrier release → shared deadline; older pods without
    drive_secs fall back to elapsed_secs); write ops/s comes from the
    coordinator's --results-dir scan (sum of per-pod aggregate_ops_per_sec).
    events_received counts timestamped data frames only (true per-record
    deliveries); per-batch SSE control frames are carried separately."""
    pods = doc.get("pods") or []
    window = max((p.get("drive_secs") or p.get("elapsed_secs") or 0 for p in pods), default=0)

    def csum(field, sub):
        return sum(((p.get(field) or {}).get(sub) or 0) for p in pods)

    write, fanout, read = doc.get("write") or {}, doc.get("fanout") or {}, doc.get("read") or {}
    read_ok = csum("read_counts", "ok")
    read_bytes = sum((p.get("read_bytes_total") or 0) for p in pods)
    events = sum((p.get("events_received") or 0) for p in pods)
    return {
        "write_ops_per_sec": write.get("aggregate_ops_per_sec"),
        "write_p50": _pos(write.get("p50_ms")), "write_p99": _pos(write.get("p99_ms")),
        "read_ops_per_sec": (read_ok / window) if window > 0 else None,
        "read_mib_per_sec": (read_bytes / window / 1048576.0) if window > 0 else None,
        "read_p50": _pos(read.get("p50_ms")), "read_p99": _pos(read.get("p99_ms")),
        "events_per_sec": (events / window) if window > 0 else None,
        "delivery_p50": _pos(fanout.get("p50_ms")), "delivery_p99": _pos(fanout.get("p99_ms")),
        "write_ok": csum("write_counts", "ok"),
        "write_bp": csum("write_counts", "backpressure"),
        "write_err": csum("write_counts", "other_err"),
        "read_ok": read_ok,
        "read_bp": csum("read_counts", "backpressure"),
        "read_err": csum("read_counts", "other_err"),
        "events_received": events,
        "control_events_received": sum((p.get("control_events_received") or 0) for p in pods),
        "elapsed_secs": max((p.get("elapsed_secs") or 0 for p in pods), default=0),
        "drive_secs": window,
    }


def record_merged(path, stream_count, level, *, image_digest, merged_path,
                  readers, subscribers, writer_rate, read_rate=0):
    """Parse a cell's merged.json and record the level. Validity: writers must
    have appended; a level that configured readers/subscribers but saw none of
    their traffic is an error (so a resume re-runs it)."""
    try:
        with open(merged_path) as f:
            doc = parse_merged(f.read())
    except OSError:
        doc = None
    if doc is None:
        m, status, reason = {}, "error", "no_merged_result"
    else:
        m = metrics_from_merged(doc)
        if not m.get("write_ok"):
            status, reason = "error", "no_writes"
        elif readers > 0 and not m.get("read_ok"):
            status, reason = "error", "no_reads"
        elif subscribers > 0 and not m.get("events_received"):
            status, reason = "error", "no_delivery"
        else:
            status, reason = "ok", "complete"
    data = _load(path)
    cell = _cell(data, stream_count, image_digest)
    cell["levels"][str(level)] = dict(
        m, level=level, readers=readers, subscribers=subscribers,
        writer_rate=writer_rate, read_rate=read_rate, status=status, reason=reason)
    _save(path, data)
    return status


def level_status(path, stream_count, level, image_digest):
    cell = _load(path)["cells"].get(str(stream_count))
    if cell is None or cell.get("image_digest") != image_digest:
        return "absent"
    sub = cell["levels"].get(str(level))
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
    # Mirror reads_cells: a completed cell with an errored level returns "error"
    # so a resume re-runs the cell and retries just the failed levels.
    if any(sub.get("status") == "error" for sub in cell.get("levels", {}).values()):
        return "error"
    return "done"


def all_cells(path):
    return list(_load(path)["cells"].values())
