"""Per-label results-and-state store for the CATCH-UP / reconnect workload,
reproducing Ursula's published methodology: N clients each reconnect to their own
pre-populated stream and catch up via each system's native replay path (ursula
/bootstrap snapshot+tail; durable & s2 full-log). One entry per pre_events value.
Records per-client catch-up latency (p50/p99) + response body size."""
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


def record(path, pre_events, *, image_digest, clients, event_bytes, snapshot_bytes,
           pods, p50, p99, bytes_received_total, body_kb, mb_per_sec=None, status, reason):
    data = _load(path)
    data["cells"][str(pre_events)] = {
        "pre_events": pre_events,              # events per stream
        "image_digest": image_digest,
        "clients": clients,                    # concurrent reconnecting clients (own stream each)
        "event_bytes": event_bytes,
        "snapshot_bytes": snapshot_bytes,      # ursula snapshot payload (durable/s2 ignore -> full replay)
        "pods": pods,
        "p50": p50, "p99": p99,                # per-client catch-up latency (ms)
        "bytes_received_total": bytes_received_total,
        "body_kb": body_kb,                    # response body per client (KiB) — Ursula's body-size metric
        "mb_per_sec": mb_per_sec,              # aggregate replay throughput (MiB/s) across all clients
        "status": status,                      # "ok" | "error"
        "reason": reason,                      # "complete" | "creation_choke" | ...
    }
    _save(path, data)


def status_of(path, pre_events, image_digest):
    cell = _load(path)["cells"].get(str(pre_events))
    if cell is None or cell.get("image_digest") != image_digest:
        return "absent"
    if cell.get("status") == "error":
        return "error"
    if cell.get("status") == "ok":
        return "done"
    return "absent"


def all_cells(path):
    return list(_load(path)["cells"].values())
