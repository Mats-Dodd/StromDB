"""Unit tests for mixed_cells: merged-composite parsing (with the coordinator's
mc chatter prefix), metric derivation from per-pod MixedResults, validity rules
(no_writes / no_reads / no_delivery), and the resume state machine."""
import json, os, sys, tempfile

sys.path.insert(0, os.path.dirname(__file__))
import mixed_cells


def _summary(p50=1.0, p99=5.0, ops=None, count=100):
    d = {"merged_count": count, "p50_ms": p50, "p90_ms": p99, "p99_ms": p99,
         "p999_ms": p99, "max_ms": p99}
    if ops is not None:
        d["aggregate_ops_per_sec"] = ops
    return d


def _pod(write_ok=1000, read_ok=50, events=200, elapsed=25.0, drive=20.0):
    return {"scenario": "mixed", "elapsed_secs": elapsed, "drive_secs": drive,
            "write_counts": {"ok": write_ok, "backpressure": 1, "other_err": 0},
            "read_counts": {"ok": read_ok, "backpressure": 0, "other_err": 2},
            "events_received": events, "control_events_received": events,
            "read_bytes_total": 20 * 1048576}


def _merged_text(write_ok=1000, read_ok=50, events=200, ops=50.0):
    doc = {"write": _summary(ops=ops), "fanout": _summary(p50=0.4, p99=2.0),
           "read": _summary(p50=3.0, p99=9.0), "pods": [_pod(write_ok, read_ok, events)]}
    body = json.dumps(doc)
    assert body.startswith(mixed_cells.MARKER)
    return "Added `local` successfully.\n`x.hdr` -> `/merge/x.hdr`\n" + body


def test_parse_skips_chatter():
    doc = mixed_cells.parse_merged(_merged_text())
    assert doc is not None and doc["write"]["merged_count"] == 100
    assert mixed_cells.parse_merged('{"error":"no_client_results_uploaded"}') is None
    assert mixed_cells.parse_merged("") is None


def test_metrics_derivation():
    m = mixed_cells.metrics_from_merged(mixed_cells.parse_merged(_merged_text()))
    assert m["write_ops_per_sec"] == 50.0
    # Rates divide by the drive window (20s), not full elapsed (25s).
    assert m["read_ops_per_sec"] == 50 / 20.0
    assert m["events_per_sec"] == 200 / 20.0
    assert m["read_mib_per_sec"] == 1.0
    assert m["control_events_received"] == 200
    assert m["write_bp"] == 1 and m["read_err"] == 2
    assert m["delivery_p99"] == 2.0 and m["read_p50"] == 3.0


def test_window_falls_back_to_elapsed():
    doc = mixed_cells.parse_merged(_merged_text())
    for p in doc["pods"]:
        del p["drive_secs"]
    m = mixed_cells.metrics_from_merged(doc)
    assert m["read_ops_per_sec"] == 50 / 25.0


def test_zero_percentiles_absent():
    # An empty class histogram merges to 0.0 percentiles — must read as absent.
    text = _merged_text()
    doc = mixed_cells.parse_merged(text)
    doc["read"] = _summary(p50=0.0, p99=0.0, count=0)
    m = mixed_cells.metrics_from_merged(doc)
    assert m["read_p50"] is None and m["read_p99"] is None


def _record(tmp, text, readers=4, subscribers=2, level=4):
    cells = os.path.join(tmp, "cells.json")
    mp = os.path.join(tmp, "merged.json")
    with open(mp, "w") as f:
        f.write(text)
    st = mixed_cells.record_merged(cells, 50, level, image_digest="d1",
                                   merged_path=mp, readers=readers,
                                   subscribers=subscribers, writer_rate=40)
    return cells, st


def test_record_and_resume():
    with tempfile.TemporaryDirectory() as tmp:
        cells, st = _record(tmp, _merged_text())
        assert st == "ok"
        assert mixed_cells.level_status(cells, 50, 4, "d1") == "done"
        assert mixed_cells.level_status(cells, 50, 8, "d1") == "absent"
        assert mixed_cells.level_status(cells, 50, 4, "OTHER") == "absent"
        assert mixed_cells.status_of(cells, 50, "d1") == "absent"  # not complete yet
        mixed_cells.mark_complete(cells, 50, "d1")
        assert mixed_cells.status_of(cells, 50, "d1") == "done"


def test_validity_rules():
    with tempfile.TemporaryDirectory() as tmp:
        _, st = _record(tmp, _merged_text(write_ok=0))
        assert st == "error"  # no_writes
    with tempfile.TemporaryDirectory() as tmp:
        _, st = _record(tmp, _merged_text(read_ok=0))
        assert st == "error"  # readers configured but silent
    with tempfile.TemporaryDirectory() as tmp:
        _, st = _record(tmp, _merged_text(read_ok=0), readers=0, subscribers=0)
        assert st == "ok"     # no readers configured → no read traffic expected
    with tempfile.TemporaryDirectory() as tmp:
        _, st = _record(tmp, '{"error":"no_client_results_uploaded"}')
        assert st == "error"  # no_merged_result


def test_error_level_makes_cell_resumable():
    with tempfile.TemporaryDirectory() as tmp:
        cells, _ = _record(tmp, _merged_text(), level=4)
        mp = os.path.join(tmp, "m2.json")
        with open(mp, "w") as f:
            f.write('{"error":"no_client_results_uploaded"}')
        mixed_cells.record_merged(cells, 50, 16, image_digest="d1", merged_path=mp,
                                  readers=16, subscribers=2, writer_rate=40)
        mixed_cells.mark_complete(cells, 50, "d1")
        assert mixed_cells.status_of(cells, 50, "d1") == "error"
        assert mixed_cells.level_status(cells, 50, 4, "d1") == "done"
        assert mixed_cells.level_status(cells, 50, 16, "d1") == "absent"


if __name__ == "__main__":
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS: {name}")
