import os, sys, json, tempfile, unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import report

def _setup():
    # Self-contained fixture: a temp suite (modes wal/ursula, no server_configs)
    # plus matching per-mode cells.json. Returns (suite_path, results_root).
    root = tempfile.mkdtemp()
    store = {}
    for mode, sc, thr, sat in [("wal", 1000, 510000, True), ("wal", 100000, 0, False),
                               ("ursula", 1000, 62000, True)]:
        status = "error" if (mode == "wal" and sc == 100000) else "ok"
        store.setdefault(mode, {})[str(sc)] = {"stream_count": sc, "throughput": thr,
            "p50": 2.0, "p99": 2.0, "pinned_pods": 16, "saturated": sat, "status": status,
            "reason": "plateau", "walk": [[16, thr]], "image_digest": "x"}
    for mode, cells in store.items():
        d = os.path.join(root, mode); os.makedirs(d, exist_ok=True)
        json.dump({"cells": cells}, open(os.path.join(d, "cells.json"), "w"))
    suite = os.path.join(tempfile.mkdtemp(), "s.json")
    json.dump({"suite": "rpt", "modes": ["wal", "ursula"],
               "stream_counts": [1000, 100000], "cluster": {}, "saturation": {},
               "pod_ladder": {"1000": [16], "100000": [16]}}, open(suite, "w"))
    return suite, root


class TestReport(unittest.TestCase):
    def test_aggregate_rows(self):
        suite, root = _setup()
        rows, md = report.build(suite, root)
        wal1k = [r for r in rows if r["mode"] == "wal" and r["stream_count"] == 1000][0]
        self.assertEqual(wal1k["throughput"], 510000)
        self.assertIs(wal1k["saturated"], True)
        err = [r for r in rows if r["mode"] == "wal" and r["stream_count"] == 100000][0]
        self.assertEqual(err["status"], "error")

    def test_markdown_has_table_and_headers(self):
        suite, root = _setup()
        _, md = report.build(suite, root)
        self.assertTrue("| stream" in md.lower() or "| streams" in md.lower())
        self.assertIn("## Findings", md)
        self.assertIn("## Caveats", md)
        self.assertIn("ERROR", md)  # the choked 100k cell is flagged, not a real number

    def test_server_config_variants_are_side_by_side(self):
        # Two wal variants (default vs tailcache) become two columns next to each
        # other, each reading its own results/<label>/cells.json.
        root = tempfile.mkdtemp()
        for label, thr in [("wal", 100000), ("wal-tailcache", 130000)]:
            d = os.path.join(root, label); os.makedirs(d)
            json.dump({"cells": {"100": {"stream_count": 100, "throughput": thr,
                "p50": 1.0, "p99": 1.0, "pinned_pods": 4, "saturated": True, "status": "ok",
                "reason": "plateau", "walk": [[4, thr]], "image_digest": "x"}}},
                open(os.path.join(d, "cells.json"), "w"))
        suite = os.path.join(tempfile.mkdtemp(), "s.json")
        json.dump({"suite": "cache-ab", "modes": ["wal"], "stream_counts": [100],
                   "cluster": {}, "saturation": {}, "pod_ladder": {"100": [4]},
                   "server_configs": {"wal": [{"label": "wal", "args": ""},
                       {"label": "wal-tailcache", "args": "--tail-cache-bytes 65536"}]}},
                  open(suite, "w"))
        rows, md = report.build(suite, root)
        labels = {r["mode"] for r in rows}
        self.assertEqual(labels, {"wal", "wal-tailcache"})
        self.assertIn("| wal | wal-tailcache |", md)  # adjacent columns


class TestHeadlineThroughput(unittest.TestCase):
    """A NOT-saturated cell (ladder_exhausted, e.g. plateau_pct=-100 full sweeps)
    must quote the walk MAX — the highest rate the server demonstrably reached, an
    honest † lower bound — not its last/top rung, which is the over-saturation
    asymptote (closed-loop queueing, "not the number we report")."""

    def test_unsaturated_cell_reports_walk_max_not_top_rung(self):
        root = tempfile.mkdtemp()
        d = os.path.join(root, "wal"); os.makedirs(d)
        # Throughput climbs then DIPS at the over-saturated top rung. Stored
        # `throughput` is the top rung (480k); the walk max is 500k @ 32 pods.
        json.dump({"cells": {"100000": {"stream_count": 100000, "throughput": 480000,
            "p50": 40.0, "p99": 700.0, "pinned_pods": 64, "saturated": False, "status": "ok",
            "reason": "ladder_exhausted",
            "walk": [[16, 300000, 2.0, 5.0], [32, 500000, 5.0, 40.0], [64, 480000, 40.0, 700.0]],
            "image_digest": "x"}}}, open(os.path.join(d, "cells.json"), "w"))
        suite = os.path.join(tempfile.mkdtemp(), "s.json")
        json.dump({"suite": "full-ladder", "modes": ["wal"], "stream_counts": [100000],
                   "cluster": {}, "saturation": {}, "pod_ladder": {"100000": [16, 32, 64]}},
                  open(suite, "w"))
        rows, _ = report.build(suite, root)
        r = rows[0]
        self.assertEqual(r["throughput"], 500000, "unsaturated headline = walk max, not top rung 480k")
        self.assertIs(r["saturated"], False)

    def test_saturated_cell_keeps_confirmed_throughput(self):
        root = tempfile.mkdtemp()
        d = os.path.join(root, "wal"); os.makedirs(d)
        # Saturated: keep the confirmed plateau throughput (47000), NOT the walk max.
        json.dump({"cells": {"100000": {"stream_count": 100000, "throughput": 47000,
            "p50": 4.5, "p99": 60.0, "pinned_pods": 8, "saturated": True, "status": "ok",
            "reason": "plateau",
            "walk": [[4, 45000, 2.0, 5.0], [8, 47000, 4.5, 60.0], [16, 90000, 66.0, 900.0]],
            "image_digest": "x"}}}, open(os.path.join(d, "cells.json"), "w"))
        suite = os.path.join(tempfile.mkdtemp(), "s.json")
        json.dump({"suite": "sat", "modes": ["wal"], "stream_counts": [100000],
                   "cluster": {}, "saturation": {}, "pod_ladder": {"100000": [4, 8, 16]}},
                  open(suite, "w"))
        rows, _ = report.build(suite, root)
        self.assertEqual(rows[0]["throughput"], 47000, "saturated keeps confirmed plateau, not walk max")


class TestSuiteStatus(unittest.TestCase):
    def _suite(self, server_configs=None):
        d = {"suite": "st", "modes": ["wal"], "stream_counts": [1, 10],
             "cluster": {}, "saturation": {}, "pod_ladder": {"1": [1], "10": [1, 2]}}
        if server_configs:
            d["server_configs"] = server_configs
        p = os.path.join(tempfile.mkdtemp(), "s.json")
        json.dump(d, open(p, "w"))
        return p

    def _cell(self, sc, status="ok"):
        return {"stream_count": sc, "throughput": 1.0, "p50": 1.0, "p99": 1.0, "pinned_pods": 1,
                "saturated": True, "status": status, "reason": "plateau",
                "walk": [[1, 1.0]], "image_digest": "x"}

    def _write(self, root, label, cells):
        d = os.path.join(root, label); os.makedirs(d, exist_ok=True)
        json.dump({"cells": {str(c["stream_count"]): c for c in cells}},
                  open(os.path.join(d, "cells.json"), "w"))

    def test_complete_when_all_present_and_ok(self):
        suite = self._suite(); root = tempfile.mkdtemp()
        self._write(root, "wal", [self._cell(1), self._cell(10)])
        self.assertEqual(report.suite_status(suite, root), "complete")

    def test_incomplete_when_a_cell_missing(self):
        suite = self._suite(); root = tempfile.mkdtemp()
        self._write(root, "wal", [self._cell(1)])  # missing sc=10
        self.assertEqual(report.suite_status(suite, root), "incomplete")

    def test_errors_when_any_error_cell(self):
        suite = self._suite(); root = tempfile.mkdtemp()
        self._write(root, "wal", [self._cell(1), self._cell(10, status="error")])
        self.assertEqual(report.suite_status(suite, root), "errors")

    def test_complete_requires_every_label(self):
        # two server-config variants -> both labels must be fully present
        suite = self._suite({"wal": [{"label": "wal", "args": ""},
                                      {"label": "wal-tc", "args": "x"}]})
        root = tempfile.mkdtemp()
        self._write(root, "wal", [self._cell(1), self._cell(10)])  # wal-tc absent
        self.assertEqual(report.suite_status(suite, root), "incomplete")

    def _catchup_suite(self):
        # catch-up cells are keyed by pre_events, not stream_count, and live under
        # the mode label (e.g. node). suite_status must use the catchup reader/key.
        d = {"suite": "cu", "workload": "catchup", "modes": ["node"],
             "stream_counts": [200, 2000], "cluster": {},
             "catchup": {"clients": 1000}}
        p = os.path.join(tempfile.mkdtemp(), "s.json")
        json.dump(d, open(p, "w"))
        return p

    def _write_catchup(self, root, label, pres, bad=()):
        d = os.path.join(root, label); os.makedirs(d, exist_ok=True)
        cells = {str(pe): {"pre_events": pe, "p50": 1.0, "p99": 1.0,
                           "status": ("error" if pe in bad else "ok"),
                           "reason": "complete", "image_digest": "x"} for pe in pres}
        json.dump({"cells": cells}, open(os.path.join(d, "cells.json"), "w"))

    def test_catchup_complete(self):
        suite = self._catchup_suite(); root = tempfile.mkdtemp()
        self._write_catchup(root, "node", [200, 2000])
        self.assertEqual(report.suite_status(suite, root), "complete")

    def test_catchup_incomplete_when_pre_events_missing(self):
        suite = self._catchup_suite(); root = tempfile.mkdtemp()
        self._write_catchup(root, "node", [200])  # 2000 absent
        self.assertEqual(report.suite_status(suite, root), "incomplete")

    def test_catchup_errors(self):
        suite = self._catchup_suite(); root = tempfile.mkdtemp()
        self._write_catchup(root, "node", [200, 2000], bad=(2000,))
        self.assertEqual(report.suite_status(suite, root), "errors")


if __name__ == "__main__":
    unittest.main()
