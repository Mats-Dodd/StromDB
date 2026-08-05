import os, sys, tempfile, unittest
sys.path.insert(0, os.path.dirname(__file__))
import reads_cells


class ReadsCellsTest(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.p = os.path.join(self.d, "cells.json")

    def _rec(self, sc, conn, digest="dig1", status="ok"):
        reads_cells.record(self.p, sc, conn, image_digest=digest,
                           ops_per_sec=100.0, bytes_per_sec=4096.0, p50=1.0, p99=5.0,
                           backpressure=0, other_err=0, status=status, reason="complete")

    def test_absent_before_any_record(self):
        self.assertEqual(reads_cells.status_of(self.p, 10, "dig1"), "absent")
        self.assertEqual(reads_cells.conn_status(self.p, 10, 8, "dig1"), "absent")

    def test_conn_status_done_after_record(self):
        self._rec(10, 8)
        self.assertEqual(reads_cells.conn_status(self.p, 10, 8, "dig1"), "done")
        self.assertEqual(reads_cells.conn_status(self.p, 10, 32, "dig1"), "absent")

    def test_status_done_only_after_mark_complete(self):
        self._rec(10, 8)
        self._rec(10, 32)
        self.assertEqual(reads_cells.status_of(self.p, 10, "dig1"), "absent")
        reads_cells.mark_complete(self.p, 10, "dig1")
        self.assertEqual(reads_cells.status_of(self.p, 10, "dig1"), "done")

    def test_digest_mismatch_is_absent(self):
        self._rec(10, 8)
        reads_cells.mark_complete(self.p, 10, "dig1")
        self.assertEqual(reads_cells.status_of(self.p, 10, "dig2"), "absent")

    def test_all_cells_returns_connections_map(self):
        self._rec(10, 8)
        self._rec(10, 32)
        cells = reads_cells.all_cells(self.p)
        self.assertEqual(len(cells), 1)
        self.assertEqual(set(cells[0]["connections"].keys()), {"8", "32"})

    def test_completed_cell_with_errored_level_reruns(self):
        # A completed cell whose sweep left an errored level must report "error"
        # (not "done") so a resume re-runs it instead of stranding the failure.
        self._rec(10, 8)  # ok
        reads_cells.record(self.p, 10, 32, image_digest="dig1",
                           ops_per_sec=0.0, bytes_per_sec=0.0, p50=None, p99=None,
                           backpressure=0, other_err=0, status="error", reason="no_reads")
        reads_cells.mark_complete(self.p, 10, "dig1")
        self.assertEqual(reads_cells.status_of(self.p, 10, "dig1"), "error")
        # the ok level stays "done"; the errored level is "absent" so it re-runs
        self.assertEqual(reads_cells.conn_status(self.p, 10, 8, "dig1"), "done")
        self.assertEqual(reads_cells.conn_status(self.p, 10, 32, "dig1"), "absent")

    def test_all_ok_completed_cell_is_done(self):
        self._rec(10, 8)
        self._rec(10, 32)
        reads_cells.mark_complete(self.p, 10, "dig1")
        self.assertEqual(reads_cells.status_of(self.p, 10, "dig1"), "done")


import json as _json
import report as _report


class ReadsSuiteStatusTest(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.d, "wal"))
        self.suite = os.path.join(self.d, "reads-x.json")
        with open(self.suite, "w") as f:
            _json.dump({"suite": "reads-x", "workload": "reads",
                        "cluster": {}, "modes": ["wal"],
                        "stream_counts": [10],
                        "reads": {"connection_levels": [8, 32]}}, f)

    def _cells(self):
        return os.path.join(self.d, "wal", "cells.json")

    def test_incomplete_when_cell_missing(self):
        self.assertEqual(_report.suite_status(self.suite, self.d), "incomplete")

    def test_complete_when_marked(self):
        reads_cells.record(self._cells(), 10, 8, image_digest="d", ops_per_sec=1,
            bytes_per_sec=1, p50=1, p99=2, backpressure=0, other_err=0,
            status="ok", reason="complete")
        reads_cells.mark_complete(self._cells(), 10, "d")
        self.assertEqual(_report.suite_status(self.suite, self.d), "complete")

    def test_errors_when_subcell_errored(self):
        reads_cells.record(self._cells(), 10, 8, image_digest="d", ops_per_sec=0,
            bytes_per_sec=0, p50=None, p99=None, backpressure=0, other_err=5,
            status="error", reason="no_reads")
        reads_cells.mark_complete(self._cells(), 10, "d")
        self.assertEqual(_report.suite_status(self.suite, self.d), "errors")


if __name__ == "__main__":
    unittest.main()
