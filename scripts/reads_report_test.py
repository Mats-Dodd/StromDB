import os, sys, json, tempfile, unittest
sys.path.insert(0, os.path.dirname(__file__))
import reads_cells
import reads_report


class ReadsReportTest(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.d, "wal"))
        self.suite = os.path.join(self.d, "reads-x.json")
        with open(self.suite, "w") as f:
            json.dump({"suite": "reads-x", "workload": "reads", "cluster": {},
                       "modes": ["wal"], "stream_counts": [10],
                       "reads": {"connection_levels": [8, 32]}}, f)
        cells = os.path.join(self.d, "wal", "cells.json")
        reads_cells.record(cells, 10, 8, image_digest="d", ops_per_sec=100,
            bytes_per_sec=4_000_000, p50=1.0, p99=5.0, backpressure=0,
            other_err=0, status="ok", reason="complete")
        reads_cells.record(cells, 10, 32, image_digest="d", ops_per_sec=300,
            bytes_per_sec=12_000_000, p50=2.0, p99=9.0, backpressure=7,
            other_err=0, status="ok", reason="complete")
        reads_cells.mark_complete(cells, 10, "d")

    def test_peak_picks_highest_bytes_per_sec(self):
        cell = reads_cells.all_cells(os.path.join(self.d, "wal", "cells.json"))[0]
        conn, bps = reads_report.peak_throughput(cell)
        self.assertEqual(conn, 32)
        self.assertEqual(bps, 12_000_000)

    def test_build_renders_grid_with_both_levels(self):
        rows, md = reads_report.build(self.suite, self.d)
        self.assertIn("reads-x", md)
        self.assertIn("| streams | 8 | 32 |", md.replace("  ", " "))
        # MiB/s rendered (12_000_000 B/s ~= 11 MiB/s) and backpressure flagged.
        self.assertIn("11", md)
        self.assertIn("‡", md)  # overload marker for the conn=32 cell (bp>0)


if __name__ == "__main__":
    unittest.main()
