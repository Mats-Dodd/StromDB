import os, tempfile, unittest, sys
sys.path.insert(0, os.path.dirname(__file__))
import sustained_cells


class SustainedCellsTest(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.p = os.path.join(self.d, "cells.json")

    def _rec(self, sc, digest="img1", status="ok", reason="complete", drift=2.0):
        sustained_cells.record(
            self.p, sc, image_digest=digest, pods=1, rate_per_stream=10,
            duration_secs=90, throughput=1500.0, p50=1.2, p99=8.0, p999=20.0,
            rss_peak_mb=120.0, rss_drift_mb=drift, cpu_mean=12.0, stable=True,
            status=status, reason=reason)

    def test_record_and_fields(self):
        self._rec(10)
        c = sustained_cells.all_cells(self.p)[0]
        self.assertEqual(c["stream_count"], 10)
        self.assertEqual(c["throughput"], 1500.0)
        self.assertEqual(c["rss_drift_mb"], 2.0)
        self.assertTrue(c["stable"])

    def test_status_of(self):
        self.assertEqual(sustained_cells.status_of(self.p, 10, "img1"), "absent")
        self._rec(10)
        self.assertEqual(sustained_cells.status_of(self.p, 10, "img1"), "done")
        self.assertEqual(sustained_cells.status_of(self.p, 10, "img2"), "absent")  # digest changed
        self._rec(50, status="error", reason="creation_choke")
        self.assertEqual(sustained_cells.status_of(self.p, 50, "img1"), "error")

    def test_upsert(self):
        self._rec(10, drift=2.0)
        self._rec(10, drift=9.0)
        cells = sustained_cells.all_cells(self.p)
        self.assertEqual(len(cells), 1)
        self.assertEqual(cells[0]["rss_drift_mb"], 9.0)


if __name__ == "__main__":
    unittest.main()
