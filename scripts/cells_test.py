import os, sys, tempfile, unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cells


def _tmp():
    return os.path.join(tempfile.mkdtemp(), "cells.json")


class TestCells(unittest.TestCase):
    def test_record_then_saturated(self):
        p = _tmp()
        cells.record(p, 1000, image_digest="abc", walk=[[16, 500000], [20, 510000]],
                     pinned_pods=16, throughput=500000, p50=2.1, p99=2.1,
                     saturated=True, status="ok", reason="plateau")
        self.assertEqual(cells.status_of(p, 1000, "abc"), "saturated")

    def test_absent_when_unseen(self):
        p = _tmp()
        cells.record(p, 1000, image_digest="abc", walk=[], pinned_pods=16,
                     throughput=1, p50=1, p99=1, saturated=True, status="ok", reason="plateau")
        self.assertEqual(cells.status_of(p, 10000, "abc"), "absent")

    def test_digest_change_invalidates(self):
        p = _tmp()
        cells.record(p, 1000, image_digest="abc", walk=[], pinned_pods=16,
                     throughput=1, p50=1, p99=1, saturated=True, status="ok", reason="plateau")
        self.assertEqual(cells.status_of(p, 1000, "xyz"), "absent")

    def test_ladder_exhausted_resumes(self):
        p = _tmp()
        cells.record(p, 100000, image_digest="abc", walk=[[400, 300000]], pinned_pods=400,
                     throughput=300000, p50=9, p99=9, saturated=False, status="ok",
                     reason="ladder_exhausted")
        self.assertEqual(cells.status_of(p, 100000, "abc"), "resume")

    def test_error_status(self):
        p = _tmp()
        cells.record(p, 100000, image_digest="abc", walk=[[200, 0]], pinned_pods=None,
                     throughput=0, p50=None, p99=None, saturated=False, status="error",
                     reason="creation_choke")
        self.assertEqual(cells.status_of(p, 100000, "abc"), "error")

    def test_upsert_overwrites(self):
        p = _tmp()
        cells.record(p, 1000, image_digest="abc", walk=[], pinned_pods=16, throughput=1,
                     p50=1, p99=1, saturated=False, status="ok", reason="ladder_exhausted")
        cells.record(p, 1000, image_digest="abc", walk=[], pinned_pods=24, throughput=2,
                     p50=1, p99=1, saturated=True, status="ok", reason="plateau")
        self.assertEqual(cells.status_of(p, 1000, "abc"), "saturated")
        self.assertEqual(len(cells.all_cells(p)), 1)


if __name__ == "__main__":
    unittest.main()
