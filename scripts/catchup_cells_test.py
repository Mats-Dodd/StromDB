import os, tempfile, unittest, sys
sys.path.insert(0, os.path.dirname(__file__))
import catchup_cells


class CatchupCellsTest(unittest.TestCase):
    def setUp(self):
        self.p = os.path.join(tempfile.mkdtemp(), "cells.json")

    def _rec(self, pe, digest="img1", status="ok", reason="complete", p99=253.0):
        catchup_cells.record(self.p, pe, image_digest=digest, clients=1000, event_bytes=1024,
                             snapshot_bytes=51200, pods=1, p50=120.0, p99=p99,
                             bytes_received_total=176128000, body_kb=172.0,
                             status=status, reason=reason)

    def test_record_fields(self):
        self._rec(200)
        c = catchup_cells.all_cells(self.p)[0]
        self.assertEqual(c["pre_events"], 200)
        self.assertEqual(c["p99"], 253.0)
        self.assertEqual(c["body_kb"], 172.0)
        self.assertEqual(c["clients"], 1000)

    def test_status_of(self):
        self.assertEqual(catchup_cells.status_of(self.p, 200, "img1"), "absent")
        self._rec(200)
        self.assertEqual(catchup_cells.status_of(self.p, 200, "img1"), "done")
        self.assertEqual(catchup_cells.status_of(self.p, 200, "img2"), "absent")
        self._rec(2000, status="error", reason="creation_choke")
        self.assertEqual(catchup_cells.status_of(self.p, 2000, "img1"), "error")

    def test_upsert(self):
        self._rec(200, p99=300.0)
        self._rec(200, p99=253.0)
        cells = catchup_cells.all_cells(self.p)
        self.assertEqual(len(cells), 1)
        self.assertEqual(cells[0]["p99"], 253.0)


if __name__ == "__main__":
    unittest.main()
