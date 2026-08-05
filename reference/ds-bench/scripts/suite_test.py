import os, sys, unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from suite import Suite

DURABLE = os.path.join(HERE, "..", "suites", "run-durable.json")
S2 = os.path.join(HERE, "..", "suites", "run-s2.json")


class TestSuite(unittest.TestCase):
    def test_loads_fields(self):
        # Structural invariants only — NOT tuning values (ladders/fleet_cpu change).
        s = Suite.load(DURABLE)
        self.assertEqual(s.name, "run-durable")
        self.assertEqual(s.modes, ["wal"])
        self.assertTrue(s.stream_counts)  # non-empty
        self.assertEqual(s.cluster["server_machine"], "c4d-standard-16-lssd")
        self.assertGreater(s.saturation["plateau_pct"], 0)
        self.assertGreater(s.saturation["fleet_cpu"], 0)

    def test_ladder_for_every_stream_count(self):
        # Each declared stream-count has a non-empty ladder of positive ints.
        s = Suite.load(DURABLE)
        for sc in s.stream_counts:
            ladder = s.ladder_for(sc)
            self.assertTrue(ladder and all(isinstance(p, int) and p >= 1 for p in ladder),
                            (sc, ladder))

    def test_ladder_for_missing_raises(self):
        s = Suite.load(DURABLE)
        with self.assertRaises(KeyError):
            s.ladder_for(999)

    def test_configs_for_default_when_absent(self):
        # A mode with no server_configs entry (s2) has one implicit config =
        # the mode itself with empty server args (the baseline).
        s = Suite.load(S2)
        self.assertEqual(s.configs_for("s2"), [{"label": "s2", "args": ""}])
        self.assertEqual(s.labels(), ["s2"])

    def test_durable_server_configs_side_by_side(self):
        # The durable suite carries wal / wal-tailcache / memory as labelled variants.
        s = Suite.load(DURABLE)
        self.assertEqual(s.configs_for("wal"), [
            {"label": "wal", "args": ""},
            {"label": "wal-tailcache", "args": "--tail-cache-bytes 65536"},
            {"label": "memory", "args": "--durability memory"}])
        self.assertEqual(s.labels(), ["wal", "wal-tailcache", "memory"])

    def test_configs_for_and_labels_in_memory(self):
        s = Suite({"suite": "x", "modes": ["wal"], "stream_counts": [1],
                   "cluster": {}, "saturation": {}, "pod_ladder": {"1": [1]},
                   "server_configs": {"wal": [
                       {"label": "wal", "args": ""},
                       {"label": "wal-tailcache", "args": "--tail-cache-bytes 65536"}]}})
        self.assertEqual(s.labels(), ["wal", "wal-tailcache"])


if __name__ == "__main__":
    unittest.main()
