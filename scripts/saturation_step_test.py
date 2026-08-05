import os, sys, unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from saturation import step_decision, cap_ladder


class TestCapLadder(unittest.TestCase):
    def test_no_cap_when_pods_below_streams(self):
        self.assertEqual(cap_ladder([10, 16, 24, 32], 10000), [10, 16, 24, 32])

    def test_caps_pods_above_streams(self):
        # 8 > 5 -> capped to the stream count
        self.assertEqual(cap_ladder([2, 4, 8], 5), [2, 4, 5])

    def test_dedups_after_cap(self):
        # n=1: [1,2] both cap to 1 -> single rung (no false 0%-gain plateau)
        self.assertEqual(cap_ladder([1, 2], 1), [1])
        self.assertEqual(cap_ladder([1, 2, 4], 1), [1])

    def test_high_cardinality_unchanged(self):
        self.assertEqual(cap_ladder([80, 100, 110], 100000), [80, 100, 110])


class TestStepDecision(unittest.TestCase):
    def test_first_rung_continues(self):
        self.assertEqual(step_decision(0, 100000, 10), "continue")

    def test_big_gain_continues(self):
        # +27% gain -> keep climbing
        self.assertEqual(step_decision(300000, 380000, 10), "continue")

    def test_small_gain_plateaus(self):
        # +2.4% gain < 10% -> saturated
        self.assertEqual(step_decision(410000, 420000, 10), "plateau")

    def test_exact_threshold_plateaus(self):
        # exactly 10% is not "above" the threshold -> plateau
        self.assertEqual(step_decision(100000, 110000, 10), "plateau")

    def test_collapse_is_error(self):
        self.assertEqual(step_decision(400000, 0, 10), "error")


if __name__ == "__main__":
    unittest.main()
