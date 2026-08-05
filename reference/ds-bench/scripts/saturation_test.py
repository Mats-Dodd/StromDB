import json, os, subprocess, sys, tempfile, unittest
import importlib.util
HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("saturation", os.path.join(HERE, "saturation.py"))
sat = importlib.util.module_from_spec(spec); spec.loader.exec_module(sat)

class TestClassify(unittest.TestCase):
    def test_cpu_bound(self):          # 370% >= 0.9*4*100=360
        self.assertEqual(sat.classify(0, 100000, 370.0, 4), "cpu")
    def test_headroom_real_n10_to_n100(self):   # +24% gain, cpu under threshold
        self.assertEqual(sat.classify(860827, 1069919, 222.0, 4), "headroom")
    def test_plateau_small_gain(self):          # +5% gain, cpu low
        self.assertEqual(sat.classify(1000000, 1050000, 200.0, 4), "plateau")
    def test_no_prev_cannot_plateau(self):
        self.assertEqual(sat.classify(0, 50, 10.0, 4), "headroom")

class TestThroughput(unittest.TestCase):
    def test_reads_aggregate_ops(self):
        p = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        p.write('coordinator log line\n{"aggregate_ops_per_sec": 1069919.5, "p99_ms": 12.7}\n'); p.close()
        self.assertAlmostEqual(sat.extract_throughput(p.name), 1069919.5)
        os.unlink(p.name)
    def test_reads_aggregate_events(self):
        p = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        p.write('{"aggregate_events_per_sec": 120000.0}\n'); p.close()
        self.assertAlmostEqual(sat.extract_throughput(p.name), 120000.0)
        os.unlink(p.name)
    def test_last_object_wins_multiple_objects(self):
        # Regression: ensure we return the LAST JSON object's value, not the first or a merged swallow
        p = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        p.write('coordinator log\n{"aggregate_ops_per_sec": 100.0}\n{"aggregate_ops_per_sec": 999.0}\n')
        p.close()
        self.assertAlmostEqual(sat.extract_throughput(p.name), 999.0)
        os.unlink(p.name)
    def test_pretty_printed_multiline_with_log_prefix(self):
        # Regression: the REAL coordinator merged.json is a `mc cp` log prefix
        # followed by a PRETTY-PRINTED (multi-line) JSON object. A per-line scan
        # misses it (no single line is a complete object) → must still parse it.
        real = (
            "Added `local` successfully.\n"
            "coordinator: 8 client result file(s) under run-x\n"
            "`local/bench-results/run-x/ms-0.json` -> `/merge/ms-0.json`\n"
            "┌───┐\n│ Total │\n└───┘\n"
            "{\n"
            '  "merged_count": 1240094,\n'
            '  "p50_ms": 0.214,\n'
            '  "p99_ms": 3.813,\n'
            '  "aggregate_ops_per_sec": 82654.70762487104\n'
            "}\n"
        )
        p = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        p.write(real); p.close()
        self.assertAlmostEqual(sat.extract_throughput(p.name), 82654.70762487104)
        os.unlink(p.name)

class TestCLI(unittest.TestCase):
    # The calibrate loop in lib-bench.sh shells out to this CLI and takes the
    # first field as the reason. Lock that "<reason> <thr>" contract.
    def _run(self, merged_text, prev_thr, cpu, cores):
        p = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        p.write(merged_text); p.close()
        try:
            r = subprocess.run(
                [sys.executable, os.path.join(HERE, "saturation.py"),
                 "--merged", p.name, "--prev-thr", str(prev_thr),
                 "--cpu", str(cpu), "--cores", str(cores)],
                capture_output=True, text=True)
            return r
        finally:
            os.unlink(p.name)

    def test_cli_prints_reason_and_throughput(self):
        r = self._run('{"aggregate_ops_per_sec": 1050000.0}\n', 1000000, 50, 4)
        self.assertEqual(r.returncode, 0, r.stderr)
        reason, thr, aligned, p50, p99 = r.stdout.split()
        self.assertEqual(reason, "plateau")        # +5% gain <10%
        self.assertAlmostEqual(float(thr), 1050000.0)
        self.assertEqual(aligned, "1")             # no stamps → aligned by default
        self.assertEqual((p50, p99), ("None", "None"))  # no latency in merged → None

    def test_cli_cpu_bound(self):
        r = self._run('{"aggregate_ops_per_sec": 5000.0}\n', 0, 370, 4)
        self.assertEqual(r.stdout.split()[0], "cpu")   # 370 >= 0.9*4*100


class TestWindowAlignment(unittest.TestCase):
    """Fleet throughput is a SUM of per-pod rates; when the pods' measure windows
    did not overlap (windows_aligned=false from hdr-merge) the sum multiply-counts
    server capacity and the number is garbage — the walker must see thr=0 so the
    rung is recorded as an error instead of an inflated value."""

    def _write(self, obj):
        p = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        p.write(json.dumps(obj)); p.close()
        return p.name

    def test_misaligned_windows_zero_throughput(self):
        p = self._write({"aggregate_ops_per_sec": 2978371.0, "windows_aligned": False,
                         "measure_span_secs": 204.0, "measure_window_secs": 8.0})
        try:
            self.assertEqual(sat.extract_throughput(p), 0.0)
        finally:
            os.unlink(p)

    def test_aligned_windows_pass_through(self):
        p = self._write({"aggregate_ops_per_sec": 578000.0, "windows_aligned": True,
                         "measure_span_secs": 9.0, "measure_window_secs": 8.0})
        try:
            self.assertAlmostEqual(sat.extract_throughput(p), 578000.0)
        finally:
            os.unlink(p)

    def test_no_alignment_field_back_compat(self):
        # Old merged.json (no stamps) must keep working unchanged.
        p = self._write({"aggregate_ops_per_sec": 578000.0})
        try:
            self.assertAlmostEqual(sat.extract_throughput(p), 578000.0)
        finally:
            os.unlink(p)

    def test_cli_third_field_signals_alignment(self):
        # Walker protocol: "<reason> <thr> <aligned> <p50> <p99>"; aligned=0 lets
        # walk_cell record reason=misaligned_windows instead of creation_choke;
        # p50/p99 let the walk carry per-rung latency (knee vs saturation).
        cli = TestCLI()
        r = cli._run(json.dumps({"aggregate_ops_per_sec": 100.0, "windows_aligned": False}), 0, 50, 4)
        parts = r.stdout.split()
        self.assertEqual(len(parts), 5, r.stdout)
        self.assertEqual(parts[2], "0")
        r = cli._run(json.dumps({"aggregate_ops_per_sec": 100.0, "windows_aligned": True}), 0, 50, 4)
        self.assertEqual(r.stdout.split()[2], "1")


class TestPlateauPin(unittest.TestCase):
    """Noise-robust plateau detection. patience=1 reproduces the legacy single-shot
    rule (plateau on the first sub-threshold gain, pin the rung before). patience>=2
    requires that many CONSECUTIVE sub-threshold gains, so one unlucky-low rung no
    longer triggers a false plateau (the reported ceiling stops early)."""

    def w(self, pairs):
        return [[p, t] for p, t in pairs]

    def test_patience1_matches_legacy_pin(self):
        # 12:400k 16:500k(+25%) 20:560k(+12%) 24:575k(+2.7%<=10%) -> plateau, pin 20.
        walk = self.w([(12, 400000), (16, 500000), (20, 560000), (24, 575000)])
        self.assertEqual(sat.plateau_pin(walk, 10, patience=1), [20, 560000])

    def test_patience1_still_climbing_returns_none(self):
        walk = self.w([(12, 400000), (16, 500000), (20, 560000)])  # last gain +12% > 10%
        self.assertIsNone(sat.plateau_pin(walk, 10, patience=1))

    def test_patience2_ignores_single_noisy_dip(self):
        # 1:100k 2:200k(+100%) 4:205k(+2.5%<=10%) 8:320k(+56%) — a single low rung.
        # patience=1 would FALSELY plateau at the +2.5% rung; patience=2 must not,
        # because the next gain recovered (+56%).
        walk = self.w([(1, 100000), (2, 200000), (4, 205000), (8, 320000)])
        self.assertIsNone(sat.plateau_pin(walk, 10, patience=2))

    def test_patience2_plateaus_on_two_consecutive(self):
        # ...300k 315k(+5%) 320k(+1.6%): two consecutive <=10% -> plateau, pin the
        # rung where climbing stopped (the one before the 2-gain window).
        walk = self.w([(4, 300000), (8, 315000), (16, 320000)])
        self.assertEqual(sat.plateau_pin(walk, 10, patience=2), [4, 300000])

    def test_too_few_rungs_returns_none(self):
        self.assertIsNone(sat.plateau_pin(self.w([(1, 100000)]), 10, patience=1))
        self.assertIsNone(sat.plateau_pin(self.w([(1, 100000), (2, 200000)]), 10, patience=2))

    def test_negative_plateau_pct_never_pins(self):
        # The -100 sentinel forces the full ladder (gain > -1 always true).
        walk = self.w([(1, 100000), (2, 100001), (4, 100002)])
        self.assertIsNone(sat.plateau_pin(walk, -100, patience=1))


class TestFleetCompleteness(unittest.TestCase):
    """A short fleet (some pods preempted/OOMed and never uploaded) makes the
    SUMMED throughput under-count the server. When the caller passes the expected
    pod count, extract_throughput must reject a merge whose pods_reported is
    below it — same thr=0 → error routing as the windows_aligned guard."""

    def _write(self, obj):
        p = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        p.write(json.dumps(obj)); p.close()
        return p.name

    def test_short_fleet_zero_throughput(self):
        p = self._write({"aggregate_ops_per_sec": 500000.0, "pods_reported": 6})
        try:
            self.assertEqual(sat.extract_throughput(p, expect_pods=8), 0.0)
        finally:
            os.unlink(p)

    def test_complete_fleet_passes_through(self):
        p = self._write({"aggregate_ops_per_sec": 500000.0, "pods_reported": 8})
        try:
            self.assertAlmostEqual(sat.extract_throughput(p, expect_pods=8), 500000.0)
        finally:
            os.unlink(p)

    def test_no_pods_reported_field_back_compat(self):
        # Old merged.json (no pods_reported) must keep working even with expect set.
        p = self._write({"aggregate_ops_per_sec": 500000.0})
        try:
            self.assertAlmostEqual(sat.extract_throughput(p, expect_pods=8), 500000.0)
        finally:
            os.unlink(p)

    def test_no_expect_pods_ignores_count(self):
        # Without an expected count, pods_reported is informational only.
        p = self._write({"aggregate_ops_per_sec": 500000.0, "pods_reported": 6})
        try:
            self.assertAlmostEqual(sat.extract_throughput(p), 500000.0)
        finally:
            os.unlink(p)

    def test_cli_expect_pods_flag_zeroes_short_fleet(self):
        cli = TestCLI()
        p = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        p.write(json.dumps({"aggregate_ops_per_sec": 500000.0, "pods_reported": 6})); p.close()
        try:
            r = subprocess.run(
                [sys.executable, os.path.join(HERE, "saturation.py"),
                 "--merged", p.name, "--prev-thr", "0", "--cpu", "50", "--cores", "4",
                 "--expect-pods", "8"],
                capture_output=True, text=True)
            self.assertEqual(float(r.stdout.split()[1]), 0.0, r.stdout)
        finally:
            os.unlink(p.name)


if __name__ == "__main__":
    unittest.main()
