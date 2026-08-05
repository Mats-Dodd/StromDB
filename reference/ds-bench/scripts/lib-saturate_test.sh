#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."
. scripts/lib-saturate.sh

# Self-contained suite fixture (decoupled from the shipped suites/*.json) with the
# exact ladders these cases exercise.
SUITE_DIR="$(mktemp -d)"
export SUITE_FILE="$SUITE_DIR/suite.json"
cat > "$SUITE_FILE" <<'JSON'
{
  "suite": "walk-test",
  "cluster": {},
  "saturation": { "plateau_pct": 10, "fleet_cpu": 0.5, "repeats": 1, "warmup_secs": 1, "measure_secs": 1 },
  "modes": ["wal"],
  "stream_counts": [1, 1000, 100000],
  "pod_ladder": {
    "1":      [1, 2],
    "1000":   [12, 16, 20, 24, 32],
    "100000": [128, 200, 256, 320, 400, 512]
  }
}
JSON

# Inject canned throughputs keyed by pod count; reset_state is a no-op in the test.
reset_state() { :; }
declare -A CANNED=( [12]=400000 [16]=500000 [20]=560000 [24]=575000 )
measure_pods() { echo "0 ${CANNED[$1]:-0}"; }   # "cpu_pct thr"
export MEASURE_FN=measure_pods

tmp="$(mktemp -d)/cells.json"
walk_cell wal 1000 "$tmp" "digest123"

# 12->16 (+25%, continue), 16->20 (+12%, continue), 20->24 (+2.7% < 10% -> plateau, pin 20)
python3 - "$tmp" <<'PY'
import sys, json
cell = json.load(open(sys.argv[1]))["cells"]["1000"]
assert cell["saturated"] is True, cell
assert cell["reason"] == "plateau", cell
assert cell["pinned_pods"] == 20, cell
print("PASS walk_cell plateau")
PY

declare -A CANNED2=( [128]=300000 [200]=360000 [256]=420000 [320]=500000 [400]=600000 [512]=720000 )
measure_pods() { echo "0 ${CANNED2[$1]:-0}"; }
tmp2="$(mktemp -d)/cells.json"
walk_cell wal 100000 "$tmp2" "d2"
python3 - "$tmp2" <<'PY'
import sys, json
cell = json.load(open(sys.argv[1]))["cells"]["100000"]
assert cell["saturated"] is False and cell["reason"] == "ladder_exhausted", cell
print("PASS walk_cell ladder_exhausted")
PY

# Capping: n=1 ladder [1,2] caps to [1] — a single rung, no overshoot, no false plateau.
declare -A CANNED3=( [1]=900 [2]=1800 )
measure_pods() { echo "0 ${CANNED3[$1]:-0}"; }
tmp3="$(mktemp -d)/cells.json"
walk_cell wal 1 "$tmp3" "d3"
python3 - "$tmp3" <<'PY'
import sys, json
cell = json.load(open(sys.argv[1]))["cells"]["1"]
assert len(cell["walk"]) == 1, cell          # only pods=1 measured (2 was capped away)
assert cell["walk"][0][0] == 1, cell
assert cell["reason"] == "ladder_exhausted", cell
print("PASS walk_cell caps low cardinality")
PY

# _record must NEVER drop a cell on a malformed p50/throughput (regression: a bad
# confirm-p50 used to crash float() and silently lose the whole cell).
tmp4="$(mktemp -d)/cells.json"
_record "$tmp4" 1000 d4 '[[16,500000]]' 16 500000 "garbage-p50" True ok plateau 3.3
python3 - "$tmp4" <<'PY'
import sys, json
cell = json.load(open(sys.argv[1]))["cells"]["1000"]
assert cell["throughput"] == 500000.0 and cell["p50"] is None, cell   # bad p50 -> None
assert cell["p99"] == 3.3, cell                                       # valid p99 stored
assert cell["saturated"] is True and cell["reason"] == "plateau", cell
print("PASS _record tolerates malformed p50, keeps p99")
PY

# ── patience=2 hysteresis: a single noisy DIP must not trigger a false plateau ──
SUITE_DIR2="$(mktemp -d)"; export SUITE_FILE="$SUITE_DIR2/suite.json"
cat > "$SUITE_FILE" <<'JSON'
{
  "suite": "patience-test",
  "cluster": {},
  "saturation": { "plateau_pct": 10, "patience": 2, "fleet_cpu": 0.5, "repeats": 1, "warmup_secs": 1, "measure_secs": 1 },
  "modes": ["wal"],
  "stream_counts": [1000],
  "pod_ladder": { "1000": [1, 2, 4, 8, 16, 32] }
}
JSON
# 1:100k 2:200k(+100%) 4:205k(+2.5% NOISE DIP) 8:320k(+56%) 16:325k(+1.6%) 32:328k(+0.9%)
# patience=1 would FALSELY plateau at rung 4 (pin 2). patience=2 rides through the
# dip and pins the REAL knee at pods=8 once two consecutive small gains appear (16,32).
declare -A CANNEDP=( [1]=100000 [2]=200000 [4]=205000 [8]=320000 [16]=325000 [32]=328000 )
measure_pods() { echo "0 ${CANNEDP[$1]:-0}"; }
tmpp="$(mktemp -d)/cells.json"
walk_cell wal 1000 "$tmpp" "dp"
python3 - "$tmpp" <<'PY'
import sys, json
cell = json.load(open(sys.argv[1]))["cells"]["1000"]
assert cell["reason"] == "plateau", cell
assert cell["pinned_pods"] == 8, cell   # NOT 2 (the noisy dip) — patience rode through it
print("PASS walk_cell patience=2 ignores single noisy dip")
PY

# ── _confirm averages throughput over reps (replicated headline, not single shot) ──
SAT_RESULT_ROOT="$(mktemp -d)"; export SAT_RESULT_ROOT
SAT_MODE=wal; SAT_SC=1000
# Mock fn: per-rep merged.json where BOTH latency and throughput differ by rep, so
# the MEAN throughput (410000) and the EVEN-count MEDIAN latency (avg of the two
# middles) are both unambiguous. rep1: p50=4 p99=8 thr=400000; rep2: p50=6 p99=12
# thr=420000. Median of {4,6}=5, {8,12}=10 (NOT the lower-middle 4/8).
measure_pods() {
  local cd; cd="$(_sat_cell_dir "$1" "${SAT_REP}")"; mkdir -p "$cd"
  local p50=4 p99=8 thr=400000
  [ "${SAT_REP}" = "2" ] && { p50=6; p99=12; thr=420000; }
  printf '{"p50_ms": %s, "p99_ms": %s, "aggregate_ops_per_sec": %s}\n' "$p50" "$p99" "$thr" > "$cd/merged.json"
}
conf="$(_confirm measure_pods wal 16 2)"
python3 - "$conf" <<'PY'
import sys
p50, p99, thr = sys.argv[1].split()
assert abs(float(thr) - 410000.0) < 1e-6, f"expected mean thr 410000, got {thr}"
assert float(p50) == 5.0, f"even-count median p50 must average the two middles (5.0), got {p50}"
assert float(p99) == 10.0, f"even-count median p99 must be 10.0, got {p99}"
print("PASS _confirm averages throughput + even-count median")
PY
