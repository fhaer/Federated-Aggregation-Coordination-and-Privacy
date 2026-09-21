from __future__ import annotations

from pathlib import Path
import math
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"

raw = pd.read_csv(RESULTS / "experiment_results.csv")
summary = pd.read_csv(RESULTS / "summary.csv")
orch = pd.read_csv(RESULTS / "orchestration_results.csv")
paper_privacy = pd.read_csv(RESULTS / "paper_privacy_utility.csv")
paper_capability = pd.read_csv(RESULTS / "paper_capability_change.csv")

assert len(raw) == 144, f"Expected 144 privacy/utility rows, found {len(raw)}"
assert len(orch) == 42, f"Expected 42 orchestration rows, found {len(orch)}"

expected = {
    ("local_suppression", 3): (0.985, 0.015, 0.000),
    ("local_suppression", 5): (0.962, 0.038, 0.000),
    ("local_suppression", 10): (0.900, 0.099, 0.000),
    ("local_suppression", 20): (0.783, 0.216, 0.000),
    ("coordinated_rollup", 3): (1.000, 0.000, 0.346),
    ("coordinated_rollup", 5): (1.000, 0.000, 0.384),
    ("coordinated_rollup", 10): (1.000, 0.000, 0.395),
    ("coordinated_rollup", 20): (1.000, 0.000, 0.418),
}

for (strategy, k), (coverage, error, loss) in expected.items():
    row = summary[(summary["strategy"] == strategy) & (summary["k"] == k)]
    assert len(row) == 1, f"Missing summary row for {(strategy, k)}"
    r = row.iloc[0]
    checks = {
        "mean_coverage": (float(r["mean_coverage"]), coverage),
        "mean_relative_sum_error": (float(r["mean_relative_sum_error"]), error),
        "mean_info_loss": (float(r["mean_info_loss"]), loss),
    }
    for name, (actual, wanted) in checks.items():
        if not math.isclose(actual, wanted, rel_tol=0, abs_tol=5e-4):
            raise AssertionError(f"{strategy}, k={k}, {name}: {actual} != {wanted}")

# Paper capability-change example: mean over the three committed drift seeds.
drift = orch[orch["query_id"] == "DRIFT"]
for mode in ("stored_snapshot", "current_site_agent_state"):
    assert (drift["mode"] == mode).sum() == 3, f"Expected 3 drift rows for {mode}"

stale = drift[drift["mode"] == "stored_snapshot"]
current = drift[drift["mode"] == "current_site_agent_state"]

assert math.isclose(stale["tested_candidates"].mean(), 5.0, abs_tol=1e-12)
assert math.isclose(stale["probe_messages"].mean(), 20.0, abs_tol=1e-12)
assert math.isclose(stale["unsupported_replies"].mean(), 3.0, abs_tol=1e-12)
assert math.isclose(current["tested_candidates"].mean(), 2.0, abs_tol=1e-12)
assert math.isclose(current["probe_messages"].mean(), 8.0, abs_tol=1e-12)
assert math.isclose(current["unsupported_replies"].mean(), 0.0, abs_tol=1e-12)

# Check the compact paper-facing CSVs exactly at the manuscript's displayed precision.
expected_privacy_rows = [
    ("Suppression", 3, 0.985, 0.015, 0.000),
    ("Suppression", 5, 0.962, 0.038, 0.000),
    ("Suppression", 10, 0.900, 0.099, 0.000),
    ("Suppression", 20, 0.783, 0.216, 0.000),
    ("Coordinated roll-up", 3, 1.000, 0.000, 0.346),
    ("Coordinated roll-up", 5, 1.000, 0.000, 0.384),
    ("Coordinated roll-up", 10, 1.000, 0.000, 0.395),
    ("Coordinated roll-up", 20, 1.000, 0.000, 0.418),
]
actual_privacy_rows = [tuple(row) for row in paper_privacy.itertuples(index=False, name=None)]
assert actual_privacy_rows == expected_privacy_rows, (
    f"paper_privacy_utility.csv differs from expected manuscript values: {actual_privacy_rows}"
)

expected_capability_rows = [
    ("Stored snapshot", 5, 20, 3),
    ("Current site-agent state", 2, 8, 0),
]
actual_capability_rows = [tuple(row) for row in paper_capability.itertuples(index=False, name=None)]
assert actual_capability_rows == expected_capability_rows, (
    f"paper_capability_change.csv differs from expected manuscript values: {actual_capability_rows}"
)

print("Committed result artifacts match the numerical values reported in the paper.")
print(f"Privacy/utility rows: {len(raw)}")
print(f"Orchestration rows: {len(orch)}")
print("Paper-facing tables: verified")
