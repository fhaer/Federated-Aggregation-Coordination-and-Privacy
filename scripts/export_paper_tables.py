from __future__ import annotations

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"


def build_privacy_utility_table() -> pd.DataFrame:
    summary = pd.read_csv(RESULTS / "summary.csv")
    labels = {
        "local_suppression": "Suppression",
        "coordinated_rollup": "Coordinated roll-up",
    }
    keep = summary[summary["strategy"].isin(labels)].copy()
    keep["Method"] = keep["strategy"].map(labels)
    out = keep[["Method", "k", "mean_coverage", "mean_relative_sum_error", "mean_info_loss"]].copy()
    out.columns = ["Method", "k", "Coverage", "SUM error", "Roll-up loss"]
    out[["Coverage", "SUM error", "Roll-up loss"]] = out[
        ["Coverage", "SUM error", "Roll-up loss"]
    ].round(3)
    order = {"Suppression": 0, "Coordinated roll-up": 1}
    out["_order"] = out["Method"].map(order)
    out = out.sort_values(["_order", "k"]).drop(columns="_order").reset_index(drop=True)
    return out


def build_capability_change_table() -> pd.DataFrame:
    raw = pd.read_csv(RESULTS / "orchestration_results.csv")
    drift = raw[raw["query_id"] == "DRIFT"].copy()
    labels = {
        "stored_snapshot": "Stored snapshot",
        "current_site_agent_state": "Current site-agent state",
    }
    drift = drift[drift["mode"].isin(labels)]
    out = (
        drift.groupby("mode", as_index=False)
        .agg(
            Candidates=("tested_candidates", "mean"),
            Replies=("probe_messages", "mean"),
            Unsupported=("unsupported_replies", "mean"),
        )
    )
    out["Capability information"] = out["mode"].map(labels)
    out = out[["Capability information", "Candidates", "Replies", "Unsupported"]]
    for col in ["Candidates", "Replies", "Unsupported"]:
        out[col] = out[col].round().astype(int)
    order = {"Stored snapshot": 0, "Current site-agent state": 1}
    out["_order"] = out["Capability information"].map(order)
    return out.sort_values("_order").drop(columns="_order").reset_index(drop=True)


def main() -> None:
    privacy = build_privacy_utility_table()
    capability = build_capability_change_table()
    privacy.to_csv(RESULTS / "paper_privacy_utility.csv", index=False, float_format="%.3f")
    capability.to_csv(RESULTS / "paper_capability_change.csv", index=False)
    print("Wrote paper-facing result tables:")
    print(f"- {RESULTS / 'paper_privacy_utility.csv'}")
    print(f"- {RESULTS / 'paper_capability_change.csv'}")


if __name__ == "__main__":
    main()
