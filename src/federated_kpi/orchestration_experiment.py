from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Iterable, List, Dict
import json

import pandas as pd

from .core import Coordinator, PrivacyPolicy, QuerySpec, levels_to_names, weighted_relative_sum_error
from .orchestration import AgentOrchestrator, LocalAnalyticsAgent, default_capability_profiles
from .synthetic import create_federation


WORKLOAD = [
    QuerySpec("A1", {"geo": 0, "age": 0}, measure="revenue"),
    QuerySpec("A2", {"geo": 0, "product": 1}, measure="revenue"),
    QuerySpec("A3", {"geo": 0, "age": 1, "product": 1}, measure="revenue"),
    QuerySpec("A4", {"geo": 1, "time": 0}, measure="revenue"),
]


def run_orchestration_experiment(
    output_dir: Path,
    seeds: Iterable[int] = (11, 22, 33),
    ks: Iterable[int] = (5, 10, 20),
) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str, object]] = []

    for seed in seeds:
        parties = create_federation(
            output_dir / f"agent_db_seed_{seed}",
            n_parties=4,
            customers_per_party=450,
            facts_per_party=5000,
            seed=seed,
        )
        profiles = default_capability_profiles(parties, heterogeneous=True)
        # Sites can enforce stricter local minima without revealing the threshold.
        local_ks = [1, 8, 12, 5]
        agents = [
            LocalAnalyticsAgent(p, cap, local_min_k=lk, max_releases_per_query=1)
            for p, cap, lk in zip(parties, profiles, local_ks)
        ]
        coord = Coordinator(parties)

        for k in ks:
            policy = PrivacyPolicy(k=k, weights={"geo": 1.2, "age": 1.0, "product": 0.8, "time": 1.0})
            for q0 in WORKLOAD:
                # Unique query IDs are required because release budgets are stateful.
                q = QuerySpec(f"{q0.query_id}-s{seed}-k{k}", dict(q0.group_levels), q0.measure, q0.year_filter)
                orch = AgentOrchestrator(agents)
                fused, meta, manifests = orch.execute(q, policy, discover_capabilities=True)
                levels = None
                # Recover committed levels from audit trail.
                for e in orch.events:
                    if e.kind == "PLAN_COMMIT":
                        levels = dict(e.payload["levels"])
                        break
                assert levels is not None
                oracle = coord.centralized_oracle(q, levels)
                err = weighted_relative_sum_error(fused, oracle, list(q.group_levels.keys()))
                rows.append(
                    {
                        "seed": seed,
                        "k": k,
                        "query_id": q0.query_id,
                        "mode": "current_capability_workload",
                        "success": 1,
                        "final_levels": levels_to_names(levels),
                        "relative_sum_error": err,
                        **meta,
                    }
                )
                if seed == 11 and k == 10 and q0.query_id == "A1":
                    orch.audit_dataframe().to_csv(output_dir / "sample_agent_audit.csv", index=False)
                    (output_dir / "sample_capability_manifests.json").write_text(
                        json.dumps(manifests, indent=2, sort_keys=True), encoding="utf-8"
                    )

        # Dynamic capability-change scenario: cached capability data becomes stale.
        q = QuerySpec(f"DRIFT-s{seed}", {"geo": 0, "age": 0}, measure="revenue")
        policy = PrivacyPolicy(k=10)
        initial = AgentOrchestrator(agents)
        cached = initial.discover(q)
        # Site P1 changes its locally exposed geography floor after discovery.
        agents[0].capability = replace(
            agents[0].capability,
            min_levels={**dict(agents[0].capability.min_levels), "geo": 2},
            capability_version="2",
        )

        # Stale-registry mode still receives runtime refusals, but wastes proposals.
        stale = AgentOrchestrator(agents)
        try:
            fused, meta, _ = stale.execute(q, policy, discover_capabilities=False, cached_manifests=cached)
            commit = next(e for e in stale.events if e.kind == "PLAN_COMMIT")
            levels = dict(commit.payload["levels"])
            oracle = coord.centralized_oracle(q, levels)
            error = weighted_relative_sum_error(fused, oracle, list(q.group_levels.keys()))
            success = 1
        except Exception:
            meta = {
                "tested_candidates": float("nan"), "probe_messages": float("nan"),
                "unsupported_replies": float("nan"), "unsafe_replies": float("nan"),
                "information_loss": float("nan"), "orchestration_events_total": float(len(stale.events)),
            }
            error = float("nan")
            success = 0
        rows.append({"seed": seed, "k": 10, "query_id": "DRIFT", "mode": "stored_snapshot", "success": success, "relative_sum_error": error, **meta})

        # Live agent discovery sees the changed capability version before planning.
        current = AgentOrchestrator(agents)
        q_live = QuerySpec(f"DRIFT-LIVE-s{seed}", {"geo": 0, "age": 0}, measure="revenue")
        fused, meta, _ = current.execute(q_live, policy, discover_capabilities=True)
        commit = next(e for e in current.events if e.kind == "PLAN_COMMIT")
        levels = dict(commit.payload["levels"])
        oracle = coord.centralized_oracle(q_live, levels)
        error = weighted_relative_sum_error(fused, oracle, list(q_live.group_levels.keys()))
        rows.append({"seed": seed, "k": 10, "query_id": "DRIFT", "mode": "current_site_agent_state", "success": 1, "relative_sum_error": error, "final_levels": levels_to_names(levels), **meta})

    df = pd.DataFrame(rows)
    df.to_csv(output_dir / "orchestration_results.csv", index=False)
    summary = df.groupby(["mode"], as_index=False).agg(
        success_rate=("success", "mean"),
        mean_candidates=("tested_candidates", "mean"),
        mean_probe_messages=("probe_messages", "mean"),
        mean_unsupported=("unsupported_replies", "mean"),
        mean_info_loss=("information_loss", "mean"),
        mean_capability_filtered=("capability_filtered_candidates", "mean"),
        mean_events=("orchestration_events_total", "mean"),
    )
    summary.to_csv(output_dir / "orchestration_summary.csv", index=False)
    (output_dir / "orchestration_summary.md").write_text(summary.to_markdown(index=False), encoding="utf-8")
    return df
