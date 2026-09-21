from __future__ import annotations

from pathlib import Path
import time
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from .core import Coordinator, PrivacyPolicy, QuerySpec, levels_to_names, weighted_relative_sum_error
from .synthetic import create_federation


WORKLOAD = [
    QuerySpec("Q1", {"geo": 0, "age": 1}, measure="revenue"),
    QuerySpec("Q2", {"geo": 0, "product": 1}, measure="revenue"),
    QuerySpec("Q3", {"geo": 0, "age": 1, "product": 1}, measure="revenue"),
    QuerySpec("Q4", {"geo": 1, "age": 1, "time": 0}, measure="revenue"),
    QuerySpec("Q5", {"geo": 0, "time": 0}, measure="revenue"),
    QuerySpec("Q6", {"age": 0, "product": 1}, measure="revenue"),
]


def _suppression_metrics(coord: Coordinator, query: QuerySpec, policy: PrivacyPolicy) -> Dict[str, float | str]:
    requested = dict(query.group_levels)
    exact = coord.centralized_oracle(query, requested)
    est, transport = coord.fuse(query, requested, policy=policy, suppress_unsafe=True)
    total_rows = coord.total_rows(query)
    released_rows = float(est["n"].sum()) if not est.empty else 0.0
    err = weighted_relative_sum_error(est, exact, list(query.group_levels.keys()))
    return {
        "strategy": "local_suppression",
        "final_levels": levels_to_names(requested),
        "information_loss": 0.0,
        "row_coverage": released_rows / total_rows if total_rows else 1.0,
        "relative_sum_error": err,
        "probe_messages": 0.0,
        "tested_candidates": 0.0,
        **transport,
    }


def _generalization_metrics(coord: Coordinator, query: QuerySpec, policy: PrivacyPolicy) -> Dict[str, float | str]:
    start = time.perf_counter()
    levels, negotiation = coord.negotiate_min_loss(query, policy)
    est, transport = coord.fuse(query, levels, policy=policy, suppress_unsafe=False)
    exact_at_output = coord.centralized_oracle(query, levels)
    total_rows = coord.total_rows(query)
    released_rows = float(est["n"].sum()) if not est.empty else 0.0
    err = weighted_relative_sum_error(est, exact_at_output, list(query.group_levels.keys()))
    wall_ms = (time.perf_counter() - start) * 1000.0
    return {
        "strategy": "coordinated_rollup",
        "final_levels": levels_to_names(levels),
        "information_loss": negotiation["information_loss"],
        "row_coverage": released_rows / total_rows if total_rows else 1.0,
        "relative_sum_error": err,
        "probe_messages": negotiation["probe_messages"],
        "tested_candidates": negotiation["tested_candidates"],
        "probe_latency_ms": negotiation["probe_latency_ms"],
        "wall_latency_ms": wall_ms,
        **transport,
    }


def run_experiment(
    output_dir: Path,
    seeds: Iterable[int] = (11, 22, 33),
    ks: Iterable[int] = (3, 5, 10, 20),
    n_parties: int = 4,
    customers_per_party: int = 450,
    facts_per_party: int = 5000,
) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    all_rows: List[Dict[str, object]] = []

    for seed in seeds:
        db_root = output_dir / f"db_seed_{seed}"
        parties = create_federation(
            db_root,
            n_parties=n_parties,
            customers_per_party=customers_per_party,
            facts_per_party=facts_per_party,
            seed=seed,
        )
        coord = Coordinator(parties)
        for k in ks:
            policy = PrivacyPolicy(k=k, weights={"geo": 1.2, "age": 1.0, "product": 0.8, "time": 1.0})
            for q in WORKLOAD:
                for fn in (_suppression_metrics, _generalization_metrics):
                    metrics = fn(coord, q, policy)
                    metrics.update({"seed": seed, "k": k, "query_id": q.query_id, "n_parties": n_parties})
                    all_rows.append(metrics)

    df = pd.DataFrame(all_rows)
    df.to_csv(output_dir / "experiment_results.csv", index=False)
    _make_figures(df, output_dir)
    _make_summary(df, output_dir)
    return df


def _make_figures(df: pd.DataFrame, output_dir: Path) -> None:
    summary = df.groupby(["strategy", "k"], as_index=False).agg(
        row_coverage=("row_coverage", "mean"),
        information_loss=("information_loss", "mean"),
        relative_sum_error=("relative_sum_error", "mean"),
        aggregate_bytes_sent=("aggregate_bytes_sent", "mean"),
    )

    fig = plt.figure(figsize=(6.3, 4.2))
    ax = fig.add_subplot(111)
    for strategy, g in summary.groupby("strategy"):
        ax.plot(g["k"], g["row_coverage"], marker="o", label=strategy.replace("_", " "))
    ax.set_xlabel("Minimum cohort threshold k")
    ax.set_ylabel("Mean row coverage")
    ax.set_ylim(0, 1.05)
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "coverage_vs_k.png", dpi=180)
    plt.close(fig)

    gen = summary[summary["strategy"] == "coordinated_rollup"]
    fig = plt.figure(figsize=(6.3, 4.2))
    ax = fig.add_subplot(111)
    ax.plot(gen["k"], gen["information_loss"], marker="o")
    ax.set_xlabel("Minimum cohort threshold k")
    ax.set_ylabel("Normalized hierarchy information loss")
    ax.set_ylim(0, 1.0)
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_dir / "information_loss_vs_k.png", dpi=180)
    plt.close(fig)

    sup = summary[summary["strategy"] == "local_suppression"]
    fig = plt.figure(figsize=(6.3, 4.2))
    ax = fig.add_subplot(111)
    ax.plot(sup["k"], sup["relative_sum_error"], marker="o")
    ax.set_xlabel("Minimum cohort threshold k")
    ax.set_ylabel("Relative error of SUM at requested granularity")
    ax.set_ylim(bottom=0)
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_dir / "suppression_error_vs_k.png", dpi=180)
    plt.close(fig)


def _make_summary(df: pd.DataFrame, output_dir: Path) -> None:
    agg = df.groupby(["strategy", "k"], as_index=False).agg(
        mean_coverage=("row_coverage", "mean"),
        mean_info_loss=("information_loss", "mean"),
        mean_relative_sum_error=("relative_sum_error", "mean"),
        mean_rows_sent=("aggregate_rows_sent", "mean"),
        mean_bytes_sent=("aggregate_bytes_sent", "mean"),
        mean_candidates=("tested_candidates", "mean"),
    )
    agg.to_csv(output_dir / "summary.csv", index=False)
    (output_dir / "summary.md").write_text(agg.to_markdown(index=False), encoding="utf-8")
