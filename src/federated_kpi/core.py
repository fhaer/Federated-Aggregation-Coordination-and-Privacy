from __future__ import annotations

from dataclasses import dataclass, field
from itertools import product
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence, Tuple
import json
import sqlite3
import time

import numpy as np
import pandas as pd


# Ordered from most detailed (index 0) to most general.
HIERARCHIES: Dict[str, Tuple[str, ...]] = {
    "geo": ("city", "state", "country", "all_geo"),
    "age": ("age", "age_decade", "age_band", "all_age"),
    "product": ("product_name", "category", "all_product"),
    "time": ("month", "quarter", "year", "all_time"),
}

DIMENSION_TABLE: Dict[str, str] = {
    "geo": "c",
    "age": "c",
    "product": "p",
    "time": "d",
}


@dataclass(frozen=True)
class QuerySpec:
    """Validated OLAP query specification.

    ``group_levels`` maps dimension names to hierarchy indices.
    """

    query_id: str
    group_levels: Mapping[str, int]
    measure: str = "revenue"
    year_filter: int | None = 2025

    def validate(self) -> None:
        if self.measure not in {"revenue", "quantity"}:
            raise ValueError(f"Unsupported measure: {self.measure}")
        if not self.group_levels:
            raise ValueError("At least one grouping dimension is required")
        for dim, level in self.group_levels.items():
            if dim not in HIERARCHIES:
                raise ValueError(f"Unknown dimension: {dim}")
            if level < 0 or level >= len(HIERARCHIES[dim]):
                raise ValueError(f"Invalid level {level} for {dim}")


@dataclass(frozen=True)
class PrivacyPolicy:
    k: int
    weights: Mapping[str, float] = field(default_factory=dict)

    def weight(self, dim: str) -> float:
        return float(self.weights.get(dim, 1.0))


@dataclass
class ProbeResult:
    safe: bool
    latency_ms: float
    groups: int
    # min_count is retained only for experimental diagnostics and is never
    # part of the protocol payload sent to the coordinator.
    min_count: int | None = None


@dataclass
class AggregateResult:
    data: pd.DataFrame
    latency_ms: float


class LocalParty:
    """A federation participant with a local star-schema SQLite database."""

    def __init__(self, party_id: str, db_path: Path, local_min_k: int = 1):
        self.party_id = party_id
        self.db_path = Path(db_path)
        self.local_min_k = int(local_min_k)
        if self.local_min_k < 1:
            raise ValueError("local_min_k must be >= 1")

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    @staticmethod
    def _select_expr(dim: str, level: int) -> str:
        col = HIERARCHIES[dim][level]
        alias = DIMENSION_TABLE[dim]
        return f'{alias}."{col}" AS "{dim}"'

    @staticmethod
    def _joins() -> str:
        return (
            " FROM fact_sales f "
            " JOIN dim_customer c ON f.customer_key = c.customer_key "
            " JOIN dim_product p ON f.product_key = p.product_key "
            " JOIN dim_date d ON f.date_key = d.date_key "
        )

    def _where(self, query: QuerySpec) -> Tuple[str, List[object]]:
        if query.year_filter is None:
            return "", []
        return " WHERE d.year = ? ", [int(query.year_filter)]

    def group_counts(self, query: QuerySpec, levels: Mapping[str, int]) -> pd.DataFrame:
        select = [self._select_expr(dim, levels[dim]) for dim in query.group_levels]
        where, params = self._where(query)
        sql = (
            "SELECT " + ", ".join(select) + ", COUNT(*) AS n" + self._joins() + where
            + " GROUP BY " + ", ".join(str(i) for i in range(1, len(select) + 1))
        )
        with self._connect() as conn:
            return pd.read_sql_query(sql, conn, params=params)

    def privacy_probe(
        self, query: QuerySpec, levels: Mapping[str, int], k: int
    ) -> ProbeResult:
        start = time.perf_counter()
        counts = self.group_counts(query, levels)
        effective_k = max(int(k), self.local_min_k)
        if counts.empty:
            safe = True
            min_count = None
        else:
            min_count = int(counts["n"].min())
            safe = min_count >= effective_k
        latency_ms = (time.perf_counter() - start) * 1000.0
        return ProbeResult(
            safe=safe,
            latency_ms=latency_ms,
            groups=len(counts),
            min_count=min_count,
        )

    def aggregate(
        self,
        query: QuerySpec,
        levels: Mapping[str, int],
        k: int | None = None,
        suppress_unsafe: bool = False,
    ) -> AggregateResult:
        query.validate()
        start = time.perf_counter()
        select = [self._select_expr(dim, levels[dim]) for dim in query.group_levels]
        where, params = self._where(query)
        m = f'f."{query.measure}"'
        sql = (
            "SELECT "
            + ", ".join(select)
            + f", COUNT(*) AS n, SUM({m}) AS sum_value, SUM({m} * {m}) AS sumsq_value"
            + self._joins()
            + where
            + " GROUP BY "
            + ", ".join(str(i) for i in range(1, len(select) + 1))
        )
        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=params)
        if suppress_unsafe:
            if k is None:
                raise ValueError("k is required when suppress_unsafe=True")
            effective_k = max(int(k), self.local_min_k)
            df = df[df["n"] >= effective_k].copy()
        df["party_id"] = self.party_id
        latency_ms = (time.perf_counter() - start) * 1000.0
        return AggregateResult(df, latency_ms)

    def guarded_aggregate(
        self, query: QuerySpec, levels: Mapping[str, int], k: int
    ) -> AggregateResult:
        """Repeat the local cohort check immediately before aggregate release."""
        start = time.perf_counter()
        effective_k = max(int(k), self.local_min_k)
        guard = self.privacy_probe(query, levels, effective_k)
        if not guard.safe:
            raise PermissionError(f"{self.party_id}: plan is no longer k-safe")
        out = self.aggregate(query, levels)
        if not out.data.empty and int(out.data["n"].min()) < effective_k:
            raise AssertionError("Local release guard invariant violated")
        out.latency_ms = (time.perf_counter() - start) * 1000.0
        return out

    def row_count(self, query: QuerySpec) -> int:
        where, params = self._where(query)
        sql = "SELECT COUNT(*) AS n" + self._joins() + where
        with self._connect() as conn:
            return int(pd.read_sql_query(sql, conn, params=params).iloc[0]["n"])


class Coordinator:
    """Coordinates privacy probes and fuses additive sufficient statistics."""

    def __init__(self, parties: Sequence[LocalParty]):
        self.parties = list(parties)

    @staticmethod
    def information_loss(query: QuerySpec, levels: Mapping[str, int], policy: PrivacyPolicy) -> float:
        numerator = 0.0
        denominator = 0.0
        for dim, requested in query.group_levels.items():
            max_level = len(HIERARCHIES[dim]) - 1
            if max_level == requested:
                continue
            w = policy.weight(dim)
            numerator += w * ((levels[dim] - requested) / (max_level - requested))
            denominator += w
        return numerator / denominator if denominator else 0.0

    @staticmethod
    def candidate_levels(
        query: QuerySpec,
        policy: PrivacyPolicy,
    ) -> List[Dict[str, int]]:
        """Enumerate OLAP hierarchy vectors in increasing information loss.

        Deployment capabilities are applied by the coordination layer before
        privacy probing.
        """
        dims = list(query.group_levels.keys())
        ranges = [
            range(query.group_levels[d], len(HIERARCHIES[d]))
            for d in dims
        ]
        candidates = [dict(zip(dims, values)) for values in product(*ranges)]
        candidates.sort(
            key=lambda c: (
                Coordinator.information_loss(query, c, policy),
                sum(c[d] - query.group_levels[d] for d in dims),
                tuple(c[d] for d in dims),
            )
        )
        return candidates

    def negotiate_min_loss(
        self,
        query: QuerySpec,
        policy: PrivacyPolicy,
    ) -> Tuple[Dict[str, int], Dict[str, float]]:
        """Find the minimum-loss hierarchy vector accepted by every party.

        Parties return only ``SAFE`` or ``UNSAFE`` during negotiation.
        """
        query.validate()
        probes = 0
        probe_latency_ms = 0.0
        tested_candidates = 0
        for candidate in self.candidate_levels(query, policy):
            tested_candidates += 1
            unanimous = True
            for party in self.parties:
                r = party.privacy_probe(query, candidate, policy.k)
                probes += 1
                probe_latency_ms += r.latency_ms
                if not r.safe:
                    unanimous = False
            if unanimous:
                return candidate, {
                    "probe_messages": float(probes),
                    "tested_candidates": float(tested_candidates),
                    "probe_latency_ms": probe_latency_ms,
                    "information_loss": self.information_loss(query, candidate, policy),
                }
        raise RuntimeError("No safe generalization found; ALL-level should normally be safe")

    def fuse(
        self,
        query: QuerySpec,
        levels: Mapping[str, int],
        policy: PrivacyPolicy | None = None,
        suppress_unsafe: bool = False,
    ) -> Tuple[pd.DataFrame, Dict[str, float]]:
        frames: List[pd.DataFrame] = []
        latency_ms = 0.0
        bytes_out = 0
        rows_out = 0
        for party in self.parties:
            if suppress_unsafe:
                r = party.aggregate(
                    query, levels, k=policy.k if policy else None, suppress_unsafe=True
                )
            elif policy is not None:
                # A committed plan is never released on the strength of a stale
                # negotiation result: every site re-checks immediately before release.
                r = party.guarded_aggregate(query, levels, policy.k)
            else:
                # Used only by the centralized-equivalent oracle/diagnostics.
                r = party.aggregate(query, levels)
            frames.append(r.data)
            latency_ms += r.latency_ms
            rows_out += len(r.data)
            # JSON size approximates the transport payload independently of framing.
            bytes_out += len(r.data.to_json(orient="records").encode("utf-8"))

        group_cols = list(query.group_levels.keys())
        if frames:
            combined = pd.concat(frames, ignore_index=True)
        else:
            combined = pd.DataFrame(columns=group_cols + ["n", "sum_value", "sumsq_value"])
        if combined.empty:
            fused = combined.drop(columns=["party_id"], errors="ignore")
        else:
            fused = (
                combined.groupby(group_cols, dropna=False, as_index=False)[["n", "sum_value", "sumsq_value"]]
                .sum()
            )
            fused["mean_value"] = fused["sum_value"] / fused["n"]
            # Population variance from additive sufficient statistics.
            fused["variance_value"] = np.maximum(
                0.0, fused["sumsq_value"] / fused["n"] - fused["mean_value"] ** 2
            )
        return fused, {
            "aggregate_latency_ms": latency_ms,
            "aggregate_rows_sent": float(rows_out),
            "aggregate_bytes_sent": float(bytes_out),
        }

    def centralized_oracle(self, query: QuerySpec, levels: Mapping[str, int]) -> pd.DataFrame:
        # Still computed by fusing local exact sufficient statistics, but without privacy gating.
        return self.fuse(query, levels)[0]

    def total_rows(self, query: QuerySpec) -> int:
        return sum(p.row_count(query) for p in self.parties)


def levels_to_names(levels: Mapping[str, int]) -> str:
    return ";".join(f"{d}={HIERARCHIES[d][i]}" for d, i in levels.items())


def weighted_relative_sum_error(estimated: pd.DataFrame, exact: pd.DataFrame, group_cols: Sequence[str]) -> float:
    """Weighted absolute relative error over group SUMs; missing groups count as zero."""
    if exact.empty:
        return 0.0
    e = exact[group_cols + ["sum_value"]].rename(columns={"sum_value": "exact_sum"})
    a = estimated[group_cols + ["sum_value"]].rename(columns={"sum_value": "est_sum"})
    merged = e.merge(a, on=list(group_cols), how="left")
    merged["est_sum"] = merged["est_sum"].fillna(0.0)
    abs_err = (merged["exact_sum"] - merged["est_sum"]).abs().sum()
    denom = merged["exact_sum"].abs().sum()
    return float(abs_err / denom) if denom > 0 else 0.0


def protocol_message(kind: str, party_id: str, payload: Mapping[str, object]) -> str:
    """Canonical JSON envelope suitable for structured message transport."""
    return json.dumps(
        {"protocol": "federated-kpi/0.3", "type": kind, "party_id": party_id, "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
    )
