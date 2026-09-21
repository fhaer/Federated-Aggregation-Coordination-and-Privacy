from __future__ import annotations

"""Deterministic agent coordination for federated KPI aggregation.

Site agents hold capability and authorization state, invoke local tools, and
exchange structured events. Privacy checks and KPI calculations remain in
deterministic local functions. The harness is independent of a specific agent
runtime or message transport.
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, Iterable, List, Mapping, Sequence, Tuple
import json
import time

import numpy as np
import pandas as pd

from .core import (
    HIERARCHIES,
    LocalParty,
    PrivacyPolicy,
    QuerySpec,
    Coordinator,
    levels_to_names,
)


@dataclass(frozen=True)
class SiteCapability:
    """Public/non-data-derived capabilities owned by one site agent.

    ``min_levels`` are capability floors, not privacy results.  For example a
    local site may expose geography only from state/province upward.  The local
    vocabulary demonstrates that canonical concepts need not use identical
    labels across organizations.
    """

    party_id: str
    min_levels: Mapping[str, int] = field(default_factory=dict)
    supported_measures: Tuple[str, ...] = ("revenue", "quantity")
    local_vocabulary: Mapping[str, Tuple[str, ...]] = field(default_factory=dict)
    capability_version: str = "1"

    def minimum_level(self, dim: str) -> int:
        return int(self.min_levels.get(dim, 0))

    def public_manifest(self) -> Dict[str, object]:
        return {
            "party_id": self.party_id,
            "min_levels": dict(self.min_levels),
            "supported_measures": list(self.supported_measures),
            "local_vocabulary": {k: list(v) for k, v in self.local_vocabulary.items()},
            "capability_version": self.capability_version,
        }


@dataclass
class OrchestrationEvent:
    seq: int
    actor: str
    kind: str
    query_id: str
    payload: Mapping[str, object]
    created_at: float = field(default_factory=time.time)

    def canonical_json(self) -> str:
        return json.dumps(
            {
                "protocol": "federated-kpi/0.3",
                "seq": self.seq,
                "actor": self.actor,
                "kind": self.kind,
                "query_id": self.query_id,
                "payload": self.payload,
                "created_at": round(self.created_at, 6),
            },
            sort_keys=True,
            separators=(",", ":"),
        )


@dataclass
class AgentProbeReply:
    party_id: str
    status: str  # SAFE, UNSAFE, UNSUPPORTED
    capability_version: str
    latency_ms: float


class LocalAnalyticsAgent:
    """Site-agent wrapper around deterministic local tools.

    The class holds current capability state, invokes approved functions, tracks
    releases, and refuses unsupported requests. It is used by the reproducible
    coordination harness and tests.
    """

    def __init__(
        self,
        party: LocalParty,
        capability: SiteCapability,
        local_min_k: int = 1,
        max_releases_per_query: int = 1,
    ):
        if capability.party_id != party.party_id:
            raise ValueError("Capability party_id must match LocalParty")
        self.party = party
        self.capability = capability
        self.local_min_k = int(local_min_k)
        self.max_releases_per_query = int(max_releases_per_query)
        self._release_counts: Dict[str, int] = {}

    def discover_capabilities(self) -> Dict[str, object]:
        return self.capability.public_manifest()

    def supports(self, query: QuerySpec, levels: Mapping[str, int]) -> bool:
        if query.measure not in self.capability.supported_measures:
            return False
        for dim in query.group_levels:
            if dim not in HIERARCHIES:
                return False
            if int(levels[dim]) < self.capability.minimum_level(dim):
                return False
        return True

    def probe(self, query: QuerySpec, levels: Mapping[str, int], requested_k: int) -> AgentProbeReply:
        start = time.perf_counter()
        if not self.supports(query, levels):
            return AgentProbeReply(
                self.party.party_id,
                "UNSUPPORTED",
                self.capability.capability_version,
                (time.perf_counter() - start) * 1000.0,
            )
        effective_k = max(int(requested_k), self.local_min_k)
        r = self.party.privacy_probe(query, levels, effective_k)
        return AgentProbeReply(
            self.party.party_id,
            "SAFE" if r.safe else "UNSAFE",
            self.capability.capability_version,
            r.latency_ms,
        )

    def release(self, query: QuerySpec, levels: Mapping[str, int], requested_k: int) -> pd.DataFrame:
        if not self.supports(query, levels):
            raise PermissionError(f"{self.party.party_id}: unsupported plan")
        count = self._release_counts.get(query.query_id, 0)
        if count >= self.max_releases_per_query:
            raise PermissionError(f"{self.party.party_id}: release budget exhausted")
        # Use the same pre-release check as the core aggregation path.
        effective_k = max(int(requested_k), self.local_min_k)
        out = self.party.guarded_aggregate(query, levels, effective_k).data.copy()
        self._release_counts[query.query_id] = count + 1
        return out


class AgentOrchestrator:
    """Coordinator over independent site agents.

    Planning uses public capability manifests. Data-derived safety decisions stay
    local. Events are stored in a transportable structured form.
    """

    def __init__(self, agents: Sequence[LocalAnalyticsAgent]):
        self.agents = list(agents)
        self.events: List[OrchestrationEvent] = []
        self._seq = 0

    def _event(self, actor: str, kind: str, query_id: str, payload: Mapping[str, object]) -> None:
        self._seq += 1
        self.events.append(OrchestrationEvent(self._seq, actor, kind, query_id, dict(payload)))

    def discover(self, query: QuerySpec) -> Dict[str, Dict[str, object]]:
        manifests: Dict[str, Dict[str, object]] = {}
        self._event("coordinator", "CAPABILITY_REQUEST", query.query_id, {})
        for agent in self.agents:
            m = agent.discover_capabilities()
            manifests[agent.party.party_id] = m
            self._event(agent.party.party_id, "CAPABILITY", query.query_id, m)
        return manifests

    @staticmethod
    def capability_floor(query: QuerySpec, manifests: Mapping[str, Mapping[str, object]]) -> Dict[str, int]:
        """Return the common capability floor for the current federation.

        Capability metadata removes plans that cannot be executed before privacy
        probing begins. It is not part of the privacy objective.
        """
        floor = dict(query.group_levels)
        for manifest in manifests.values():
            mins = manifest.get("min_levels", {})
            if not isinstance(mins, Mapping):
                continue
            for dim in query.group_levels:
                floor[dim] = max(floor[dim], int(mins.get(dim, 0)))
        return floor

    @classmethod
    def capability_compatible_candidates(
        cls,
        query: QuerySpec,
        requested_policy: PrivacyPolicy,
        manifests: Mapping[str, Mapping[str, object]],
    ) -> Tuple[List[Dict[str, int]], int]:
        """Filter candidate levels using current site capabilities.

        Returns ``(feasible_candidates, number_filtered)``. Ordering follows the
        core information-loss order. Stale manifests may still cause runtime
        refusal.
        """
        all_candidates = Coordinator.candidate_levels(query, requested_policy)
        # A measure unsupported by one participant removes all candidates.
        for manifest in manifests.values():
            measures = manifest.get("supported_measures")
            if isinstance(measures, (list, tuple, set)) and query.measure not in measures:
                return [], len(all_candidates)
        floor = cls.capability_floor(query, manifests)
        feasible = [
            c for c in all_candidates
            if all(int(c[d]) >= int(floor[d]) for d in query.group_levels)
        ]
        return feasible, len(all_candidates) - len(feasible)

    def negotiate(
        self,
        query: QuerySpec,
        requested_policy: PrivacyPolicy,
        discover_capabilities: bool = True,
        cached_manifests: Mapping[str, Mapping[str, object]] | None = None,
    ) -> Tuple[Dict[str, int], Dict[str, float], Dict[str, Dict[str, object]]]:
        query.validate()
        if discover_capabilities:
            manifests = self.discover(query)
        elif cached_manifests is not None:
            manifests = {k: dict(v) for k, v in cached_manifests.items()}
        else:
            manifests = {}
        candidates, capability_filtered = self.capability_compatible_candidates(
            query, requested_policy, manifests
        )

        tested = 0
        probe_replies = 0
        probe_latency = 0.0
        unsupported = 0
        unsafe = 0
        for candidate in candidates:
            tested += 1
            self._event(
                "coordinator",
                "PLAN_PROPOSAL",
                query.query_id,
                {"levels": dict(candidate), "level_names": levels_to_names(candidate)},
            )
            all_safe = True
            for agent in self.agents:
                r = agent.probe(query, candidate, requested_policy.k)
                probe_replies += 1
                probe_latency += r.latency_ms
                unsupported += int(r.status == "UNSUPPORTED")
                unsafe += int(r.status == "UNSAFE")
                self._event(
                    agent.party.party_id,
                    "PLAN_REPLY",
                    query.query_id,
                    {
                        "status": r.status,
                        "capability_version": r.capability_version,
                    },
                )
                if r.status != "SAFE":
                    all_safe = False
            if all_safe:
                self._event(
                    "coordinator",
                    "PLAN_COMMIT",
                    query.query_id,
                    {"levels": dict(candidate), "level_names": levels_to_names(candidate)},
                )
                return candidate, {
                    "tested_candidates": float(tested),
                    "probe_messages": float(probe_replies),
                    "probe_latency_ms": probe_latency,
                    "unsupported_replies": float(unsupported),
                    "unsafe_replies": float(unsafe),
                    "information_loss": Coordinator.information_loss(query, candidate, requested_policy),
                    "capability_filtered_candidates": float(capability_filtered),
                    "orchestration_events_before_release": float(len(self.events)),
                }, manifests
        raise RuntimeError("No mutually supported k-safe candidate found")

    def execute(
        self,
        query: QuerySpec,
        requested_policy: PrivacyPolicy,
        discover_capabilities: bool = True,
        cached_manifests: Mapping[str, Mapping[str, object]] | None = None,
    ) -> Tuple[pd.DataFrame, Dict[str, float], Dict[str, Dict[str, object]]]:
        levels, meta, manifests = self.negotiate(
            query,
            requested_policy,
            discover_capabilities=discover_capabilities,
            cached_manifests=cached_manifests,
        )
        frames: List[pd.DataFrame] = []
        rows_sent = 0
        bytes_sent = 0
        for agent in self.agents:
            self._event("coordinator", "AGGREGATE_REQUEST", query.query_id, {"levels": dict(levels)})
            frame = agent.release(query, levels, requested_policy.k)
            frames.append(frame)
            rows_sent += len(frame)
            payload = frame.drop(columns=["party_id"], errors="ignore").to_json(orient="records")
            bytes_sent += len(payload.encode("utf-8"))
            self._event(
                agent.party.party_id,
                "AGGREGATE_RELEASE",
                query.query_id,
                {"rows": int(len(frame)), "payload_bytes": len(payload.encode("utf-8"))},
            )

        group_cols = list(query.group_levels.keys())
        combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if combined.empty:
            fused = pd.DataFrame(columns=group_cols + ["n", "sum_value", "sumsq_value", "mean_value", "variance_value"])
        else:
            fused = (
                combined.groupby(group_cols, dropna=False, as_index=False)[["n", "sum_value", "sumsq_value"]]
                .sum()
            )
            fused["mean_value"] = fused["sum_value"] / fused["n"]
            fused["variance_value"] = np.maximum(
                0.0, fused["sumsq_value"] / fused["n"] - fused["mean_value"] ** 2
            )
        meta.update(
            {
                "aggregate_rows_sent": float(rows_sent),
                "aggregate_bytes_sent": float(bytes_sent),
                "orchestration_events_total": float(len(self.events)),
            }
        )
        return fused, meta, manifests

    def audit_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "seq": e.seq,
                    "actor": e.actor,
                    "kind": e.kind,
                    "query_id": e.query_id,
                    "payload": json.dumps(e.payload, sort_keys=True),
                }
                for e in self.events
            ]
        )


def default_capability_profiles(parties: Sequence[LocalParty], heterogeneous: bool = True) -> List[SiteCapability]:
    """Build reproducible public capability profiles for experiments."""
    vocab = [
        {"geo": ("city", "state", "country", "ALL"), "age": ("age", "decade", "band", "ALL")},
        {"geo": ("municipality", "province", "country", "ALL"), "age": ("exact_age", "decade", "band", "ALL")},
        {"geo": ("locality", "region", "country", "ALL"), "age": ("age", "ten_year_band", "broad_band", "ALL")},
        {"geo": ("town", "administrative_region", "country", "ALL"), "age": ("age", "decade", "segment", "ALL")},
    ]
    floors = [
        {},
        {"age": 1} if heterogeneous else {},
        {"geo": 1} if heterogeneous else {},
        {"time": 1} if heterogeneous else {},
    ]
    out: List[SiteCapability] = []
    for idx, p in enumerate(parties):
        out.append(
            SiteCapability(
                party_id=p.party_id,
                min_levels=floors[idx % len(floors)],
                local_vocabulary=vocab[idx % len(vocab)],
                capability_version="1",
            )
        )
    return out
