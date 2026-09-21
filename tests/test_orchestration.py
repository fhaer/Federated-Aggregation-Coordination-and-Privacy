from dataclasses import replace
from pathlib import Path
import tempfile
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from federated_kpi.core import Coordinator, PrivacyPolicy, QuerySpec
from federated_kpi.orchestration import AgentOrchestrator, LocalAnalyticsAgent, default_capability_profiles
from federated_kpi.synthetic import create_federation


def test_orchestration_invariants():
    with tempfile.TemporaryDirectory() as td:
        parties = create_federation(Path(td), n_parties=4, customers_per_party=140, facts_per_party=1200, seed=9)
        caps = default_capability_profiles(parties, heterogeneous=True)
        agents = [LocalAnalyticsAgent(p, c, local_min_k=5) for p, c in zip(parties, caps)]
        q = QuerySpec("agent-test", {"geo": 0, "age": 0}, measure="revenue")
        policy = PrivacyPolicy(k=8)
        orch = AgentOrchestrator(agents)
        core_candidates = Coordinator.candidate_levels(q, policy)
        assert core_candidates[0] == dict(q.group_levels)
        manifests0 = orch.discover(q)
        feasible, filtered = orch.capability_compatible_candidates(q, policy, manifests0)
        assert filtered > 0
        assert feasible[0]["geo"] >= 1 and feasible[0]["age"] >= 1
        orch = AgentOrchestrator(agents)
        fused, meta, manifests = orch.execute(q, policy)
        commit = next(e for e in orch.events if e.kind == "PLAN_COMMIT")
        levels = dict(commit.payload["levels"])
        assert levels["geo"] >= 1
        assert levels["age"] >= 1
        oracle = Coordinator(parties).centralized_oracle(q, levels)
        assert fused.equals(oracle)
        assert meta["orchestration_events_total"] > meta["probe_messages"]
        assert set(manifests) == {p.party_id for p in parties}
        try:
            agents[0].release(q, levels, policy.k)
        except PermissionError:
            pass
        else:
            raise AssertionError("second release should be denied by the stateful release budget")


def test_live_measure_capability_filters_infeasible_query_before_probing():
    with tempfile.TemporaryDirectory() as td:
        parties = create_federation(Path(td), n_parties=2, customers_per_party=80, facts_per_party=500, seed=12)
        caps = default_capability_profiles(parties, heterogeneous=False)
        caps[1] = replace(caps[1], supported_measures=("quantity",))
        agents = [LocalAnalyticsAgent(p, c) for p, c in zip(parties, caps)]
        q = QuerySpec("measure-filter", {"geo": 0}, measure="revenue")
        policy = PrivacyPolicy(k=5)
        orch = AgentOrchestrator(agents)
        manifests = orch.discover(q)
        feasible, filtered = orch.capability_compatible_candidates(q, policy, manifests)
        assert feasible == []
        assert filtered == len(Coordinator.candidate_levels(q, policy))


if __name__ == "__main__":
    test_orchestration_invariants()
    test_live_measure_capability_filters_infeasible_query_before_probing()
    print("All orchestration tests passed")
