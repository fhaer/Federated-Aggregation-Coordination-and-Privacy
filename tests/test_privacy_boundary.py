from pathlib import Path
import tempfile
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from federated_kpi.core import PrivacyPolicy, QuerySpec
from federated_kpi.orchestration import LocalAnalyticsAgent, default_capability_profiles
from federated_kpi.synthetic import create_federation


def test_capability_manifest_does_not_expose_private_local_threshold():
    with tempfile.TemporaryDirectory() as td:
        party = create_federation(Path(td), n_parties=1, customers_per_party=80, facts_per_party=500, seed=21)[0]
        cap = default_capability_profiles([party], heterogeneous=False)[0]
        agent = LocalAnalyticsAgent(party, cap, local_min_k=37)
        manifest = agent.discover_capabilities()
        assert "local_min_k" not in manifest
        assert "k_i" not in manifest
        assert "threshold" not in manifest


def test_probe_reply_exposes_status_not_local_counts():
    with tempfile.TemporaryDirectory() as td:
        party = create_federation(Path(td), n_parties=1, customers_per_party=80, facts_per_party=500, seed=22)[0]
        cap = default_capability_profiles([party], heterogeneous=False)[0]
        agent = LocalAnalyticsAgent(party, cap, local_min_k=7)
        query = QuerySpec("boundary", {"geo": 2, "age": 3})
        reply = agent.probe(query, {"geo": 2, "age": 3}, requested_k=5)
        assert reply.status in {"SAFE", "UNSAFE", "UNSUPPORTED"}
        assert not hasattr(reply, "min_count")
