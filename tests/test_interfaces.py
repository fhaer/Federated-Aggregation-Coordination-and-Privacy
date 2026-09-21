from pathlib import Path
import json
import os
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from federated_kpi.buzz_transport import parse_protocol_content
from federated_kpi.orchestration import OrchestrationEvent
from federated_kpi.synthetic import create_federation


def test_canonical_audit_event_is_parseable_by_buzz_adapter():
    e = OrchestrationEvent(1, "P1", "PLAN_REPLY", "Q1", {"status": "SAFE"}, created_at=1.0)
    parsed = parse_protocol_content(e.canonical_json())
    assert parsed is not None
    assert parsed["kind"] == "PLAN_REPLY"
    assert parsed["actor"] == "P1"


def test_audit_dataframe_has_stable_schema():
    from federated_kpi.orchestration import AgentOrchestrator

    orch = AgentOrchestrator([])
    orch._event("coordinator", "CAPABILITY_REQUEST", "Q1", {})
    frame = orch.audit_dataframe()
    assert list(frame.columns) == ["seq", "actor", "kind", "query_id", "payload"]


def test_local_probe_public_output_does_not_reveal_group_cardinality_or_timing():
    with tempfile.TemporaryDirectory() as td:
        party = create_federation(Path(td), n_parties=1, customers_per_party=80, facts_per_party=500, seed=5)[0]
        query = json.dumps({"query_id": "tool-test", "group_levels": {"geo": 0, "age": 1}})
        levels = json.dumps({"geo": 2, "age": 3})
        env = dict(os.environ)
        env["FEDERATED_KPI_LOCAL_MIN_K"] = "7"
        proc = subprocess.run(
            [sys.executable, str(ROOT / "local_tool.py"), "probe", "--party", "P1", "--db", str(party.db_path),
             "--query", query, "--levels", levels, "--k", "5"],
            text=True, capture_output=True, check=True, env=env,
        )
        out = json.loads(proc.stdout)
        assert set(out) == {"party_id", "status"}
        assert out["status"] in {"SAFE", "UNSAFE"}


def test_local_aggregate_release_respects_private_site_threshold():
    with tempfile.TemporaryDirectory() as td:
        party = create_federation(Path(td), n_parties=1, customers_per_party=80, facts_per_party=500, seed=6)[0]
        query = json.dumps({"query_id": "release-test", "group_levels": {"geo": 3, "age": 3}})
        levels = json.dumps({"geo": 3, "age": 3})
        env = dict(os.environ)
        env["FEDERATED_KPI_LOCAL_MIN_K"] = "10000"
        proc = subprocess.run(
            [sys.executable, str(ROOT / "local_tool.py"), "aggregate", "--party", "P1", "--db", str(party.db_path),
             "--query", query, "--levels", levels, "--k", "1"],
            text=True, capture_output=True, check=False, env=env,
        )
        assert proc.returncode != 0
        assert "Refusing release" in (proc.stderr + proc.stdout)


if __name__ == "__main__":
    test_canonical_audit_event_is_parseable_by_buzz_adapter()
    test_audit_dataframe_has_stable_schema()
    test_local_probe_public_output_does_not_reveal_group_cardinality_or_timing()
    test_local_aggregate_release_respects_private_site_threshold()
    print("All interface tests passed")
