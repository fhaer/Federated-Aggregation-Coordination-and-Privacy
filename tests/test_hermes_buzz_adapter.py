from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import os
import stat
import sys
import tempfile
import time

import pandas as pd
from pandas.testing import assert_frame_equal

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from federated_kpi.buzz_transport import BuzzCLI, parse_protocol_content
import federated_kpi.hermes_buzz_adapter as adapter
from federated_kpi.core import Coordinator, LocalParty, PrivacyPolicy, QuerySpec
from federated_kpi.hermes_buzz_adapter import (
    coordinator_status,
    handle_coordinator_message,
    handle_site_message,
    runtime_doctor,
    start_coordinator,
)
from federated_kpi.orchestration import default_capability_profiles
from federated_kpi.synthetic import create_federation


def _write_site_config(path: Path, party: LocalParty, capability, local_min_k: int = 3) -> None:
    path.write_text(
        json.dumps(
            {
                "party_id": party.party_id,
                "db_path": str(party.db_path),
                "local_min_k": local_min_k,
                "max_releases_per_query": 1,
                "state_path": str(path.with_suffix(".state.json")),
                "capability": capability.public_manifest(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def test_live_adapter_roundtrip_matches_existing_centralized_oracle():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        parties = create_federation(
            root / "db", n_parties=2, customers_per_party=120, facts_per_party=1000, seed=17
        )
        capabilities = default_capability_profiles(parties, heterogeneous=True)
        configs = {}
        for party, capability in zip(parties, capabilities):
            config_path = root / f"{party.party_id}.json"
            _write_site_config(config_path, party, capability, local_min_k=3)
            configs[party.party_id] = config_path

        query = QuerySpec(
            "live-roundtrip", {"geo": 0, "age": 0}, measure="revenue", year_filter=2025
        )
        policy = PrivacyPolicy(5, {"geo": 1.2, "age": 1.0})
        state_path = root / "coordinator.json"
        start = start_coordinator(state_path, query, policy, [p.party_id for p in parties])
        queue = list(start["outbound"])
        transcript = list(queue)

        for _ in range(200):
            if not queue:
                break
            message = queue.pop(0)
            parsed = parse_protocol_content(message)
            assert parsed is not None
            site_result = handle_site_message(configs[parsed["party_id"]], message)
            for reply in site_result["outbound"]:
                transcript.append(reply)
                coordinator_result = handle_coordinator_message(state_path, reply)
                transcript.extend(coordinator_result["outbound"])
                queue.extend(coordinator_result["outbound"])
        else:
            raise AssertionError("live adapter did not converge")

        status = coordinator_status(state_path)
        assert status["phase"] == "done"
        assert status["result"] is not None

        selected = status["selected_levels"]
        exact = Coordinator(parties).centralized_oracle(query, selected)
        live = pd.DataFrame(status["result"])
        cols = list(query.group_levels) + [
            "n",
            "sum_value",
            "sumsq_value",
            "mean_value",
            "variance_value",
        ]
        exact = exact[cols].sort_values(list(query.group_levels)).reset_index(drop=True)
        live = live[cols].sort_values(list(query.group_levels)).reset_index(drop=True)
        assert_frame_equal(live, exact, check_dtype=False, rtol=1e-9, atol=1e-9)

        joined = "\n".join(transcript)
        assert "local_min_k" not in joined
        assert "min_count" not in joined
        assert "latency_ms" not in joined


def test_site_release_budget_persists_across_transient_adapter_invocations():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        party = create_federation(
            root / "db", n_parties=1, customers_per_party=80, facts_per_party=500, seed=19
        )[0]
        capability = default_capability_profiles([party], heterogeneous=False)[0]
        config_path = root / "P1.json"
        _write_site_config(config_path, party, capability, local_min_k=1)
        query = {
            "query_id": "budget-test",
            "group_levels": {"geo": 3, "age": 3},
            "measure": "revenue",
            "year_filter": 2025,
        }
        message = json.dumps(
            {
                "protocol": "federated-kpi/0.3",
                "type": "AGGREGATE_REQUEST",
                "party_id": "P1",
                "payload": {
                    "query_id": "budget-test",
                    "candidate_index": 0,
                    "query": query,
                    "levels": {"geo": 3, "age": 3},
                    "k": 1,
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        first = handle_site_message(config_path, message)
        second = handle_site_message(config_path, message)
        assert json.loads(first["outbound"][0])["type"] == "AGGREGATE_RELEASE"
        refusal = json.loads(second["outbound"][0])
        assert refusal["type"] == "AGGREGATE_REFUSAL"
        assert refusal["payload"]["status"] == "RELEASE_BUDGET_EXHAUSTED"


def test_buzz_cli_send_content_honors_buzz_cli_path(monkeypatch):
    with tempfile.TemporaryDirectory() as td:
        fake = Path(td) / "buzz-custom"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "import json, sys\n"
            "body = sys.stdin.read()\n"
            "print(json.dumps({'event_id':'evt1','accepted':True,'message':body,'argv':sys.argv[1:]}))\n",
            encoding="utf-8",
        )
        fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
        monkeypatch.setenv("BUZZ_RELAY_URL", "https://relay.example")
        monkeypatch.setenv("BUZZ_PRIVATE_KEY", "nsec1test")
        monkeypatch.setenv("PATH", str(Path(td)) + os.pathsep + os.environ.get("PATH", ""))
        monkeypatch.setenv("BUZZ_CLI_PATH", str(fake))

        reply = BuzzCLI("channel-1").send_content('{"protocol":"federated-kpi/0.3"}')
        assert reply["accepted"] is True
        assert reply["message"] == '{"protocol":"federated-kpi/0.3"}'
        assert reply["argv"] == [
            "messages",
            "send",
            "--channel",
            "channel-1",
            "--content",
            "-",
        ]


def test_doctor_uses_same_buzz_path_and_reports_role_prerequisites(monkeypatch):
    with tempfile.TemporaryDirectory() as td:
        fake = Path(td) / "buzz"
        fake.write_text(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'buzz 1.2.3'; exit 0; fi\nexit 0\n",
            encoding="utf-8",
        )
        fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
        hermes = Path(td) / "hermes"
        hermes.write_text(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'hermes 0.9.0'; exit 0; fi\nexit 0\n",
            encoding="utf-8",
        )
        hermes.chmod(hermes.stat().st_mode | stat.S_IXUSR)
        site_config = Path(td) / "P1.json"
        site_config.write_text("{}", encoding="utf-8")
        monkeypatch.setenv("PATH", str(Path(td)) + os.pathsep + os.environ.get("PATH", ""))
        monkeypatch.setenv("BUZZ_CLI_PATH", str(fake))
        monkeypatch.setenv("BUZZ_RELAY_URL", "https://relay.example")
        monkeypatch.setenv("BUZZ_PRIVATE_KEY", "nsec1test")
        monkeypatch.setenv("BUZZ_HOME_CHANNEL", "channel-1")
        monkeypatch.setenv("FEDERATED_KPI_ROLE", "site")
        monkeypatch.setenv("FEDERATED_KPI_SITE_CONFIG", str(site_config))

        report = runtime_doctor()
        assert report["role_valid"] is True
        assert report["hermes_executable"] == str(hermes)
        assert report["hermes_version"] == "hermes 0.9.0"
        assert report["buzz_executable"] == str(fake)
        assert report["buzz_version"] == "buzz 1.2.3"
        assert report["direct_buzz_cli_auth_ready"] is True
        assert report["site_config_exists"] is True
        assert report["project_skill_present"] is True


def test_hermes_profile_examples_disable_mentions_only_with_strict_allowlists():
    for name in ("hermes-site-config.example.yaml", "hermes-coordinator-config.example.yaml"):
        text = (ROOT / "integrations" / "hermes-buzz" / name).read_text(encoding="utf-8")
        assert "require_mention: false" in text
        assert "allow_all_users: false" in text
        assert "allowed_users:" in text
        assert "interim_assistant_messages: false" in text
        assert "tool_progress: off" in text
        assert "terminal:" in text and "cwd: /ABSOLUTE/PATH/TO/federated-kpi-aggregation" in text


def test_live_documentation_states_membership_and_sender_binding_scope():
    text = (ROOT / "integrations" / "hermes-buzz" / "README.md").read_text(encoding="utf-8")
    assert "relay or community" in text
    assert "buzz channels members --channel" in text
    assert "not cryptographically bound" in text
    assert "authenticated sender pubkey" in text


def test_concurrent_coordinator_replies_preserve_both_updates(monkeypatch):
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        parties = create_federation(
            root / "db", n_parties=2, customers_per_party=40, facts_per_party=120, seed=31
        )
        capabilities = default_capability_profiles(parties, heterogeneous=False)
        query = QuerySpec("concurrent-coordinator", {"geo": 0, "age": 0}, measure="revenue", year_filter=2025)
        state_path = root / "coordinator.json"
        start_coordinator(state_path, query, PrivacyPolicy(1), [p.party_id for p in parties])

        messages = []
        for capability in capabilities:
            messages.append(
                json.dumps(
                    {
                        "protocol": "federated-kpi/0.3",
                        "type": "CAPABILITY",
                        "party_id": capability.party_id,
                        "payload": {
                            "query_id": query.query_id,
                            "manifest": capability.public_manifest(),
                        },
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )

        original_read = adapter._read_json

        def slow_read(path):
            value = original_read(path)
            if Path(path) == state_path:
                time.sleep(0.08)
            return value

        monkeypatch.setattr(adapter, "_read_json", slow_read)
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda message: handle_coordinator_message(state_path, message), messages))

        state = json.loads(state_path.read_text(encoding="utf-8"))
        assert set(state["manifests"]) == {p.party_id for p in parties}
        assert state["phase"] == "probing"


def test_concurrent_site_release_requests_consume_budget_once(monkeypatch):
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        party = create_federation(
            root / "db", n_parties=1, customers_per_party=80, facts_per_party=500, seed=37
        )[0]
        capability = default_capability_profiles([party], heterogeneous=False)[0]
        config_path = root / "P1.json"
        _write_site_config(config_path, party, capability, local_min_k=1)
        query = {
            "query_id": "concurrent-budget",
            "group_levels": {"geo": 3, "age": 3},
            "measure": "revenue",
            "year_filter": 2025,
        }
        message = json.dumps(
            {
                "protocol": "federated-kpi/0.3",
                "type": "AGGREGATE_REQUEST",
                "party_id": "P1",
                "payload": {
                    "query_id": "concurrent-budget",
                    "candidate_index": 0,
                    "query": query,
                    "levels": {"geo": 3, "age": 3},
                    "k": 1,
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        )

        original_load = adapter._load_site_state

        def slow_load(config):
            value = original_load(config)
            time.sleep(0.08)
            return value

        monkeypatch.setattr(adapter, "_load_site_state", slow_load)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: handle_site_message(config_path, message), range(2)))

        replies = [json.loads(result["outbound"][0]) for result in results]
        assert sorted(reply["type"] for reply in replies) == ["AGGREGATE_REFUSAL", "AGGREGATE_RELEASE"]
        refusal = next(reply for reply in replies if reply["type"] == "AGGREGATE_REFUSAL")
        assert refusal["payload"]["status"] == "RELEASE_BUDGET_EXHAUSTED"
        state = json.loads(config_path.with_suffix(".state.json").read_text(encoding="utf-8"))
        assert state["release_counts"]["concurrent-budget"] == 1
