from __future__ import annotations

"""Optional Hermes and Buzz runtime adapter.

The reported experiments use the deterministic harness in
:mod:`federated_kpi.orchestration`. The module maps the coordinator and
site-agent roles to Hermes and sends canonical ``federated-kpi/0.3`` messages
through the Buzz CLI. Privacy checks, release checks, candidate ordering, and
KPI calculations remain in deterministic code.

Hermes authorizes Buzz senders before the project skill runs. The adapter
validates protocol state and ``party_id`` but does not receive authenticated
sender metadata.
"""

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import argparse
import json
import os
import shutil
import subprocess
import tempfile
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence

import numpy as np
import pandas as pd

from .buzz_transport import BuzzCLI, parse_protocol_content
from .core import LocalParty, PrivacyPolicy, QuerySpec, protocol_message
from .orchestration import AgentOrchestrator, LocalAnalyticsAgent, SiteCapability

PROTOCOL = "federated-kpi/0.3"


# ---------------------------------------------------------------------------
# Generic JSON/state helpers
# ---------------------------------------------------------------------------

def _read_json(path: Path) -> Dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return obj


def _write_json_atomic(path: Path, obj: Mapping[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2, sort_keys=True)
            f.write("\n")
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


@contextmanager
def _state_file_lock(state_path: Path):
    """Serialize one persisted state transition across Hermes tool processes."""

    try:
        import fcntl
    except ImportError as exc:  # pragma: no cover - live extension targets POSIX hosts
        raise RuntimeError("Live runtime state locking requires POSIX fcntl support") from exc

    state_path = Path(state_path)
    lock_path = Path(f"{state_path}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _read_text(path_or_dash: str) -> str:
    if path_or_dash == "-":
        import sys

        return sys.stdin.read()
    return Path(path_or_dash).read_text(encoding="utf-8")


def _compact_envelope(content: str) -> Dict[str, Any]:
    obj = parse_protocol_content(content)
    if obj is None or not isinstance(obj.get("type"), str) or not isinstance(obj.get("party_id"), str):
        raise ValueError("Expected a compact federated KPI protocol envelope")
    if obj.get("protocol") != PROTOCOL:
        raise ValueError(f"Unsupported protocol version: {obj.get('protocol')!r}")
    return obj


def _query_from_dict(obj: Mapping[str, Any]) -> QuerySpec:
    q = QuerySpec(
        query_id=str(obj.get("query_id", "ad_hoc")),
        group_levels={str(k): int(v) for k, v in dict(obj["group_levels"]).items()},
        measure=str(obj.get("measure", "revenue")),
        year_filter=(None if obj.get("year_filter", 2025) is None else int(obj.get("year_filter", 2025))),
    )
    q.validate()
    return q


def _query_to_dict(query: QuerySpec) -> Dict[str, Any]:
    return {
        "query_id": query.query_id,
        "group_levels": {str(k): int(v) for k, v in query.group_levels.items()},
        "measure": query.measure,
        "year_filter": query.year_filter,
    }


def _policy_to_dict(policy: PrivacyPolicy) -> Dict[str, Any]:
    return {"k": int(policy.k), "weights": {str(k): float(v) for k, v in policy.weights.items()}}


def _policy_from_dict(obj: Mapping[str, Any]) -> PrivacyPolicy:
    k = int(obj["k"])
    if k < 1:
        raise ValueError("k must be >= 1")
    return PrivacyPolicy(k=k, weights={str(x): float(y) for x, y in dict(obj.get("weights", {})).items()})


def _json_rows(frame: pd.DataFrame) -> List[Dict[str, Any]]:
    # pandas' JSON conversion normalizes numpy scalars into ordinary JSON types.
    return json.loads(frame.to_json(orient="records"))


def _safe_query_id(payload: Mapping[str, Any]) -> str:
    query_id = payload.get("query_id")
    if not isinstance(query_id, str) or not query_id:
        raise ValueError("Protocol payload is missing query_id")
    return query_id


# ---------------------------------------------------------------------------
# Site-side deterministic adapter
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SiteRuntimeConfig:
    party_id: str
    db_path: Path
    capability: SiteCapability
    local_min_k: int = 1
    max_releases_per_query: int = 1
    state_path: Path | None = None

    @classmethod
    def load(cls, path: Path) -> "SiteRuntimeConfig":
        path = Path(path).resolve()
        raw = _read_json(path)
        party_id = str(raw["party_id"])
        db_path = Path(str(raw["db_path"]))
        if not db_path.is_absolute():
            db_path = (path.parent / db_path).resolve()
        cap_raw = dict(raw.get("capability", {}))
        cap = SiteCapability(
            party_id=party_id,
            min_levels={str(k): int(v) for k, v in dict(cap_raw.get("min_levels", {})).items()},
            supported_measures=tuple(str(x) for x in cap_raw.get("supported_measures", ["revenue", "quantity"])),
            local_vocabulary={
                str(k): tuple(str(x) for x in v)
                for k, v in dict(cap_raw.get("local_vocabulary", {})).items()
            },
            capability_version=str(cap_raw.get("capability_version", "1")),
        )
        state_raw = raw.get("state_path")
        if state_raw is None:
            state_path = path.with_name(f".{party_id.lower()}-runtime-state.json")
        else:
            state_path = Path(str(state_raw))
            if not state_path.is_absolute():
                state_path = (path.parent / state_path).resolve()
        local_min_k = int(raw.get("local_min_k", 1))
        max_releases = int(raw.get("max_releases_per_query", 1))
        if local_min_k < 1 or max_releases < 1:
            raise ValueError("local_min_k and max_releases_per_query must be >= 1")
        return cls(
            party_id=party_id,
            db_path=db_path,
            capability=cap,
            local_min_k=local_min_k,
            max_releases_per_query=max_releases,
            state_path=state_path,
        )

    def build_agent(self) -> LocalAnalyticsAgent:
        party = LocalParty(self.party_id, self.db_path, local_min_k=self.local_min_k)
        return LocalAnalyticsAgent(
            party,
            self.capability,
            local_min_k=self.local_min_k,
            max_releases_per_query=self.max_releases_per_query,
        )


def _load_site_state(config: SiteRuntimeConfig) -> Dict[str, Any]:
    path = config.state_path
    if path is None or not path.exists():
        return {"protocol": PROTOCOL, "party_id": config.party_id, "release_counts": {}}
    state = _read_json(path)
    if state.get("party_id") != config.party_id:
        raise ValueError("Site runtime state belongs to another party")
    state.setdefault("release_counts", {})
    return state


def _save_site_state(config: SiteRuntimeConfig, state: Mapping[str, Any]) -> None:
    if config.state_path is not None:
        _write_json_atomic(config.state_path, state)


def handle_site_message(config_path: Path, content: str) -> Dict[str, Any]:
    """Process one coordinator message at a site and return at most one reply.

    Site configuration is reloaded for every message. The private threshold
    remains local.
    """

    config = SiteRuntimeConfig.load(config_path)
    msg = _compact_envelope(content)
    if msg["party_id"] != config.party_id:
        return {"ignored": True, "reason": "message is for another site", "outbound": []}

    kind = msg["type"]
    payload = dict(msg["payload"])
    query_id = _safe_query_id(payload)
    agent = config.build_agent()

    if kind == "CAPABILITY_REQUEST":
        reply = protocol_message(
            "CAPABILITY",
            config.party_id,
            {"query_id": query_id, "manifest": agent.discover_capabilities()},
        )
        return {"ignored": False, "outbound": [reply]}

    if kind == "PLAN_PROPOSAL":
        query = _query_from_dict(dict(payload["query"]))
        if query.query_id != query_id:
            raise ValueError("query_id differs between envelope and query")
        levels = {str(k): int(v) for k, v in dict(payload["levels"]).items()}
        requested_k = int(payload["k"])
        candidate_index = int(payload["candidate_index"])
        r = agent.probe(query, levels, requested_k)
        reply = protocol_message(
            "PLAN_REPLY",
            config.party_id,
            {
                "query_id": query_id,
                "candidate_index": candidate_index,
                "status": r.status,
                "capability_version": r.capability_version,
            },
        )
        return {"ignored": False, "outbound": [reply]}

    if kind == "AGGREGATE_REQUEST":
        query = _query_from_dict(dict(payload["query"]))
        if query.query_id != query_id:
            raise ValueError("query_id differs between envelope and query")
        levels = {str(k): int(v) for k, v in dict(payload["levels"]).items()}
        requested_k = int(payload["k"])
        candidate_index = int(payload["candidate_index"])

        # Persist the release budget across separate tool processes.
        assert config.state_path is not None
        with _state_file_lock(config.state_path):
            state = _load_site_state(config)
            release_counts = dict(state.get("release_counts", {}))
            count = int(release_counts.get(query_id, 0))
            if count >= config.max_releases_per_query:
                reply = protocol_message(
                    "AGGREGATE_REFUSAL",
                    config.party_id,
                    {
                        "query_id": query_id,
                        "candidate_index": candidate_index,
                        "status": "RELEASE_BUDGET_EXHAUSTED",
                        "capability_version": config.capability.capability_version,
                    },
                )
                return {"ignored": False, "outbound": [reply]}

            if not agent.supports(query, levels):
                reply = protocol_message(
                    "AGGREGATE_REFUSAL",
                    config.party_id,
                    {
                        "query_id": query_id,
                        "candidate_index": candidate_index,
                        "status": "UNSUPPORTED",
                        "capability_version": config.capability.capability_version,
                    },
                )
                return {"ignored": False, "outbound": [reply]}

            try:
                frame = agent.release(query, levels, requested_k)
            except PermissionError:
                # Do not forward local counts, thresholds, or diagnostic details.
                reply = protocol_message(
                    "AGGREGATE_REFUSAL",
                    config.party_id,
                    {
                        "query_id": query_id,
                        "candidate_index": candidate_index,
                        "status": "UNSAFE",
                        "capability_version": config.capability.capability_version,
                    },
                )
                return {"ignored": False, "outbound": [reply]}

            release_counts[query_id] = count + 1
            state["release_counts"] = release_counts
            _save_site_state(config, state)

            released = frame.drop(columns=["party_id"], errors="ignore")
            reply = protocol_message(
                "AGGREGATE_RELEASE",
                config.party_id,
                {
                    "query_id": query_id,
                    "candidate_index": candidate_index,
                    "capability_version": config.capability.capability_version,
                    "rows": _json_rows(released),
                },
            )
            return {"ignored": False, "outbound": [reply]}

    return {"ignored": True, "reason": f"unsupported site message type {kind}", "outbound": []}


# ---------------------------------------------------------------------------
# Coordinator-side deterministic state machine
# ---------------------------------------------------------------------------

def _new_coordinator_state(
    query: QuerySpec,
    policy: PrivacyPolicy,
    parties: Sequence[str],
) -> Dict[str, Any]:
    party_ids = [str(x) for x in parties]
    if not party_ids or len(set(party_ids)) != len(party_ids):
        raise ValueError("parties must be a non-empty list of unique IDs")
    return {
        "protocol": PROTOCOL,
        "role": "coordinator",
        "query": _query_to_dict(query),
        "policy": _policy_to_dict(policy),
        "parties": party_ids,
        "phase": "capabilities",
        "manifests": {},
        "candidates": [],
        "candidate_index": None,
        "plan_replies": {},
        "selected_levels": None,
        "aggregate_releases": {},
        "result": None,
        "error": None,
    }


def _coordinator_request(kind: str, party_id: str, query: QuerySpec, extra: Mapping[str, Any] | None = None) -> str:
    payload: Dict[str, Any] = {"query_id": query.query_id}
    if extra:
        payload.update(dict(extra))
    return protocol_message(kind, party_id, payload)


def start_coordinator(
    state_path: Path,
    query: QuerySpec,
    policy: PrivacyPolicy,
    parties: Sequence[str],
    *,
    force: bool = False,
) -> Dict[str, Any]:
    state_path = Path(state_path)
    with _state_file_lock(state_path):
        if state_path.exists() and not force:
            raise FileExistsError(f"Coordinator state already exists: {state_path}")
        state = _new_coordinator_state(query, policy, parties)
        _write_json_atomic(state_path, state)
    outbound = [_coordinator_request("CAPABILITY_REQUEST", pid, query) for pid in state["parties"]]
    return {"phase": state["phase"], "complete": False, "outbound": outbound}


def _candidate_messages(state: MutableMapping[str, Any], index: int) -> List[str]:
    query = _query_from_dict(dict(state["query"]))
    policy = _policy_from_dict(dict(state["policy"]))
    candidates = list(state["candidates"])
    if index < 0 or index >= len(candidates):
        raise IndexError("candidate index out of range")
    candidate = {str(k): int(v) for k, v in dict(candidates[index]).items()}
    state["phase"] = "probing"
    state["candidate_index"] = index
    state["plan_replies"] = {}
    return [
        _coordinator_request(
            "PLAN_PROPOSAL",
            pid,
            query,
            {
                "candidate_index": index,
                "query": _query_to_dict(query),
                "levels": candidate,
                "k": policy.k,
            },
        )
        for pid in state["parties"]
    ]


def _aggregate_messages(state: MutableMapping[str, Any]) -> List[str]:
    query = _query_from_dict(dict(state["query"]))
    policy = _policy_from_dict(dict(state["policy"]))
    index = int(state["candidate_index"])
    levels = {str(k): int(v) for k, v in dict(state["selected_levels"]).items()}
    state["phase"] = "release"
    state["aggregate_releases"] = {}
    return [
        _coordinator_request(
            "AGGREGATE_REQUEST",
            pid,
            query,
            {
                "candidate_index": index,
                "query": _query_to_dict(query),
                "levels": levels,
                "k": policy.k,
            },
        )
        for pid in state["parties"]
    ]


def _fuse_remote_rows(query: QuerySpec, releases: Mapping[str, Sequence[Mapping[str, Any]]]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for rows in releases.values():
        frame = pd.DataFrame(list(rows))
        if not frame.empty:
            frames.append(frame)
    group_cols = list(query.group_levels.keys())
    if not frames:
        return pd.DataFrame(columns=group_cols + ["n", "sum_value", "sumsq_value", "mean_value", "variance_value"])
    combined = pd.concat(frames, ignore_index=True)
    required = set(group_cols + ["n", "sum_value", "sumsq_value"])
    missing = required.difference(combined.columns)
    if missing:
        raise ValueError(f"Aggregate release is missing fields: {sorted(missing)}")
    fused = combined.groupby(group_cols, dropna=False, as_index=False)[["n", "sum_value", "sumsq_value"]].sum()
    fused["mean_value"] = fused["sum_value"] / fused["n"]
    fused["variance_value"] = np.maximum(
        0.0, fused["sumsq_value"] / fused["n"] - fused["mean_value"] ** 2
    )
    return fused


def handle_coordinator_message(state_path: Path, content: str) -> Dict[str, Any]:
    """Advance one persisted coordinator session using one site reply."""

    state_path = Path(state_path)
    with _state_file_lock(state_path):
        return _handle_coordinator_message_locked(state_path, content)


def _handle_coordinator_message_locked(state_path: Path, content: str) -> Dict[str, Any]:
    state = _read_json(state_path)
    if state.get("role") != "coordinator" or state.get("protocol") != PROTOCOL:
        raise ValueError("Not a federated KPI coordinator state file")
    if state.get("phase") in {"done", "failed"}:
        return {
            "phase": state["phase"],
            "complete": state["phase"] == "done",
            "outbound": [],
            "result": state.get("result"),
            "error": state.get("error"),
        }

    msg = _compact_envelope(content)
    party_id = msg["party_id"]
    if party_id not in state["parties"]:
        return {"phase": state["phase"], "complete": False, "ignored": True, "outbound": []}
    payload = dict(msg["payload"])
    query = _query_from_dict(dict(state["query"]))
    if _safe_query_id(payload) != query.query_id:
        return {"phase": state["phase"], "complete": False, "ignored": True, "outbound": []}

    outbound: List[str] = []
    kind = msg["type"]

    if state["phase"] == "capabilities" and kind == "CAPABILITY":
        manifest = dict(payload["manifest"])
        if str(manifest.get("party_id")) != party_id:
            raise ValueError("Capability manifest party_id does not match envelope")
        state["manifests"][party_id] = manifest
        if all(pid in state["manifests"] for pid in state["parties"]):
            policy = _policy_from_dict(dict(state["policy"]))
            candidates, _ = AgentOrchestrator.capability_compatible_candidates(
                query, policy, state["manifests"]
            )
            state["candidates"] = [dict(c) for c in candidates]
            if not candidates:
                state["phase"] = "failed"
                state["error"] = "No mutually supported candidate exists"
            else:
                outbound = _candidate_messages(state, 0)

    elif state["phase"] == "probing" and kind == "PLAN_REPLY":
        index = int(payload["candidate_index"])
        if index != int(state["candidate_index"]):
            return {"phase": state["phase"], "complete": False, "ignored": True, "outbound": []}
        status = str(payload["status"])
        if status not in {"SAFE", "UNSAFE", "UNSUPPORTED"}:
            raise ValueError(f"Invalid PLAN_REPLY status: {status}")
        state["plan_replies"][party_id] = status
        if all(pid in state["plan_replies"] for pid in state["parties"]):
            if all(state["plan_replies"][pid] == "SAFE" for pid in state["parties"]):
                state["selected_levels"] = dict(state["candidates"][index])
                outbound = _aggregate_messages(state)
            else:
                next_index = index + 1
                if next_index >= len(state["candidates"]):
                    state["phase"] = "failed"
                    state["error"] = "No mutually supported k-safe candidate found"
                else:
                    outbound = _candidate_messages(state, next_index)

    elif state["phase"] == "release" and kind == "AGGREGATE_RELEASE":
        index = int(payload["candidate_index"])
        if index != int(state["candidate_index"]):
            return {"phase": state["phase"], "complete": False, "ignored": True, "outbound": []}
        rows = payload.get("rows")
        if not isinstance(rows, list):
            raise ValueError("AGGREGATE_RELEASE rows must be a list")
        state["aggregate_releases"][party_id] = rows
        if all(pid in state["aggregate_releases"] for pid in state["parties"]):
            fused = _fuse_remote_rows(query, state["aggregate_releases"])
            state["result"] = _json_rows(fused)
            state["phase"] = "done"

    elif state["phase"] == "release" and kind == "AGGREGATE_REFUSAL":
        state["phase"] = "failed"
        state["error"] = f"{party_id} refused aggregate release: {payload.get('status', 'REFUSED')}"

    else:
        return {"phase": state["phase"], "complete": False, "ignored": True, "outbound": []}

    _write_json_atomic(state_path, state)
    return {
        "phase": state["phase"],
        "complete": state["phase"] == "done",
        "ignored": False,
        "outbound": outbound,
        "selected_levels": state.get("selected_levels"),
        "result": state.get("result"),
        "error": state.get("error"),
    }


def coordinator_status(state_path: Path) -> Dict[str, Any]:
    state = _read_json(Path(state_path))
    return {
        "protocol": state.get("protocol"),
        "phase": state.get("phase"),
        "query_id": dict(state.get("query", {})).get("query_id"),
        "parties": state.get("parties", []),
        "candidate_index": state.get("candidate_index"),
        "selected_levels": state.get("selected_levels"),
        "result": state.get("result"),
        "error": state.get("error"),
    }


# ---------------------------------------------------------------------------
# Live Buzz delivery and CLI
# ---------------------------------------------------------------------------

def _resolve_channel(explicit: str | None) -> str:
    channel = explicit or os.environ.get("BUZZ_HOME_CHANNEL")
    if not channel:
        raise RuntimeError("Set --channel or BUZZ_HOME_CHANNEL before using --send")
    return channel


def _send_outbound(outbound: Sequence[str], channel: str | None) -> List[Dict[str, Any]]:
    if not outbound:
        return []
    buzz = BuzzCLI(_resolve_channel(channel))
    return [buzz.send_content(content) for content in outbound]


def _tool_version(executable: str | None) -> str | None:
    """Return a short tool version string without exposing environment secrets."""
    if not executable:
        return None
    try:
        proc = subprocess.run(
            [executable, "--version"],
            text=True,
            capture_output=True,
            check=False,
            timeout=3,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    text = (proc.stdout or proc.stderr).strip()
    if proc.returncode != 0 or not text:
        return None
    return text.splitlines()[0][:200]


def runtime_doctor() -> Dict[str, Any]:
    """Report runtime prerequisites without printing secret values."""
    role = os.environ.get("FEDERATED_KPI_ROLE", "")
    buzz_setting = os.environ.get("BUZZ_CLI_PATH")
    hermes_executable = shutil.which("hermes")
    buzz_executable = shutil.which(buzz_setting or "buzz")
    site_config = os.environ.get("FEDERATED_KPI_SITE_CONFIG")
    coordinator_state = os.environ.get("FEDERATED_KPI_COORDINATOR_STATE")
    skill_path = Path(__file__).resolve().parents[2] / ".hermes" / "skills" / "federated-kpi-aggregation" / "SKILL.md"

    return {
        "role": role or None,
        "role_valid": role in {"site", "coordinator"},
        "hermes_executable": hermes_executable,
        "hermes_version": _tool_version(hermes_executable),
        "buzz_executable": buzz_executable,
        "buzz_version": _tool_version(buzz_executable),
        "buzz_cli_path_setting": buzz_setting,
        "buzz_relay_configured": bool(os.environ.get("BUZZ_RELAY_URL")),
        "buzz_private_key_configured": bool(os.environ.get("BUZZ_PRIVATE_KEY")),
        "buzz_credentials_file_configured": bool(os.environ.get("BUZZ_CREDENTIALS_FILE")),
        "direct_buzz_cli_auth_ready": bool(os.environ.get("BUZZ_PRIVATE_KEY")),
        "buzz_channel_configured": bool(os.environ.get("BUZZ_HOME_CHANNEL")),
        "site_config": site_config,
        "site_config_exists": bool(site_config and Path(site_config).expanduser().exists()),
        "coordinator_state": coordinator_state,
        "coordinator_state_parent_exists": bool(
            coordinator_state and Path(coordinator_state).expanduser().parent.exists()
        ),
        "project_skill_present": skill_path.exists(),
    }


def _load_query_file(path: str) -> QuerySpec:
    return _query_from_dict(_read_json(Path(path)))


def _parse_weights(value: str | None) -> Dict[str, float]:
    if not value:
        return {}
    obj = json.loads(value)
    if not isinstance(obj, dict):
        raise ValueError("--weights must be a JSON object")
    return {str(k): float(v) for k, v in obj.items()}


def _print_result(result: Mapping[str, Any], sent: Sequence[Mapping[str, Any]] | None = None) -> None:
    out = dict(result)
    if sent is not None:
        out["sent"] = list(sent)
    # Emit parsed envelopes for human/Hermes inspection while preserving the
    # exact canonical strings in outbound_canonical.
    canonical = list(out.get("outbound", []))
    out["outbound_canonical"] = canonical
    out["outbound"] = [json.loads(x) for x in canonical]
    print(json.dumps(out, indent=2, sort_keys=True))


def main(argv: Sequence[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Optional Hermes and Buzz runtime adapter for federated KPI aggregation")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("doctor", help="Check the optional Hermes and Buzz runtime prerequisites")

    site = sub.add_parser("site-handle", help="Handle one coordinator envelope at a site")
    site.add_argument("--config", default=os.environ.get("FEDERATED_KPI_SITE_CONFIG"))
    site.add_argument("--message-file", required=True, help="Path containing one JSON envelope, or - for stdin")
    site.add_argument("--send", action="store_true", help="Send generated replies with the Buzz CLI")
    site.add_argument("--channel")

    start = sub.add_parser("coordinator-start", help="Start a persisted live coordinator session")
    start.add_argument("--state", default=os.environ.get("FEDERATED_KPI_COORDINATOR_STATE"))
    start.add_argument("--query", required=True, help="Query JSON file")
    start.add_argument("--k", required=True, type=int)
    start.add_argument("--weights", help='JSON object, e.g. {"geo":1.2,"age":1.0}')
    start.add_argument("--parties", nargs="+", required=True)
    start.add_argument("--force", action="store_true")
    start.add_argument("--send", action="store_true")
    start.add_argument("--channel")

    coord = sub.add_parser("coordinator-handle", help="Advance a live coordinator session by one site reply")
    coord.add_argument("--state", default=os.environ.get("FEDERATED_KPI_COORDINATOR_STATE"))
    coord.add_argument("--message-file", required=True, help="Path containing one JSON envelope, or - for stdin")
    coord.add_argument("--send", action="store_true")
    coord.add_argument("--channel")

    status = sub.add_parser("coordinator-status", help="Show a persisted coordinator session")
    status.add_argument("--state", default=os.environ.get("FEDERATED_KPI_COORDINATOR_STATE"))

    args = ap.parse_args(argv)

    if args.cmd == "doctor":
        print(json.dumps(runtime_doctor(), indent=2, sort_keys=True))
        return

    if args.cmd == "site-handle":
        if not args.config:
            raise SystemExit("site-handle requires --config or FEDERATED_KPI_SITE_CONFIG")
        result = handle_site_message(Path(args.config), _read_text(args.message_file))
        sent = _send_outbound(result["outbound"], args.channel) if args.send else None
        _print_result(result, sent)
        return

    if args.cmd == "coordinator-start":
        if not args.state:
            raise SystemExit("coordinator-start requires --state or FEDERATED_KPI_COORDINATOR_STATE")
        query = _load_query_file(args.query)
        policy = PrivacyPolicy(k=args.k, weights=_parse_weights(args.weights))
        result = start_coordinator(Path(args.state), query, policy, args.parties, force=args.force)
        sent = _send_outbound(result["outbound"], args.channel) if args.send else None
        _print_result(result, sent)
        return

    if args.cmd == "coordinator-handle":
        if not args.state:
            raise SystemExit("coordinator-handle requires --state or FEDERATED_KPI_COORDINATOR_STATE")
        result = handle_coordinator_message(Path(args.state), _read_text(args.message_file))
        sent = _send_outbound(result["outbound"], args.channel) if args.send else None
        _print_result(result, sent)
        return

    if args.cmd == "coordinator-status":
        if not args.state:
            raise SystemExit("coordinator-status requires --state or FEDERATED_KPI_COORDINATOR_STATE")
        print(json.dumps(coordinator_status(Path(args.state)), indent=2, sort_keys=True))
        return

    raise AssertionError(args.cmd)


if __name__ == "__main__":
    main()
