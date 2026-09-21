from __future__ import annotations

"""Buzz CLI transport for the federated KPI protocol.

The module sends canonical JSON messages through the external Buzz CLI.
``BUZZ_RELAY_URL`` and ``BUZZ_PRIVATE_KEY`` provide relay access and signing.
"""

from dataclasses import dataclass
import json
import os
import subprocess
from typing import Any, Dict, List, Mapping

from .core import protocol_message
from .orchestration import OrchestrationEvent


@dataclass
class BuzzCLI:
    channel_id: str
    executable: str | None = None

    def _resolve_executable(self) -> str:
        """Resolve the Buzz CLI consistently with Hermes' BUZZ_CLI_PATH setting."""
        return self.executable or os.environ.get("BUZZ_CLI_PATH") or "buzz"

    def _run(self, args: List[str], stdin: str | None = None) -> Any:
        if not os.environ.get("BUZZ_RELAY_URL"):
            raise RuntimeError("BUZZ_RELAY_URL is not set")

        # Direct Buzz CLI calls require the signing key in the environment.
        if not os.environ.get("BUZZ_PRIVATE_KEY"):
            raise RuntimeError(
                "BUZZ_PRIVATE_KEY is not set; the direct Buzz CLI transport requires it"
            )

        proc = subprocess.run(
            [self._resolve_executable(), *args],
            input=stdin,
            text=True,
            capture_output=True,
            check=False,
        )
        if proc.returncode != 0:
            detail = proc.stderr.strip() or proc.stdout.strip()
            raise RuntimeError(f"Buzz CLI failed ({proc.returncode}): {detail}")
        try:
            return json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Buzz CLI returned non-JSON output") from exc

    def send_content(self, body: str) -> Dict[str, Any]:
        """Send one message body through Buzz stdin."""
        result = self._run(
            ["messages", "send", "--channel", self.channel_id, "--content", "-"],
            stdin=body,
        )
        if not isinstance(result, dict):
            raise RuntimeError("Buzz messages send returned an unexpected JSON value")
        return result

    # Compatibility with the original prototype helper name.
    def _send_content(self, body: str) -> Dict[str, Any]:
        return self.send_content(body)

    def send_protocol(self, kind: str, party_id: str, payload: Mapping[str, object]) -> Dict[str, Any]:
        """Send a compact federated KPI protocol envelope."""
        return self.send_content(protocol_message(kind, party_id, payload))

    def send_event(self, event: OrchestrationEvent) -> Dict[str, Any]:
        """Send one canonical orchestration event."""
        return self.send_content(event.canonical_json())

    def get_messages(self, limit: int = 50) -> List[Dict[str, Any]]:
        result = self._run(["messages", "get", "--channel", self.channel_id, "--limit", str(limit)])
        if isinstance(result, list):
            return result
        return result.get("messages", []) if isinstance(result, dict) else []


def parse_protocol_content(content: str) -> Dict[str, Any] | None:
    """Parse a compact protocol message or orchestration event."""
    try:
        obj = json.loads(content)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(obj, dict):
        return None
    if obj.get("protocol") not in {"federated-kpi/0.1", "federated-kpi/0.2", "federated-kpi/0.3"}:
        return None
    if not isinstance(obj.get("payload"), dict):
        return None
    compact = isinstance(obj.get("type"), str) and isinstance(obj.get("party_id"), str)
    audit = (
        isinstance(obj.get("kind"), str)
        and isinstance(obj.get("actor"), str)
        and isinstance(obj.get("query_id"), str)
        and isinstance(obj.get("seq"), int)
    )
    return obj if compact or audit else None
