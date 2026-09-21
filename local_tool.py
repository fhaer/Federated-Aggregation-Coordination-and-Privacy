"""Deterministic site-local commands for privacy probing and aggregate release.

Example:
  python local_tool.py probe --party P1 --db results/db_seed_11/P1.sqlite \
      --query '{"query_id":"Q","group_levels":{"geo":0,"age":1}}' \
      --levels '{"geo":1,"age":2}' --k 10
"""
from pathlib import Path
import argparse
import json
import os
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from federated_kpi.core import LocalParty, QuerySpec


def load_query(s: str) -> QuerySpec:
    x = json.loads(s)
    q = QuerySpec(
        query_id=x.get("query_id", "ad_hoc"),
        group_levels=x["group_levels"],
        measure=x.get("measure", "revenue"),
        year_filter=x.get("year_filter", 2025),
    )
    q.validate()
    return q


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for cmd in ("probe", "aggregate"):
        p = sub.add_parser(cmd)
        p.add_argument("--party", required=True)
        p.add_argument("--db", required=True)
        p.add_argument("--query", required=True)
        p.add_argument("--levels", required=True)
        p.add_argument("--k", type=int, required=True)
    args = ap.parse_args()

    # The local threshold is configured at the site. The requested --k can only increase it.
    local_min_k = int(os.environ.get("FEDERATED_KPI_LOCAL_MIN_K", "1"))
    party = LocalParty(args.party, Path(args.db), local_min_k=local_min_k)
    query = load_query(args.query)
    levels = json.loads(args.levels)

    if args.cmd == "probe":
        r = party.privacy_probe(query, levels, args.k)
        # Probe replies contain only site identity and feasibility status.
        print(json.dumps({"party_id": args.party, "status": "SAFE" if r.safe else "UNSAFE"}))
    else:
        try:
            r = party.guarded_aggregate(query, levels, args.k)
        except PermissionError as exc:
            raise SystemExit(f"Refusing release: {exc}") from exc
        print(r.data.drop(columns=["party_id"]).to_json(orient="records"))


if __name__ == "__main__":
    main()
