from pathlib import Path
import tempfile
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from federated_kpi.core import Coordinator, LocalParty, PrivacyPolicy, QuerySpec
from federated_kpi.synthetic import create_federation


def test_core_invariants():
    with tempfile.TemporaryDirectory() as td:
        parties = create_federation(Path(td), n_parties=3, customers_per_party=150, facts_per_party=1200, seed=7)
        coord = Coordinator(parties)
        q = QuerySpec("T", {"geo": 0, "age": 1, "product": 1})
        policy = PrivacyPolicy(k=10)
        levels, meta = coord.negotiate_min_loss(q, policy)
        assert meta["tested_candidates"] >= 1
        for p in parties:
            local = p.aggregate(q, levels).data
            assert local.empty or int(local["n"].min()) >= policy.k
        protected, _ = coord.fuse(q, levels, policy=policy)
        oracle = coord.centralized_oracle(q, levels)
        assert protected.equals(oracle)
        assert int(protected["n"].sum()) == coord.total_rows(q)


def test_transport_neutral_site_can_enforce_stricter_local_k():
    with tempfile.TemporaryDirectory() as td:
        parties = create_federation(Path(td), n_parties=2, customers_per_party=100, facts_per_party=800, seed=19)
        # k_i belongs to the site and remains effective even without an agent wrapper.
        parties[0].local_min_k = 12
        q = QuerySpec("local-k", {"geo": 1, "age": 2})
        coord = Coordinator(parties)
        levels, _ = coord.negotiate_min_loss(q, PrivacyPolicy(k=5))
        r = parties[0].privacy_probe(q, levels, 5)
        assert r.safe
        local = parties[0].aggregate(q, levels).data
        assert local.empty or int(local["n"].min()) >= 12


def test_committed_release_rechecks_current_local_threshold():
    with tempfile.TemporaryDirectory() as td:
        party = create_federation(Path(td), n_parties=1, customers_per_party=80, facts_per_party=500, seed=9)[0]
        q = QuerySpec("recheck", {"geo": 0, "age": 1})
        policy = PrivacyPolicy(k=2)
        coord = Coordinator([party])
        levels, _ = coord.negotiate_min_loss(q, policy)
        # Simulate a local policy change after negotiation but before release.
        party.local_min_k = 10_000
        try:
            coord.fuse(q, levels, policy=policy, suppress_unsafe=False)
        except PermissionError:
            pass
        else:
            raise AssertionError("release should fail closed after a stricter local policy change")


def test_empty_site_is_safe_and_releases_empty_result():
    with tempfile.TemporaryDirectory() as td:
        party = create_federation(Path(td), n_parties=1, customers_per_party=80, facts_per_party=500, seed=13)[0]
        # The synthetic warehouse contains no rows for this year.
        q = QuerySpec("empty-site", {"geo": 0, "age": 1}, year_filter=1900)
        levels = dict(q.group_levels)

        probe = party.privacy_probe(q, levels, k=10)
        assert probe.safe
        assert probe.groups == 0
        assert probe.min_count is None

        released = party.guarded_aggregate(q, levels, k=10).data
        assert released.empty


if __name__ == "__main__":
    test_core_invariants()
    test_transport_neutral_site_can_enforce_stricter_local_k()
    test_committed_release_rechecks_current_local_threshold()
    test_empty_site_is_safe_and_releases_empty_result()
    print("All core tests passed")
