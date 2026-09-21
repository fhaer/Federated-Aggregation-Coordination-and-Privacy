# Method

This document summarizes the deterministic coordination method. The optional Hermes and Buzz runtime is documented under `integrations/hermes-buzz/`.

## Implementation

- `src/federated_kpi/core.py`: query model, hierarchy levels, privacy probes, guarded release, and KPI aggregation.
- `src/federated_kpi/orchestration.py`: deterministic site-agent and coordinator logic.
- `local_tool.py`: site-local `probe` and `aggregate` commands.

## OLAP Hierarchies

The synthetic warehouse uses these hierarchy levels:

```text
geography: city -> state/region -> country -> all
age:       exact age -> decade -> age group -> all
product:   product -> category -> all
time:      month -> quarter -> year -> all
```

The internal column name for `age group` is `age_band`.

## Local Minimum-Cohort Rule

For federation threshold `k`, site `i` may use a stricter local threshold `k_i`:

```text
kappa_i = max(k, k_i)
```

A candidate is `SAFE` when every non-empty local output group contains at least `kappa_i` privacy units. A site with no non-empty groups is also safe and contributes an empty result. The coordinator receives the status, not `k_i` or the local minimum count.

The experiments count fact rows. A subject-oriented deployment can count distinct subject identifiers instead.

## Candidate Ordering

For dimension `d`, let `r_d` be the requested level, `l_d` the candidate level, `m_d` the coarsest level, and `w_d` the dimension weight. The information-loss contribution is:

```text
w_d * (l_d - r_d) / (m_d - r_d)
```

Candidate loss is the weighted mean over grouped dimensions. Candidates are ordered by loss, total level increase, and level vector. Current site capabilities can remove unsupported candidates before privacy probing. With a stale capability snapshot, a site may still return `UNSUPPORTED`.

## Coordination Sequence

The deterministic harness records:

```text
CAPABILITY_REQUEST
CAPABILITY
PLAN_PROPOSAL
PLAN_REPLY        # SAFE, UNSAFE, or UNSUPPORTED
PLAN_COMMIT
AGGREGATE_REQUEST
AGGREGATE_RELEASE
```

After a common level is selected, each site repeats the local threshold check before release.

## Released Statistics

For each selected group, a site releases:

```text
n_i = COUNT(*)
s_i = SUM(x)
q_i = SUM(x*x)
```

The coordinator calculates:

```text
N        = sum_i n_i
S        = sum_i s_i
Q        = sum_i q_i
mean     = S / N
variance = Q / N - (S / N)^2
```

Small negative variance values caused by floating-point error are clipped to zero.
