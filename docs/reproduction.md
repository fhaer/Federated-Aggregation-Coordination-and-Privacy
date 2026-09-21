# Reproduction

This document describes the experiment configuration and reproduction path. The reported measurements use the deterministic harness.

## Experiment Configuration

The privacy and analytical-granularity experiment uses four sites, 450 customers and 5,000 fact rows per site, seeds `11`, `22`, and `33`, thresholds `3`, `5`, `10`, and `20`, and six query shapes. The hierarchy-loss weights are `geo=1.2`, `age=1.0`, `product=0.8`, and `time=1.0`. The raw output contains 144 strategy/query/seed rows.

The capability-change experiment uses four capability profiles. After a stored snapshot is created, one site's geography capability changes to country level. `stored_snapshot` uses the stale snapshot. `current_site_agent_state` discovers current capabilities before candidate evaluation. The reported rows use seeds `11`, `22`, and `33` with `k=10`.

The optional Hermes and Buzz runtime is not part of the measured experiment path.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On Windows, activate the environment with `.venv\\Scripts\\activate`.

## Validation

```bash
python -m pytest -q
python scripts/verify_reported_results.py
```

## Run the Experiments

Privacy and analytical granularity:

```bash
python run_experiment.py
```

Coordination under capability changes:

```bash
python run_orchestration_experiment.py
```

Regenerate the paper-facing tables and verify them:

```bash
python scripts/export_paper_tables.py
python scripts/verify_reported_results.py
```

Generated SQLite databases are ignored by Git. Timing fields are machine-dependent diagnostics and are not replication targets. Committed outputs are described in [`../results/README.md`](../results/README.md).
