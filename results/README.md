# Results

Committed outputs used to verify the values reported in the paper.

## Privacy and Utility

Generated with:

```bash
python run_experiment.py
```

- `experiment_config.json`: experiment configuration.
- `experiment_results.csv`: 144 raw strategy/query/seed rows.
- `summary.csv`: mean metrics by strategy and threshold.
- `summary.md`: Markdown rendering of the summary.
- `coverage_vs_k.png`: retained-row coverage.
- `information_loss_vs_k.png`: roll-up loss.
- `suppression_error_vs_k.png`: relative SUM error under suppression.
- `paper_privacy_utility.csv`: compact table used for paper verification.

## Capability Changes

Generated with:

```bash
python run_orchestration_experiment.py
```

- `orchestration_results.csv`: raw coordination rows.
- `orchestration_summary.csv`: mean coordination metrics by mode.
- `orchestration_summary.md`: Markdown rendering of the summary.
- `sample_agent_audit.csv`: representative event trace.
- `sample_capability_manifests.json`: representative capability metadata.
- `paper_capability_change.csv`: compact table used for paper verification.

The reported capability-change rows use `query_id == DRIFT` over seeds `11`, `22`, and `33`.

## Verification

```bash
python scripts/export_paper_tables.py
python scripts/verify_reported_results.py
```

Timing fields are machine-dependent diagnostics. They are not replication targets.
