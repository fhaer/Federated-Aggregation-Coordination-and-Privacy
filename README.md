# Federated KPI Aggregation with Local Privacy Constraints and Coordinated OLAP Roll-Ups

Research prototype and replication material for the paper **Federated KPI Aggregation with Local Privacy Constraints and Coordinated OLAP Roll-Ups**.

The prototype evaluates KPI queries across independently administered dimensional warehouses without centralizing detailed records. A coordinator searches compatible OLAP roll-ups in increasing information loss. Each site checks its local minimum-cohort rule before release. After a common level is selected, the sites release additive aggregate statistics that are combined into the federation-wide KPI.

## Implementation

The prototype is implemented in Python 3.13 with one SQLite star schema per site.

- `src/federated_kpi/core.py` implements OLAP roll-up selection, local privacy checks, guarded aggregate release, and KPI aggregation.
- `src/federated_kpi/orchestration.py` implements the deterministic coordinator and site-agent logic used for the capability-change experiment.
- `local_tool.py` exposes deterministic site-local `probe` and `aggregate` commands.
- `run_experiment.py` and `run_orchestration_experiment.py` reproduce the two experiments.
- `scripts/` exports and verifies the paper-facing result tables.

For federation threshold `k`, a site may apply a stricter local threshold. A candidate is accepted only when every non-empty local output group satisfies the effective threshold. A site with no non-empty groups is locally safe and contributes an empty result. After selection, every site repeats the threshold check before releasing grouped `COUNT`, `SUM(x)`, and `SUM(x^2)` values. No record-level rows are transferred by the protocol.

## Installation and Usage

### 1. Project Setup

Python 3.13.5 was used for the reported experiments.

macOS/Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Windows:

```text
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Validate the Prototype

```bash
python -m pytest -q
python scripts/verify_reported_results.py
```

### 3. Reproduce the Experiments

Privacy and analytical granularity:

```bash
python run_experiment.py
```

Coordination under capability changes:

```bash
python run_orchestration_experiment.py
```

Regenerate the compact paper-facing tables and verify them:

```bash
python scripts/export_paper_tables.py
python scripts/verify_reported_results.py
```

Synthetic SQLite databases are created below `results/` and ignored by Git. The committed outputs and their purpose are described in [`results/README.md`](results/README.md).

### 4. Site-Local Commands

`local_tool.py` exposes the operations that a site agent may invoke. After `run_experiment.py` has created the synthetic databases, a privacy probe can be run as follows:

```bash
python local_tool.py probe \
  --party P1 \
  --db results/db_seed_11/P1.sqlite \
  --query '{"query_id":"Q","group_levels":{"geo":0,"age":1}}' \
  --levels '{"geo":1,"age":2}' \
  --k 10
```

For an approved level, replace `probe` with `aggregate` to release the grouped sufficient statistics. A stricter site-local threshold can be set with `FEDERATED_KPI_LOCAL_MIN_K`.

## Project Structure

```text
federated-kpi-aggregation/
|-- README.md
|-- requirements.txt
|-- local_tool.py                         # Site-local probe and aggregate commands.
|-- run_experiment.py                     # Privacy and granularity experiment.
|-- run_orchestration_experiment.py       # Capability-change experiment.
|-- hermes_buzz.py                        # Optional runtime entry point.
|
|-- src/
|   `-- federated_kpi/
|       |-- __init__.py
|       |-- core.py                       # OLAP, privacy checks, and KPI aggregation.
|       |-- synthetic.py                  # Synthetic warehouse generation.
|       |-- experiment.py                 # Privacy and granularity experiment.
|       |-- orchestration.py              # Coordinator and site-agent logic.
|       |-- orchestration_experiment.py   # Capability-change experiment.
|       |-- buzz_transport.py             # Optional Buzz transport.
|       `-- hermes_buzz_adapter.py        # Optional Hermes and Buzz adapter.
|
|-- tests/
|   |-- test_core.py
|   |-- test_orchestration.py
|   |-- test_privacy_boundary.py
|   |-- test_interfaces.py
|   `-- test_hermes_buzz_adapter.py
|
|-- scripts/
|   |-- export_paper_tables.py            # Export paper-facing CSV files.
|   `-- verify_reported_results.py        # Verify committed paper values.
|
|-- docs/
|   |-- method.md                         # Method and coordination protocol.
|   `-- reproduction.md                   # Experiment setup and reproduction.
|
|-- results/
|   |-- README.md                         # Result files and generation commands.
|   |-- experiment_config.json
|   |-- experiment_results.csv
|   |-- summary.csv
|   |-- summary.md
|   |-- orchestration_results.csv
|   |-- orchestration_summary.csv
|   |-- orchestration_summary.md
|   |-- paper_privacy_utility.csv
|   |-- paper_capability_change.csv
|   |-- sample_agent_audit.csv
|   |-- sample_capability_manifests.json
|   |-- coverage_vs_k.png
|   |-- suppression_error_vs_k.png
|   `-- information_loss_vs_k.png
|
|-- integrations/
|   `-- hermes-buzz/
|       |-- README.md                     # Optional runtime setup and usage.
|       |-- hermes-coordinator-config.example.yaml
|       |-- hermes-site-config.example.yaml
|       |-- coordinator.env.example
|       |-- site.env.example
|       |-- site.example.json
|       `-- query.example.json
|
`-- .hermes/
    `-- skills/
        `-- federated-kpi-aggregation/
            `-- SKILL.md                  # Optional Hermes project skill.
```

Only material needed to inspect, run, or reproduce the prototype is included. Manuscript files, literature PDFs, local databases, credentials, and runtime state are not part of the repository.

## Documentation

[`docs/method.md`](docs/method.md) describes the OLAP hierarchies, minimum-cohort rule, candidate ordering, coordination sequence, and released statistics.

[`docs/reproduction.md`](docs/reproduction.md) describes the experiment configuration, validation, and reproduction commands.

[`results/README.md`](results/README.md) documents the committed result files and their generation.

## Optional Hermes and Buzz Runtime

The coordination method is independent of a specific agent runtime or message transport. An optional implementation adapter uses the Hermes agent framework for the coordinator and site-agent roles and the Buzz agent communication platform for structured messages. The reported measurements use the deterministic harness and do not require this extension.

Runtime files are kept under [`integrations/hermes-buzz/`](integrations/hermes-buzz/):

- `README.md` gives the complete setup and usage procedure.
- `hermes-coordinator-config.example.yaml` and `hermes-site-config.example.yaml` provide separate Hermes profile configurations.
- `coordinator.env.example` and `site.env.example` provide the corresponding environment variables.
- `site.example.json` defines site-local database, capability, threshold, and release settings.
- `query.example.json` gives an example analytical request.
- `.hermes/skills/federated-kpi-aggregation/SKILL.md` defines the project skill used by Hermes.

The local integration surface can be checked with:

```bash
python hermes_buzz.py doctor
```

See [`integrations/hermes-buzz/README.md`](integrations/hermes-buzz/README.md) for profile setup, Buzz identities and channel membership, project-skill trust, gateway startup, and a live coordination example.

## Scope

The experiments use synthetic data and fact rows as privacy units. A subject-oriented deployment can count distinct subject identifiers instead. The minimum-cohort rule is an aggregate-release criterion; it does not by itself provide differential privacy, cryptographic secure aggregation, or repeated-query protection.

## License

No software license is included in this release package. Add the intended license before granting reuse rights.
