# Optional Hermes and Buzz Runtime Adapter

This directory contains the optional runtime configuration for the prototype. The reported experiments use the deterministic coordination harness and do not require Hermes or Buzz.

## Files

- `hermes-coordinator-config.example.yaml`: Hermes profile settings for the coordinator.
- `hermes-site-config.example.yaml`: Hermes profile settings for a site.
- `coordinator.env.example`: coordinator environment variables.
- `site.env.example`: site environment variables.
- `site.example.json`: site-local database, threshold, capability, and release settings.
- `query.example.json`: example analytical request.
- `.hermes/skills/federated-kpi-aggregation/SKILL.md`: project skill used for protocol turns.
- `hermes_buzz.py`: command-line entry point in the repository root.

## Protocol

Hermes realizes the coordinator and site-agent roles and invokes the local adapter. Candidate ordering, cohort checks, release checks, and KPI calculations remain in deterministic Python code.

Buzz carries canonical `federated-kpi/0.3` JSON messages through the `buzz` CLI:

```text
CAPABILITY_REQUEST -> CAPABILITY
PLAN_PROPOSAL      -> PLAN_REPLY (SAFE / UNSAFE / UNSUPPORTED)
AGGREGATE_REQUEST  -> AGGREGATE_RELEASE
                         or AGGREGATE_REFUSAL
```

Protocol replies do not include a site's private `local_min_k`, minimum local count, or probe timing. `AGGREGATE_RELEASE` contains grouping values and the approved statistics `n`, `sum_value`, and `sumsq_value`.

## 1. Prepare the Prototype

Create the Python environment as described in the main README. For a demonstration with synthetic site databases, run:

```bash
python run_experiment.py
```

This creates local SQLite warehouses below `results/`. It is not needed when existing site databases are configured.

## 2. Install Hermes and Buzz

Install a current Hermes release and the current Buzz CLI. The adapter uses Hermes project skills and the Buzz CLI message interface.

Check the local integration surface with:

```bash
python hermes_buzz.py doctor
```

The command reports detected executables, versions, and required configuration without printing secret values. If it is run outside an active Hermes Buzz session, profile-specific values may not be visible to the shell.

Hermes, Buzz, model credentials, relay credentials, and Nostr private keys are not included in this repository.

## 3. Create One Profile per Agent

Create one Hermes profile for the coordinator and one for every site, for example:

```bash
hermes profile create kpi-coordinator
hermes profile create kpi-site-p1
hermes profile create kpi-site-p2
hermes profile create kpi-site-p3
hermes profile create kpi-site-p4
```

Use a separate Buzz identity and Nostr keypair for every profile.

Use these files as templates:

```text
hermes-coordinator-config.example.yaml
hermes-site-config.example.yaml
coordinator.env.example
site.env.example
```

For every profile, set `terminal.cwd` to the absolute repository root. Keep `BUZZ_PRIVATE_KEY` in the profile `.env` file, not in repository files. The supplied configurations pass only the required `FEDERATED_KPI_*` variables to terminal children and suppress interim Buzz messages on the protocol channel.

## 4. Trust the Project Skill

Trust the repository for every profile that runs a gateway:

```bash
hermes -p kpi-coordinator skills trust /absolute/path/to/federated-kpi-aggregation
hermes -p kpi-site-p1 skills trust /absolute/path/to/federated-kpi-aggregation
hermes -p kpi-site-p2 skills trust /absolute/path/to/federated-kpi-aggregation
hermes -p kpi-site-p3 skills trust /absolute/path/to/federated-kpi-aggregation
hermes -p kpi-site-p4 skills trust /absolute/path/to/federated-kpi-aggregation
```

The trusted project root and `terminal.cwd` must refer to the same checkout.

## 5. Configure the Buzz Channel

Every coordinator and site identity must be a member of the Buzz relay or community and of the dedicated protocol channel. Verify membership with:

```bash
buzz channels list --member
buzz channels members --channel <PROTOCOL_CHANNEL_UUID>
```

The structured protocol messages contain no conversational mentions. The supplied Hermes profile templates therefore use:

```yaml
require_mention: false
allow_all_users: false
```

Use explicit peer allowlists:

- a site profile allows only the coordinator identity;
- the coordinator profile allows the participating site identities and, if required, an authorized operator.

Do not open the protocol channel to arbitrary users.

### Sender Identity

Hermes authenticates and allowlists the Buzz sender before invoking the project skill. The skill passes the message content to the deterministic adapter. The application-level `party_id` is not cryptographically bound to the authenticated sender pubkey in this prototype.

The runtime therefore assumes authorized participants use their assigned `party_id`. A deployment that requires protection against malicious authorized participants should bind sender pubkeys to party IDs or use separate channels or direct messages.

## 6. Configure Site State

Create one site configuration from `site.example.json` and set `db_path` to the local warehouse, for example:

```text
integrations/hermes-buzz/P1.json
```

`local_min_k` remains in this site-local file and is not sent through Buzz. The file is reloaded for each request so capability changes can take effect without changing coordinator state.

Runtime state is stored under `.runtime/`, which is ignored by Git. Coordinator transitions and site release-budget updates use an advisory file lock and atomic JSON replacement. The runtime adapter uses POSIX file locking and is intended for Linux or macOS hosts.

## 7. Start the Gateways

Start one gateway for each profile:

```bash
hermes -p kpi-coordinator gateway start
hermes -p kpi-site-p1 gateway start
hermes -p kpi-site-p2 gateway start
hermes -p kpi-site-p3 gateway start
hermes -p kpi-site-p4 gateway start
```

Use `gateway status` with the corresponding profile to check each process.

Incoming `federated-kpi/0.3` messages are handled by the project skill. It stores the received JSON under `.runtime/`, invokes the deterministic adapter, and lets the adapter send the canonical reply through the Buzz CLI. After a successful protocol tool call, the skill returns `[SILENT]` so Hermes does not add a second conversational message. Adapter errors remain visible.

## 8. Start a Coordination Session

From the coordinator checkout:

```bash
python hermes_buzz.py coordinator-start \
  --state .runtime/coordinator.json \
  --query integrations/hermes-buzz/query.example.json \
  --k 10 \
  --parties P1 P2 P3 P4 \
  --send
```

Use `--weights` when non-default hierarchy-loss weights are required:

```bash
--weights '{"geo":1.2,"age":1.0,"product":0.8,"time":1.0}'
```

Inspect coordinator state with:

```bash
python hermes_buzz.py coordinator-status --state .runtime/coordinator.json
```

`--send` uses `BUZZ_HOME_CHANNEL` unless `--channel` is given. If Buzz is not on `PATH`, set `BUZZ_CLI_PATH`.

When `hermes_buzz.py --send` is started directly from a shell instead of a Buzz-triggered Hermes turn, the shell must provide the Buzz settings required by the CLI, including `BUZZ_RELAY_URL`, `BUZZ_PRIVATE_KEY`, and `BUZZ_HOME_CHANNEL`.

## Measurement Boundary

The reported candidate and reply counts are produced by `run_orchestration_experiment.py`. Relay, network, and model latency are not part of these measurements. A live relay run additionally requires installed Hermes and Buzz binaries, identities, channel membership, credentials, and model configuration.
