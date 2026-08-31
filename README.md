# LiuXin-alpha

Public, development fork of LiuXin. Alpha. DO NOT USE IN PROD.

## Local Setup

LiuXin-alpha expects Python 3.12 or newer.

Create the repo-local virtual environment and install the common development extras with:

```bash
bash scripts/create_venv.sh
```

That creates `.venv/` in the repository root and installs the package in editable mode with the `test` and `search` extras.

Activate it with:

```bash
. .venv/bin/activate
```

Run the test suite with:

```bash
.venv/bin/python -m pytest
```

For a smaller local confidence pass—representative database contracts plus
smoke coverage across the rest of the project—run:

```bash
.venv/bin/python scripts/run_test_stream.py --stream confidence
```

The runner also exposes `database` and `smoke` streams; omitting `--stream`
still runs the full suite. See `dev-docs/test-streams.md` for the
selection and maintenance rules.

The full-suite helper also expects this repo-local venv:

```bash
bash scripts/run_full_test_suite.sh
```

Run the strict static typing target set with:

```bash
bash scripts/run_type_checks.sh
```

The type-check helper installs the `typing` extra into `.venv`, verifies
complete callable annotations across `file_formats`, and then runs
`basedpyright` and `mypy` over the configured strict targets. Use
`--skip-install` to reuse an already prepared environment.

## Storage ingest CLI

For a new local deployment, initialise a self-contained system root and then
point LiuXin at a mounted drive or untidy source tree:

```bash
liuxin init /srv/liuxin
liuxin connect /srv/liuxin
liuxin ingest /media/archive-drives/disk-01
```

Running `liuxin init` with no location in an interactive terminal opens the
guided initializer. It can select SQLite, APSW, or PostgreSQL, shows a plan
before changing anything, and performs the appropriate post-initialization
checks. The PostgreSQL route initializes LiuXin's schema in an existing
database/login and runs the full backend readiness checker.

`init` creates the catalogue, a managed primary Store, materialization and log
directories, plus a non-secret `liuxin-system.json` manifest. It is idempotent
and does not delete existing catalogue or Store state. The concise `ingest`
form uses the bounded recursive mixed-file pipeline.

The full mixed-file ingest surface remains available after installation:

```bash
liuxin storage ingest --help
```

Use `--discover-only` for classification with no catalogue writes and
`--preflight-only` to check an intended remote-host run before creating its
SQLite catalogue or materialization cache. See
`dev-docs/storage/mixed_ingest_operations.md` for the full run, resume,
logging, locking, signals, reports, and service-supervision contract.
Completed and interrupted attempts can be inspected without reading JSONL by
hand:

```bash
liuxin ingest runs list --system-root /srv/liuxin
liuxin ingest runs issues RUN_UUID --system-root /srv/liuxin
liuxin ingest runs resume RUN_UUID --system-root /srv/liuxin
```

## Metadata CLI

The packaged metadata surface supports hydrated catalogue reads, deterministic
whole-catalogue or selected-Item JSON dumps, WEMI field updates, OPF export,
embedded ebook metadata inspection and safe rewritten-artifact output, plus
managed online identify and cover jobs:

```bash
liuxin metadata --help
liuxin metadata dump-json --database catalogue.sqlite --all --output metadata.json
```

Every command can instead use `--core-endpoint`. File paths always refer to the
CLI host and are transferred as bounded bytes; embedded writes create a new
artifact by default. See `dev-docs/metadata-cli.md` for the complete command,
JSON, atomic-output, in-place safety, and remote-operation contracts.

## Operational CLI

The installed command also exposes Core diagnostics and serving, managed jobs,
semantic catalogue search/acquisition, Store and Replica administration,
managed ingest/conversion/backup workflows, database upkeep, guarded
maintenance, packaged HTTP surfaces, and plugin/capability inspection:

```bash
liuxin connect /srv/liuxin
liuxin connect status
liuxin core health
liuxin doctor
liuxin disconnect

liuxin core health --database catalogue.sqlite
liuxin doctor --system-root /srv/liuxin
liuxin jobs list --database catalogue.sqlite
liuxin storage stores --database catalogue.sqlite
liuxin storage status --system-root /srv/liuxin
liuxin plugins inspect --database catalogue.sqlite
```

Core-backed leaves accept `--database`, `--core-endpoint`, `--system-root`, or
`--profile`; the last two also have `LIUXIN_SYSTEM_ROOT`/`LIUXIN_PROFILE`
environment forms. `connect` persists a mode-0600 manifest pointer for later
commands and terminals; explicit arguments and environment selectors take
precedence, while `disconnect` removes only the pointer. `config show|validate`
explains the selected deployment and
`diagnostics collect` creates a redacted support report. Byte
transfer paths belong to the CLI host; ingest, conversion, and backup workflow
paths belong to the Core host. HTTP services default to loopback because they
do not provide built-in authentication or TLS. See
`dev-docs/operational-cli.md` for the command map and safety contract.

## PostgreSQL Backend

For large-library work, install the `postgres` extra and follow the PostgreSQL
setup/check/smoke runbook:

```bash
liuxin init --wizard
```

The guided route is suitable when the PostgreSQL database and login role
already exist. Server-level role/database provisioning remains explicit in the
runbook.

```text
dev-docs/postgresql-backend.md
```
