# LiuXin-alpha

Public, development fork of LiuXin. Alpha. DO NOT USE IN PROD.

## Installation

LiuXin-alpha requires Python 3.12 or newer. Install the current development
version directly from GitHub with:

```bash
python3 -m pip install "liuxin-alpha @ git+https://github.com/ajCameron/LiuXin-alpha.git"
```

An unpacked checkout can be installed in the same way with
`python3 -m pip install .`. The installation provides these commands:

- `liuxin` — terminal database browser
- `liuxin-cli` — operational CLI surfaces
- `liuxin-storage-audit` — portable, SQLite-only storage-drive audit

For example, to scan a drive on a machine with no PostgreSQL service:

```bash
liuxin-storage-audit \
  --database ./storage-drive-audit.sqlite3 \
  --disk-root /path/to/mounted/storage-drive \
  --store-name portable-storage-audit
```

The audit command creates the local SQLite database if needed, hashes ebook
files by default, and only reads the mounted drive. Use `--no-hash` for a
faster metadata-only pass. Run `liuxin-storage-audit --help` for all options.

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

## PostgreSQL Backend

For large-library work, install the `postgres` extra and follow the PostgreSQL
setup/check/smoke runbook:

```text
dev-docs/postgresql-backend.md
```
