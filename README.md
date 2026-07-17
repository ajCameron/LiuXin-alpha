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

The full-suite helper also expects this repo-local venv:

```bash
bash scripts/run_full_test_suite.sh
```

Run the strict static typing target set with:

```bash
bash scripts/run_type_checks.sh
```

The type-check helper installs the `typing` extra into `.venv` before running
`basedpyright` and `mypy`. Use `--skip-install` to reuse an already prepared
environment.

## PostgreSQL Backend

For large-library work, install the `postgres` extra and follow the PostgreSQL
setup/check/smoke runbook:

```text
docs/development/postgresql-backend.md
```
