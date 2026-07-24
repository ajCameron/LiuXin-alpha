# PostgreSQL Backend Runbook

LiuXin's PostgreSQL backend is intended for large libraries where SQLite is not
the right persistence layer. The backend is registered as `PostgreSQL` with
aliases `postgres`, `postgresql`, and `pg`.

## Install

Use the repo-local environment and include the PostgreSQL extra:

```bash
bash scripts/create_venv.sh
. .venv/bin/activate
python -m pip install -e '.[postgres,test,search]'
```

The PostgreSQL extra installs `psycopg2-binary`. System `psycopg2` is also fine
when available in the active Python environment.

## Create Roles, Database, And Schema

Generate server-level setup SQL first. Run this as a PostgreSQL admin from a
maintenance database such as `postgres`:

```bash
python -m LiuXin_alpha.surfaces.cli postgres setup-sql \
  --section server \
  --database liuxin \
  --owner-role liuxin_owner \
  --runtime-role liuxin_runtime \
  --schema liuxin \
  > /tmp/liuxin-postgres-server.sql

sudo -u postgres psql -d postgres -f /tmp/liuxin-postgres-server.sql
```

Set role passwords separately with `psql` or your normal secret-management
tooling; do not put passwords in generated SQL files:

```bash
sudo -u postgres psql -d postgres
\password liuxin_owner
\password liuxin_runtime
\q
```

Then generate the database-local setup section and run it while connected to the
target database:

```bash
python -m LiuXin_alpha.surfaces.cli postgres setup-sql \
  --section database \
  --database liuxin \
  --owner-role liuxin_owner \
  --runtime-role liuxin_runtime \
  --schema liuxin \
  > /tmp/liuxin-postgres-database.sql

sudo -u postgres psql -d liuxin -f /tmp/liuxin-postgres-database.sql
```

The database section grants runtime privileges for existing tables and sets
default privileges for the owner role, so tables created later by
`liuxin_owner` are readable/writable by `liuxin_runtime`.

## Initialise LiuXin Tables

Create the LiuXin schema as the owner role:

```bash
python -m LiuXin_alpha.surfaces.cli postgres init \
  --url postgresql://liuxin_owner@localhost/liuxin \
  --schema liuxin
```

Use `.pgpass`, `PGSERVICE`, `LIUXIN_POSTGRES_PASSWORD`, or the interactive
password prompt for authentication. For non-interactive automation, add
`--no-password-prompt` only after credentials are already configured.

## Check Runtime Readiness

Run the strict checker as the runtime role:

```bash
python -m LiuXin_alpha.surfaces.cli postgres check \
  --url postgresql://liuxin_runtime@localhost/liuxin \
  --schema liuxin
```

For a login-only check before schema creation:

```bash
python -m LiuXin_alpha.surfaces.cli postgres check \
  --url postgresql://liuxin_runtime@localhost/liuxin \
  --schema liuxin \
  --connect-only
```

To write a reusable environment file:

```bash
python -m LiuXin_alpha.surfaces.cli postgres check \
  --url postgresql://liuxin_runtime@localhost/liuxin \
  --schema liuxin \
  --connect-only \
  --store-env-file /tmp/liuxin-postgres.env
```

Password export is intentionally opt-in with `--store-password`; prefer
`.pgpass`, `PGSERVICE`, or a secret manager for persistent credentials.

## Service Profiles

`PGSERVICE` and `LIUXIN_POSTGRES_SERVICE` are supported. A service profile keeps
connection details out of command history:

```bash
python -m LiuXin_alpha.surfaces.cli postgres check \
  --service liuxin_runtime \
  --schema liuxin
```

The generated env file can also export a service name:

```bash
python -m LiuXin_alpha.surfaces.cli postgres write-env \
  --service liuxin_runtime \
  --schema liuxin \
  --output /tmp/liuxin-postgres.env
```

## Live Smoke

After the checker passes, run the disposable live smoke harness:

```bash
python scripts/run_postgres_live_smoke.py \
  --url postgresql://liuxin_owner@localhost/liuxin \
  --schema liuxin_smoke \
  --drop-schema
```

The smoke script creates/initialises the schema, runs strict checks, exercises a
small driver CRUD path, and drops the disposable schema when requested. Use
`--service`, `--env-file`, or `LIUXIN_POSTGRES_URL`/`LIUXIN_POSTGRES_SERVICE`
instead of `--url` when that better matches the environment.

## Common Failures

`role "... " does not exist` means PostgreSQL accepted the socket/network
connection but the login role has not been created. Generate and apply the
`setup-sql --section server` output or create the role manually.

`No PostgreSQL URL or service profile configured` means no target was supplied
through `--url`, `--service`, `LIUXIN_POSTGRES_URL`, `LIUXIN_DATABASE_URL`,
`LIUXIN_POSTGRES_SERVICE`, or `PGSERVICE`.

Missing privilege checks after schema creation usually mean the database-local
setup section was not run, or default privileges were not applied for the owner
role that created the LiuXin tables.
