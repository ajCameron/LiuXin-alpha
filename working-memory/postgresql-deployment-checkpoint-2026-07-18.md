# PostgreSQL And Deployment Checkpoint - 2026-07-18

## Scope

This checkpoint closes the dirty worktree on
`codex/stabilize-base-20260627` before the project changes direction. It
combines the post-merge stabilization work with the July PostgreSQL backend and
deployment-source packaging slice.

## What Landed

- Database API and implementation signatures were brought back into alignment,
  including direct driver naming, custom-column discovery, dirty-record
  callbacks, SQL execution, tree, trigger, search, and schema-introspection
  surfaces.
- Legacy file compatibility tables and interlinks were restored for device,
  folder, language, store, and derivation workflows while the storage model
  continues moving toward digital assets.
- Storage compatibility aliases and small backend fixes were added, including
  replication `file_identifier` aliases and cached rclone stat payloads.
- OPF and metadata readers/writers now keep `title_sort` on each metadata
  object instead of using shared module state.
- The `LiuXin_alpha_data` submodule pointer moved from `a8a37b6f` to
  `00d1d2a`, incorporating the two local test-data updates.
- A PostgreSQL backend was added with:
  - driver registration and aliases `postgres`, `postgresql`, and `pg`
  - URL and service-profile configuration with credential redaction
  - a DB-API connection adapter and schema-qualified native SQL
  - native schema generation, runtime grants, and strict readiness checks
  - CLI setup, init, check, grant, schema-SQL, and env-file workflows
  - a disposable live-smoke harness and development runbook
- `scripts/build_deployment_package.py` now builds a focused source tarball,
  checksum, metadata, and remote install/PostgreSQL helper scripts.

## Verification

- Focused PostgreSQL, CLI, live-smoke, and deployment-package tests:
  `54 passed in 1.71s`.
- Full suite run id `pre-direction-change-2026-07-18`:
  `4593 passed, 83 skipped, 18 xfailed`; four HTTP daemon tests could not bind
  localhost sockets inside the restricted sandbox.
- The four HTTP daemon tests were rerun outside the socket-restricted sandbox:
  `4 passed in 1.28s`.
- `git diff --check` passed before staging.

The full-suite JSON, log, and done marker are under
`working-memory/test-results/pre-direction-change-2026-07-18.*` and remain
ignored run artifacts.

## Deliberate Boundaries

- No live PostgreSQL server was available for the external database smoke; the
  mock/native-SQL contract and CLI tests are green.
- PostgreSQL scratch-database switching and unsupported generic macro paths
  still raise explicit not-implemented errors.
- The PostgreSQL runbook is canonical at
  `dev-docs/postgresql-backend.md`.
