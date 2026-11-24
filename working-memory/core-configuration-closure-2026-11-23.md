# Core configuration closure — 2026-11-23

Implemented CORE-04 / completion-plan slice 2 / architecture A12 after the user
requested implementation with heavy Core regression coverage. Separate local
commit on `core-review`, following `7dc7d426`. Worktree:
`/tmp/liuxin-core-review-20261123`. No push/PR requested.

The original `/mnt/c/dev/LiuXin-alpha` checkout remains on `cache-review`; its
existing dirty data submodule and six stashes must remain untouched.

## Changes

- `ptempfiles` uses passive `utils.paths`; the direction gate rejects utils
  imports from startup, including deferred and type-only dependencies.
- Core captures concrete preference stores, including empty mappings and live
  proxies. Construction, lazy services, dispatch and cleanup use that store;
  nested/failing scopes restore the caller. Closed graph contexts release their
  ContextVar bookkeeping keys.
- `core.connections` captures absolute SQLite/APSW paths and native PostgreSQL
  targets/schemas, normalizes aliases and independently copies metadata. Named
  workers and durable workers reopen the selected graph. Alternate sync targets
  resolve independently; one-shot registration preserves URL/service selectors.
- Managed jobs capture the submitting store even when it is the process default.
  Retries retain it. Serial work shares values; spawned work copies at startup.
  Backend/pickling/startup failures become terminal failed jobs and pipes close.
- The configuration ownership inventory records component capture/reload rules,
  application/library separation and process-singleton limits. Service files and
  authentication remain driver-owned inputs read when opening connections.

## Validation

Added 117 cases: 79 configuration, 32 registration selector and six direction
cases. Concurrent profile dispatch, alternating queued stores, serial/process
retries, failure cleanup, private connection options and repeated graph contexts
are covered. Real worker graphs use SQLite, APSW and disposable local PostgreSQL
16, including independent URL/service schemas and named durable maintenance.

- Final production-code verification: **311 passed** in 268.16s.
- Corrected service contracts and documentation: **46 passed** in 16.67s.
- Earlier acceptance including HTTP contracts: **276 passed** in 165.40s.
- Full quality gate passed: 479 formatted files, 482 annotation targets, 268
  protected import modules, 2,092 direction modules, 31 cold-import budgets,
  lint/complexity, basedpyright zero errors/warnings, mypy 575 files and 127/123
  rejected negative examples respectively.
- Broad Core/preferences/jobs/config/cache/surface selection: **813 passed, one
  failed** in 1,340.73s. Its already-collected assertion required shared nested
  job arguments. The corrected contract test checks caller-mutation isolation
  instead and passed in the 46-test selection above. No other broad failures.
- Final documentation link/public-boundary checks: **22 passed** in 15.78s.

These selections overlap. Logs/JUnit are under
`/tmp/liuxin-core-configuration-{verified,service-contracts-final,acceptance,regressions}.*`;
quality output is `/tmp/liuxin-core-configuration-quality-complete.log`.
The focused warning is an existing ISO-639 `codecs.open` deprecation.

Tests use `.venv/bin/python` (shared `/tmp/liuxin-pr-192-venv`, Python 3.14),
`PYTHONPATH=src` and
`LIUXIN_PLUGIN_CACHE_PATH=/tmp/liuxin-core-review-plugin-cache.json`.
Live tests set `LIUXIN_TEST_POSTGRES_URL` to the disposable local server.

## Additional domain gap and next work

Real `ingest.disk.start` jobs reached the correct native PostgreSQL graph but
failed before scanning: `storage.reconcile.store_db_sync._ensure_schema_support`
requires the legacy `files` table. Both URL metadata and service selectors
reproduced it. This is documented in the review and task lists for domain slices
5/8; do not claim native PostgreSQL unmanaged ingestion works. Exploratory
`/tmp/liuxin-core-configuration-final.log` records 148 passes and these two
failures, separately from the passing configuration acceptance above.

Next is completion-plan slice 3: expose the complete native metadata workflow
through named Core operations. Whole-Core sign-off remains pending. Canonical
scope, evidence and ordering live in `dev-docs/core/configuration-ownership.md`,
`next_tasks.md` and `review-and-completion-plan-2026-11-23.md`.

## Disposable resources

Task-only PostgreSQL cluster: `/tmp/liuxin-core-configuration-pg`, socket
`/tmp/liuxin-core-configuration-pg-socket`, localhost port 55439, user blackjane,
database postgres. The task cluster was stopped after validation. The other
pre-existing PostgreSQL cluster was left alone. No external database was used.
