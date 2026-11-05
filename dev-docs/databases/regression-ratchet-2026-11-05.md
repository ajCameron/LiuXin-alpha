# Database regression ratchet — 2026-11-05

Status: implemented and locally verified; final validation is recorded below. This follows the
[package typing review](package-typing-2026-11-04.md). Typing exposed runtime
contract defects as well as incomplete annotations. Each repaired defect needs
an executable regression that remains a merge requirement.

## What changed

| Boundary | Repair or mitigation | Required regression owner |
| --- | --- | --- |
| Single-use iterators | Materialize reused owner IDs/column specifications once; iteration failure publishes no partial write. | `test_custom_column_service_contracts.py`, `test_schema_builder_contracts.py`, `test_custom_value_transactions.py` |
| Runtime ownership | Bind `.db`; prepare maintenance before starting it, reuse the worker on repeated initialization, and stop/join before disabling. Restore callbacks, owners and preferences on failure. Retain a worker whose cleanup failed for a later `close()` attempt. | `test_database_lifecycle_contracts.py` |
| SQLite/APSW callbacks | Native aggregate factories use their backend's required shape; scalar reads use supported cursor iteration. | `test_sql_helper_contracts.py` |
| Error/resource handling | Preserve database errors, close owned cursors/connections on success and failure, and leave borrowed connections/transactions alone. | `test_macro_type_boundaries.py`, `test_postgres_adapter_contracts.py`, `test_schema_builder_contracts.py`, `test_sql_helper_contracts.py` |
| Backend capabilities/lifetimes | PostgreSQL exposes its supported macro API; legacy-only calls fail explicitly. Closed facade collaborator access fails explicitly. | `test_macro_capability_contracts.py`, `test_database_lifecycle_contracts.py` |
| Row state | `Row.state` distinguishes `DRAFT`, `IDENTIFIED` and `MISSING` without conflating `{}`, `False` and `None`. Draft `sync()` inserts its complete payload in one operation, assigning an ID only after success. | `test_row_state_contracts.py` |
| Legacy cached values | Public set/bulk/delete/rename operations own a transaction, restore touched cache rows on failure and notify after commit. Shared-value edits reload affected owners without losing their other choices. | `test_custom_value_transactions.py` |
| Temporary SQL | Execute TEMP DDL statements individually: SQLite `executescript` could commit unrelated work. Per-operation scratch names avoid collisions. | `test_custom_value_transactions.py` |

These test files live in [tests/databases](../../tests/databases). The existing
contracts for registration, facade metadata and legacy exports also belong to
the ratchet; they protect the supporting boundaries changed during typing.

## Enforced baseline

The typing extra also installs APSW and the native PostgreSQL adapter alongside
its stubs. The previous PR's full suite passed, but its clean CI typing environment
failed because APSW was present locally and absent from the declared extra.
`test_run_type_checks.py` now protects this installation requirement.

The [manifest](../../tests/databases/regression_ratchet.json) names individual
pytest node IDs, including backend/parameter variants, grouped by invariant.
The [runner](../../scripts/run_database_regression_ratchet.py) requires every
listed case to be collected and pass setup, call and teardown. A missing test,
deselection, skip, xfail or xpass fails the gate. A test-count increase cannot
compensate for losing a named regression.

The **Database Regression Ratchet** CI job installs APSW and the PostgreSQL
extra, provides a disposable PostgreSQL service, and runs serially. Its result
is required by **Tests Passed**. It compares the manifest with the PR base or
previous push commit and refuses to remove a case or move it out of its
invariant. The first commit introducing the manifest bootstraps this check;
manual workflow runs still enforce all current cases. Git lookup failures do
not silently disable comparison.

Telemetry and a machine-readable result are uploaded even on failure. Scratch
is outside the checkout and removed after the run. This focused gate supplements
the full suite and both type checkers; it does not replace them.

For local validation, point at an expendable test server and run:

```bash
PYTHONPATH=src:. \
LIUXIN_TEST_POSTGRES_URL='postgresql://user:password@localhost/test_database' \
.venv/bin/python scripts/run_database_regression_ratchet.py \
  --baseline-ref <full-base-commit-sha> \
  --artifacts /tmp/liuxin-database-regressions
```

Omit `--baseline-ref` for a current-baseline run. PostgreSQL-unconfigured and
APSW-unavailable skips are failures in this gate. Ordinary focused pytest runs
remain available while developing, but they are not a ratchet signoff.

## Adding coverage

1. Reproduce the defect with a test at its observable boundary. Prefer real
   database transactions and an independent reader for durability checks.
2. Include failure and retry paths, iterator reuse, borrowed resource ownership,
   and relevant backends. Use fault injection when a real failure is impractical;
   assert the durable state and cache state, not only which helper was called.
3. Fix the defect, run its focused tests, then add every relevant parameterized
   node ID to the corresponding manifest invariant. New invariants need their
   own group and a short explanation in this guide or their feature guide.
4. Run the ratchet, affected integration tests and required quality gates. Keep
   old cases. Do not lower the baseline, add xfails/skips, or substitute an
   unrelated test to make a regression green.

Stable test identities are deliberate. A future removal or rename needs an
explicitly reviewed migration of the invariant and gate policy; there is no
routine “accept lower baseline” switch. This is a ratchet for demonstrated
behavior, not a claim of complete line/branch coverage. Review must still check
that assertions meaningfully test the invariant.

## Compatibility and remaining limits

The legacy cached custom-column facade now rejects an active transaction before
mutation. Its `set_custom(..., commit=False)` raises `ValueError`; use explicit
SQL macros for caller-managed transactions without this cache. This deliberate
restriction prevents a cache from appearing committed while the caller can
still roll back. The modern WEMI definition-storage path remains separate.
`delete_cc_item` at the SQL layer preserves a caller transaction with a savepoint.

Cache-aware calls must be serialized with their cache readers. Rollback restores
touched row values and enumeration lists in place. Host dirty hooks must honor
`commit=False` and avoid external side effects; arbitrary third-party callback
side effects cannot be rolled back. Notifications run after SQL/cache success;
a listener exception includes a note that the write committed. Do not blindly
retry the write. Durable/retriable listener delivery is future work.

Maintenance initialization likewise requires the caller to serialize it with
normal database use. A shutdown timeout is reported and ownership retained;
it does not force-kill a worker. `Database.close()` retains its best-effort
cleanup contract. `RowState.IDENTIFIED` means the local payload has an identity,
not that a concurrent database deletion is impossible. Existing missing-load
sentinels and writable `deepcopy(Row)` behavior are preserved and tested.

## Validation

- Required manifest: **240 passed, zero skips/xfails**, including native APSW
  callbacks and live PostgreSQL cursor/Row writes.
- Affected integration/tooling selection: **292 passed**, with one initial
  documentation failure because newly linked files were unstaged. After staging,
  the final documentation/CI/tooling run passed **87 tests**. Selections overlap.
- All existing formatting, lint, architecture, import-footprint and typing gates
  passed. Both checkers also passed against a fresh environment containing only
  declared base/typing dependencies: **453 mypy sources**, and **zero basedpyright
  errors or warnings**. This includes the APSW dependency repair found in CI.
- The gate itself has subprocess regressions proving that real pytest skips and
  xfails cannot produce a green result, plus baseline-removal and missing-case tests.

Runs were serial, with telemetry and scratch under `/tmp`; the private PostgreSQL
instance was stopped and removed. The earlier lifecycle tests reproduced worker
leaks before the fix, and fault-injection tests exercise SQL, cache, dirty-hook,
commit, startup, wiring and notification failures. No new full-suite run or
production archive import is claimed for this follow-up.
