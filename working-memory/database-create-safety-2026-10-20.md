# Database creation safety, 2026-10-20

The active goal is the **entire A01–A16 architecture programme**, delivered as
focused PRs/commits. Its acceptance criteria remain in
`dev-docs/architectural_reviews/next_tasks.md`; completing A03 does not complete
that goal. A01 is PR #128, A02 is PR #129, and A14 is documented in PR #127.
Continue through A04–A13/A15/A16 and verify the complete programme before closing
the goal. Do not silently reduce it to the first implementation slice.

Branch `database-create-safety` starts at `d5956d6e` and targets
`store-read-only-intent` while #129 is open.

## A03 implementation

- SQLite/APSW creation builds and checks an owned sibling candidate, closes its
  connections, then publishes with an exclusive hard link. Existing files,
  symlinks, orphaned sidecars and targets appearing during preparation survive.
- `Database` initializes before connecting its runtime graph. Legacy create
  flags cannot authorize replacement. The registry uses the same FRBR path.
- Explicit offline replacement requires an absent recovery path. It prepares a
  new schema and verified SQLite backup before atomic replacement. Existing
  backups are protected and published backups survive final-rename failure.
  All users must be stopped; observed identity checks are not a process lease.
- Core exposes replacement and the terminal wizard records separate intent and
  a mandatory backup path. PostgreSQL refuses file-style replacement.
- PostgreSQL creation accepts absent/empty namespaces, uses a transaction advisory
  lock for competing initializers, rejects populated namespaces and rolls back
  partial schema creation. Empty setup-sql namespaces remain supported.
- The new creation helper is in formatting, lint and strict typing scopes.
  Canonical creation/recovery docs and architecture/database task status are updated.

## Verification checkpoint

- 105 focused tests passed, with two Windows-only deletion checks skipped:
  initial A03 regressions, existing driver lifecycle/PostgreSQL unit checks,
  Core lifecycle, CLI init and PostgreSQL CLI checks.
- Seven real PostgreSQL 16 tests passed: public creation routes preserve
  populated schemas, absent/precreated-empty schemas work, concurrent creators
  have one winner, and partial DDL rolls back.
- Two existing live PostgreSQL macro and storage concurrency/restart/recovery
  tests also passed. The owned disposable cluster has been stopped and removed.
- The complete formatting, annotations, dependency/layer, cold-import, lint,
  complexity, basedpyright/mypy and internal-contract gates passed in isolation.
  The final helper edits also passed both type checkers and lint.
- The isolated 123-test run passed: A03 regressions, A02 read-only regressions,
  documentation links/public boundaries, and format/type-runner tests.
- The final follow-up passed all 60 tests: 38 creation/replacement regressions,
  including closed WAL databases, busy-writer refusal and backup permissions,
  plus 22 documentation checks. Final lint/type checks and whitespace checks pass.
- The main checkout's formatter is blocked by a pre-existing staged change in
  `storage/durable_manager.py`. An isolated review worktree
  contains only this PR's changes for full-gate validation; preserve all six
  unrelated staged files and the dirty data submodule in the main checkout.
- PR #129's focused/quality checks are green, but full run `39368762975` has 40
  failures and 7,308 passes. Its failing test IDs exactly match parent #128's run
  `39366322310`. Missing test dependencies and surface/image fallback groups need
  reproduction and repair under A15 before declaring the series verified.

Tests use serial execution, telemetry and immediate scratch cleanup. Ignored
evidence is under `working-memory/test-results/database-create-20261020/`.
The isolated checkout used for validation can be removed after publication;
all proposed changes also reside in the main checkout.

Next: repair the shared full-suite baseline under A15, then A04's transaction/close
contract and A05's deletion policy;
A06 transport authorization remains independent. Preserve A07–A16's complete
acceptance criteria rather than treating their first suggested PR as completion.
