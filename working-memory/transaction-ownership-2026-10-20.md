# Transaction ownership, 2026-10-20

The full **A01–A16** programme remains open. A04 is on
`database-transaction-ownership`, based on A15's `a0e4d2ed`. No PRs have been
merged. Preserve the six unrelated staged storage files and dirty data submodule;
use explicit-path commits. Their hashes and index entries remain unchanged.

## Implementation

- `databases/transactions.py` separates transaction and connection ownership.
  Owned units explicitly BEGIN; nested and borrowed units use unique savepoints.
  A caught inner failure rolls back only that unit. A failed commit rolls back
  the owned unit; caller-owned connections stay open.
- Portable macro writes and temporary value/ID tables use that boundary.
  Temporary-table cleanup no longer commits or rolls back a caller's outer unit.
- Database close/resource exit and replacement of a different driver discard
  pending work. Already committed operations survive resource cleanup.
- DriverWrapper.lock is a real reentrant mutex. Raw SQL contract tests and six
  legacy cache fixture callers use explicit connections instead. Cache fixtures
  now register cleanup or use a resource context.
- API docstrings, caller comments, the connection guide and the new canonical
  `dev-docs/databases/transactions.md` describe compatibility and inventory the
  remaining direct commit points. A08/A10 retain further writer consolidation.
- The helper and new regressions enter formatting/lint/type scopes.

## Verification

- Initial nine regressions failed before the implementation and passed after it.
  The expanded module now has nineteen cases covering cleanup, replacement,
  nesting, borrowed work, temporary tables and actual mutex exclusion.
- **40 live PostgreSQL checks passed**, including six new ownership regressions,
  existing portable macros, backend unit contracts and storage integration.
  A disposable socket-only cluster was stopped and its data directory removed.
- The entire quality runner passed after using Generator for the new context
  manager's return annotation: formatting, lint, architecture, cold imports,
  complexity, basedpyright/mypy and both sets of 37 negative type contracts.
- The serial SQLite/APSW-named facade, Catalog/Row/Agent, storage and Core run
  completed with 426 passes and 25 skips. Its sole failure was the tracked-link
  guard because new candidate files had not been staged in the review checkout.
  After staging the complete tree, documentation/workflow/API checks passed all
  52 cases. A separate UTF-8 PostgreSQL Catalog parity run is still active.
  The first attempt used a SQL_ASCII fixture and failed Unicode cases; the
  alias case passes in UTF-8. Do not treat the misconfigured run as a new
  application baseline. Windows-specific handle checks remain skipped on WSL.
- Cache migration selection: **22 passed, six legacy modules skipped** by their
  existing opt-in gates. Enabling the generated Calibre-library case fails during
  Database bootstrap because FRBR helper tables are absent. The same failure is
  reproduced on the unchanged A15 parent, before the migrated fixture line.
  Do not claim that this enables or validates the disabled legacy cache feature.
- The first broad A04 run was stopped after two old commit-on-close expectations
  failed; those tests now assert rollback and that close never requests commit.
  The interruption also appeared as one setup error; it is not a new baseline.

Local evidence: `/tmp/liuxin-a04-final.log`, `-postgres.log`, `-catalog-utf8.log`,
`-docs-final.log`, `-quality-final.log`,
`-cache-migration.log`, `-legacy-probe.log`, `-legacy-baseline.log`, and telemetry
under `/tmp/liuxin-a04-telemetry`. The final isolated checkout is
`/tmp/liuxin-a04-quality`; do not remove it while its test process is active.

## A15 follow-up

PR #131 now includes process-local CI pytest options and matching workflow
contracts. The local permitted full suite on initial A15 source completed with
7,389 passes and the terminal no-create failure fixed afterward. Remote dd44c953
had 7,384 passes and four nested pytest failures from inherited plugin flags.
The corrected workflow/child probes pass 32 checks; the complete formatting scope
passes. Latest head `a0e4d2ed` has green quality, packaging and format lanes; its
full-suite CI run #39441669661 remains in progress. Check the current-head result
before marking A15 complete. The whole A programme is not complete.

Next implementation after A04 is A05 deletion policy. Guard both remove_replica
and router.delete before byte effects, serialize concurrent decisions, preserve
retention/last-copy/read-only rules, and report partial byte/metadata outcomes
honestly. Raw Store/driver deletion remains privileged low-level access.
