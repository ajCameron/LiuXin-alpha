# Custom-column management docstrings: D126 complete

The explicit next-tranche request authorized **D126: 11 declarations** in
SQL/macros/cc_macros_mixin/cc_management_macros.py, including its nested rating-table
probe. The complete file is reviewed and promoted. **D127 is next**: two declarations
in the SQLite plugin initializer. Earlier broad authorizations remain paused.

Documented definition updates, normalized-name keys, JSON display values, requested
updates versus affected rows, and explicit-connection commits. Table-creation docs
cover both layouts, trigger/view behavior, rating fallback, the unused ordered flag
and the distinction between the probe connection and driver DDL execution. Deletion
docs distinguish flagging, metadata-only deletion and physical cleanup, including
the final deletion of every marked definition regardless of the supplied object map.
Existing behavior, including the trigger's unprefixed value-column limitation, is unchanged.

Verification:

- All 11 declarations pass complete-file audit and normalizer checks. Executable
  AST, comments, signatures and non-docstring literals are unchanged; no added Ruff
  findings or whitespace errors. Original baseline hash retained.
- **15 regression checks passed**, no skips: 13 SQLite caller cases covering four
  normalized datatypes, six scalar datatypes, metadata updates, deferred marking and
  loader cleanup, plus two inherited macro API contract checks.
- **64 runnable example statements passed** with explicit lightweight hosts and
  closed in-memory SQLite connections. They verify normalization/JSON behavior,
  missing-row change flags, driver-routed DDL, fallback views, the update-trigger
  limitation, binding shapes, marked-ID results and empty-map cleanup. The nested
  probe is exercised through table creation; prose examples are not counted as run.
  No live PostgreSQL result is claimed.
- Full quality runner passed: 156 formatted files, 454 annotation-checked files,
  215 protected modules, zero basedpyright errors, mypy 182 files, and both
  contract checkers rejecting all 37 invalid examples. All checks used the final hash.

Coverage: **849/2,671 complete files**, **12,331 declarations in complete files**,
plus **37 partial-file declarations**. Remaining: **1,822 files and 29,793
declarations**. D progress: **126/232 units**, **3,103/5,685 declarations**,
**163/374 complete files**. Across all tracks: **158 documentation units verified**,
**1,214 remaining**. No commit or publication requested. Token usage unavailable.

- [Exact observations and commands](test-results/docstrings-d126-2026-09-22/observations.json)
- [Static proof](test-results/docstrings-d126-2026-09-22/static.json)
- [Examples](test-results/docstrings-d126-2026-09-22/examples.json)
- [Regression log](test-results/docstrings-d126-2026-09-22/regressions.log)
- [Quality log](test-results/docstrings-d126-2026-09-22/quality.log)
- [Inventory reconciliation](test-results/docstrings-d126-2026-09-22/reconciliation.json)
- [Documentation diff](test-results/docstrings-d126-2026-09-22/docstrings.diff)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
