# SQL view docstrings: D120 complete

The proceed request authorized D120 under the one-module contract:
**all four declarations in SQL/databasedriver/view_mixin.py**. The file is now
reviewed and promoted. **D121 is next**: SQL/macros package and hash-table macro
mixin, 32 declarations. Earlier broad authorizations remain paused.

Documented the host dependencies, required literal `id` column, parameter-bound
row IDs, typed row conversion, missing and duplicate matches, trusted relation
names, and PRAGMA heading behavior. The docs describe existing cleanup limits:
the headings helper does not explicitly close its connection, and row lookup has
no finally cleanup for query/conversion errors. No behavior was changed.

Verification:

- All four declarations pass the complete-file audit and normalizer. Executable
  AST, comments, signatures and non-docstring literals are unchanged. No added
  Ruff findings or whitespace errors; original baseline hash preserved.
- **One focused SQLite regression passed**, no skips. It creates a real view
  with an `id` alias and checks headings and typed row contents. Unrelated trigger
  tests were excluded after source review.
- **Nine runnable example statements passed**, checking in-memory SQLite view
  headings, a missing relation, and the connection remaining open after lookup.
  The class and row-method examples are explanatory, not additional doctests.
- Full quality runner passed: 156 formatted files, 454 annotation-checked files,
  215 protected modules, zero basedpyright errors, mypy 182 files, and both
  contract checkers rejecting all 37 invalid examples.

Coverage: **842/2,671 complete files**, **12,168 declarations in complete files**,
plus **37 partial-file declarations**. Remaining: **1,829 files and 29,956
declarations**. D progress: **120/232 units**, **2,940/5,685 declarations**,
**156/374 complete files**. Across all tracks: **152 documentation units verified**,
**1,220 remaining**. No commit or publication requested. Token usage unavailable.

- [Exact observations and commands](test-results/docstrings-d120-2026-09-20/observations.json)
- [Static proof](test-results/docstrings-d120-2026-09-20/static.json)
- [Examples](test-results/docstrings-d120-2026-09-20/examples.json)
- [Regression log](test-results/docstrings-d120-2026-09-20/regressions.log)
- [Quality log](test-results/docstrings-d120-2026-09-20/quality.log)
- [Inventory reconciliation](test-results/docstrings-d120-2026-09-20/reconciliation.json)
- [Documentation diff](test-results/docstrings-d120-2026-09-20/docstrings.diff)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
