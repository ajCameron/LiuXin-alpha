# Portable SQL macro docstrings: D122 complete

The onwards request authorized D122 under the one-module contract:
**40 selected declarations** in SQL/macros/portable_macros_mixin.py. The file
remains partial, with **40/76 declarations verified**. **D123 is next** and owns
the remaining 36 declarations. Earlier broad authorizations remain paused.

Reviewed identifier/value helpers, driver and connection selection, temporary
SQL names/types, transaction nesting, row CRUD, link validation, bulk reads and
upserts. Documented connection ownership, outer commit/rollback, cache invalidation,
link identities, static/live type restrictions, partial updates and concurrent
insert behavior. Nested transactions share the outer connection and do not add
independent rollback boundaries.

Verification:

- All 40 selected declarations pass the audit and normalizer checks. Executable
  AST, comments, signatures and non-docstring literals are unchanged. The 36
  D123 docstrings are unchanged; no added Ruff findings or whitespace errors.
- **Seven focused regressions passed**, no skips: portable link writes, bulk
  reads, atomic/scoped replacement, type restrictions and real-facade row CRUD
  with nested rollback. Link cases use SQLite and a PostgreSQL-shaped SQLite
  adapter; these results do not represent a live PostgreSQL run.
- **47 runnable example statements passed**, covering pure helpers, explicit
  lightweight hosts and an in-memory SQLite nested transaction/rollback example.
  Other usage examples are explanatory and are not counted as executed.
- Full quality runner passed: 156 formatted files, 454 annotation-checked files,
  215 protected modules, zero basedpyright errors, mypy 182 files, and both
  contract checkers rejecting all 37 invalid examples.

One initial example expected the public exception alias in a traceback, although
Python reports its underlying class name and quoted message. Corrected the example
to catch the public alias; all examples and final static checks then passed.
Also clarified exact schema-name mapping keys in three parameter descriptions.
Regression and quality results are retained because these final edits only affect
docstrings and the executable AST remains unchanged. Initial example evidence is
preserved alongside the successful result.

Coverage: **844/2,671 complete files**, **12,200 declarations in complete files**,
plus **77 partial-file declarations**. Remaining: **1,827 files and 29,884
declarations**. D progress: **122/232 units**, **3,012/5,685 declarations**,
**158/374 complete files**. Across all tracks: **154 documentation units verified**,
**1,218 remaining**. No files promoted this tranche. No commit or publication
requested. Token usage unavailable.

- [Exact observations and commands](test-results/docstrings-d122-2026-09-21/observations.json)
- [Static proof](test-results/docstrings-d122-2026-09-21/static.json)
- [Examples](test-results/docstrings-d122-2026-09-21/examples.json)
- [Regression log](test-results/docstrings-d122-2026-09-21/regressions.log)
- [Quality log](test-results/docstrings-d122-2026-09-21/quality.log)
- [Inventory reconciliation](test-results/docstrings-d122-2026-09-21/reconciliation.json)
- [Documentation diff](test-results/docstrings-d122-2026-09-21/docstrings.diff)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
