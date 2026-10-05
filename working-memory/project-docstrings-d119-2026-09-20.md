# Value-casting docstrings: D119 complete

The next-tranche request authorized D119 under the one-module contract:
the four remaining methods in SQL/databasedriver/value_casting_mixin.py.
**The whole file is now reviewed: 44 declarations across D118 and D119.**
Next: **D120**, SQL/databasedriver/view_mixin.py (four declarations), on request.
Earlier broad authorizations remain paused; no commit or publication requested.

Documented `_sqlite_affinity`, `_coerce_db_value`, `_coerce_untyped_value` and
`_row_to_dict`. Source review confirmed that `force_unicode` aliases `str`, so
byte inputs outside BLOB handling are stringified, not decoded. The docs also
explain NUMERIC fallback for empty declarations, fractional values in INTEGER
conversion, numeric-text overflow, untyped boolean preservation, shared set
cells, duplicate headings, extra/short rows and unsupported set keys.

Verification:

- Complete-file audit and normalizer pass for all 44 declarations. Executable
  AST, comments, signatures and non-docstring literals are unchanged; all 40
  D118 docstrings are unchanged. No added Ruff findings or whitespace errors.
- **36 runnable examples passed**, including **26 new example statements**.
- **Two SQLite regressions passed**, no skips: row fetch/update round-trip and
  all-rows/iterator consistency. D118's 12 metadata regressions remain supported
  by unchanged source and are not counted as newly executed tests.
- Full quality runner passed: 156 formatted files, 454 annotation-checked files,
  215 protected modules, zero basedpyright errors, mypy 182 files, and both
  contract checkers rejecting all 37 invalid examples.

The file entered the reviewed manifest only after both selections and all
complete-file checks passed. Baseline hashes and other inventory unit/file
records were preserved.

Coverage: **841/2,671 complete files**, **12,164 declarations in complete files**,
plus **37 partial-file declarations**. Remaining: **1,830 files and 29,960
declarations**. D progress: **119/232 units**, **2,936/5,685 declarations**,
**155/374 complete files**. Across all tracks: **151 documentation units verified**,
**1,221 remaining**. Token usage is unavailable in this session.

- [Exact observations and commands](test-results/docstrings-d119-2026-09-20/observations.json)
- [Static proof and complete-file checks](test-results/docstrings-d119-2026-09-20/static.json)
- [Examples](test-results/docstrings-d119-2026-09-20/examples.json)
- [Regression log](test-results/docstrings-d119-2026-09-20/regressions.log)
- [Quality log](test-results/docstrings-d119-2026-09-20/quality.log)
- [Inventory reconciliation](test-results/docstrings-d119-2026-09-20/reconciliation.json)
- [Documentation diff](test-results/docstrings-d119-2026-09-20/docstrings.diff)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
