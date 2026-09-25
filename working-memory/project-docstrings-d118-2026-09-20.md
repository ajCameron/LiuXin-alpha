# SQL column-policy docstrings: D118 complete

Resumed from working memory on 2026-09-20 in `codex/project-docstrings`.
The unnumbered resume request used the established one-module contract:
**D118, 40 declarations** in SQL/databasedriver/value_casting_mixin.py.
Earlier broad authorizations remain paused. **D119 is next** on a new request.

Reviewed link capability discovery, declared types, column-policy catalog reads
and writes, normalized identities, field accessors and serialization helpers.
Descriptions cover legacy fallbacks, committed writes, immutable identity rules,
validation errors, frozen options and the mutable declared-type cache. Corrected
the module introduction: declared numeric affinities do parse numeric strings.

Verification:

- All 40 selected declarations pass the auditor and normalizer. Executable AST,
  signatures, non-docstring literals and comments are unchanged; no added Ruff
  findings or whitespace errors. Original baseline hash retained.
- **12 focused SQLite regressions passed**, six unrelated tests deselected,
  no skips. Coverage includes catalog round-trips, typed field accessors,
  normalized identity restrictions, link capabilities and declared-type caching.
- **10 runnable examples passed**. Other examples explain usage with an existing
  driver; these are not counted as executed doctests.
- Full configured quality runner passed: 156 formatted files, 454 files with
  annotation coverage, 215 protected modules, basedpyright zero errors, mypy
  182 files, and both contract checkers rejecting all 37 invalid examples.
- Final whitespace and exception-description corrections changed no executable
  code or runnable examples. Final static checks were rerun; successful runtime
  and quality evidence was reused.

The file remains **partial: 40/44 declarations reviewed**. D119 still owns
`_sqlite_affinity`, `_coerce_db_value`, `_coerce_untyped_value` and `_row_to_dict`;
their docstrings are unchanged. Do not promote the file until that selection and
the complete-file audit/normalizer checks pass.

Project coverage: **840/2,671 complete files**, **12,120 declarations in complete
files**, plus **77 verified partial-file declarations**. Remaining: **1,831 files
and 29,964 declarations**. D progress: **118/232 units**, **2,932/5,685 declarations**,
**154/374 complete files**. Across all tracks, **150 documentation units verified**,
**1,222 remaining**.

Existing SQL utility documentation, storage/adaptor changes, the data submodule
and architecture review were preserved. No commit or publication requested.
Token usage is unavailable in this session.

- [Exact observations and commands](test-results/docstrings-d118-2026-09-20/observations.json)
- [Static checks and unchanged D119 proof](test-results/docstrings-d118-2026-09-20/static.json)
- [Runnable examples](test-results/docstrings-d118-2026-09-20/examples.json)
- [Regression log](test-results/docstrings-d118-2026-09-20/regressions.log)
- [Quality log](test-results/docstrings-d118-2026-09-20/quality.log)
- [Inventory reconciliation](test-results/docstrings-d118-2026-09-20/reconciliation.json)
- [Final documentation diff](test-results/docstrings-d118-2026-09-20/docstrings.diff)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
