# Portable SQL macro docstrings: D123 complete

The explicit D123 request authorized the remaining **36 declarations** in
SQL/macros/portable_macros_mixin.py. All **76 declarations** across D122 and D123
are now reviewed, and the file is promoted. **D124 is next**: five declarations
in SQL/macros/temp_tables_macros_mixin.py. Earlier broad authorizations remain paused.

Reviewed link replacement and priority staging, owned destination updates,
policy-aware ensure/find, scoped canonical identities, identity audit/migration,
metadata seeding, temporary tables, orphan pruning and fingerprints. Documented
the distinction between nested macro transactions and connection-level temporary
table contexts, retained destinations after unlinking, supplied-link-only pruning,
and migration index names that include existing indexes requested with IF NOT EXISTS.

Verification:

- Complete-file audit and normalizer checks pass for all 76 declarations.
  Executable AST, comments, signatures, non-docstring literals and all 40 D122
  docstrings are unchanged. No added Ruff findings or whitespace errors.
- **21 regressions passed**, one expected skip and one unrelated legacy selector
  deselected. Coverage includes replacement rollback, owned rows, policy/scoped
  matching, collision/backfill migration, temporary cleanup, pruning, fingerprints
  and real SQLite facade wiring. The PostgreSQL-shaped SQLite temporary-table
  case skips because that harness lacks pg_temp; no live PostgreSQL result is claimed.
- **92 runnable example statements passed** across the whole file, including
  **45 D123 statements**. Examples exercise pure helpers, explicit lightweight
  hosts, and in-memory SQLite transactions, temporary tables and fingerprints.
  Explanatory usage examples are not counted as executed.
- Full quality runner passed: 156 formatted files, 454 annotation-checked files,
  215 protected modules, zero basedpyright errors, mypy 182 files, and both
  contract checkers rejecting all 37 invalid examples.

Final review clarified four parameter descriptions after runtime checks: canonical
lookups do not apply empty-value policy, the private lookup receives an original
display value, and index suffixes describe identity scopes. Examples and executable
code were unchanged; final static checks were rerun, retaining runtime/quality results.

Coverage: **845/2,671 complete files**, **12,276 declarations in complete files**,
plus **37 partial-file declarations**. Remaining: **1,826 files and 29,848
declarations**. D progress: **123/232 units**, **3,048/5,685 declarations**,
**159/374 complete files**. Across all tracks: **155 documentation units verified**,
**1,217 remaining**. No commit or publication requested. Token usage unavailable.

- [Exact observations and commands](test-results/docstrings-d123-2026-09-21/observations.json)
- [Static proof](test-results/docstrings-d123-2026-09-21/static.json)
- [Examples](test-results/docstrings-d123-2026-09-21/examples.json)
- [Regression log](test-results/docstrings-d123-2026-09-21/regressions.log)
- [Quality log](test-results/docstrings-d123-2026-09-21/quality.log)
- [Inventory reconciliation](test-results/docstrings-d123-2026-09-21/reconciliation.json)
- [Documentation diff](test-results/docstrings-d123-2026-09-21/docstrings.diff)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
