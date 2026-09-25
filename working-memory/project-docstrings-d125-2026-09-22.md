# Custom-column macro docstrings: D125 complete

The next-up request authorized **D125: 39 declarations** across
SQL/macros/cc_macros_mixin/__init__.py (36) and cc_ensure_values_mixin.py (3).
Both complete files are reviewed and promoted. **D126 is next**: all 11 declarations
in cc_macros_mixin/cc_management_macros.py. Earlier broad authorizations remain paused.

Reviewed legacy reads, dirty-cache schema drift, link/value writes, bulk tag editing,
cleanup and insert-then-search value lookup. Documented actual row/scalar return shapes,
connection-dependent commit behavior, truthiness-based deletion, unsupported connection
overrides, default bulk-binding differences and the cleanup helper's inconsistent
column prefix. The ensure helper can insert duplicates while returning an older ID
when no uniqueness constraint exists. These implementation behaviors remain unchanged.

Verification:

- Complete-file audit and normalizer checks pass for all 39 declarations. Executable
  AST, comments, signatures and non-docstring literals are unchanged; no added Ruff
  findings or whitespace errors. Original baseline hashes retained.
- **Two macro API contract checks passed**, no skips. No dedicated runtime selector
  for most selected legacy methods was found: nearby custom-column database contracts
  exercise different owners, and the old cache bootstrap suite is gated off.
- **81 runnable example statements passed**, checking selected behavior directly
  with pure naming helpers, explicit lightweight hosts and closed in-memory SQLite
  connections, including the real SQLite get extension. Examples verify row/scalar
  results, dirty-queue filtering, link writes, commit forwarding, falsey deletion,
  explicit bulk deletion, unsupported overrides, cleanup SQL failure and duplicate
  insertion during ensure. Other usage examples are not counted as executed.
  These results do not represent a complete legacy application or live PostgreSQL run.
- Full quality runner passed: 156 formatted files, 454 annotation-checked files,
  215 protected modules, zero basedpyright errors, mypy 182 files, and both
  contract checkers rejecting all 37 invalid examples. All checks used the final hashes.

Coverage: **848/2,671 complete files**, **12,320 declarations in complete files**,
plus **37 partial-file declarations**. Remaining: **1,823 files and 29,804
declarations**. D progress: **125/232 units**, **3,092/5,685 declarations**,
**162/374 complete files**. Across all tracks: **157 documentation units verified**,
**1,215 remaining**. No commit or publication requested. Token usage unavailable.

- [Exact observations and commands](test-results/docstrings-d125-2026-09-22/observations.json)
- [Static proof](test-results/docstrings-d125-2026-09-22/static.json)
- [Examples](test-results/docstrings-d125-2026-09-22/examples.json)
- [API contract log](test-results/docstrings-d125-2026-09-22/regressions.log)
- [Quality log](test-results/docstrings-d125-2026-09-22/quality.log)
- [Inventory reconciliation](test-results/docstrings-d125-2026-09-22/reconciliation.json)
- [Documentation diff](test-results/docstrings-d125-2026-09-22/docstrings.diff)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
