# SQL macro docstrings: D121 complete

The next-tranche request authorized D121 under the one-module contract:
**32 declarations** across SQL/macros/__init__.py (29) and
SQL/macros/hash_tables_macros_mixin.py (3). Both files are now reviewed and
promoted. **D122 is next**: the first 40-declaration selection in
SQL/macros/portable_macros_mixin.py. Earlier broad authorizations remain paused.

Reviewed facade forwarding, generic link writes, conditional reads, scalar and
bulk updates/deletes, typed/priority link containers, path replacements, legacy
library/version markers and MD5 fingerprint delegation. Corrected misleading
descriptions of reversed no-priority link bindings, three-value bulk repointing,
TypeError-only lookup defaults and result-container shapes. Documented ignored
arguments and wrapper-owned transaction behavior without changing implementation.

Verification:

- Both complete-file audits and normalizer checks pass for all 32 declarations.
  Executable AST, comments, signatures and non-docstring literals are unchanged;
  no added Ruff findings or whitespace errors. Original baseline hashes retained.
- **Three focused regressions passed**, no skips: SQLite fingerprint/hash alias,
  direct update/bulk priority/version behavior, and folder-store path replacement.
- **28 runnable example statements passed**, including reversed link insertion,
  empty conditional lookup, the ignored trigger argument and all four single-row
  link-result modes. Some examples use explicit stand-in facade/readers; these
  check conversion/forwarding behavior, not database integration. Other usage
  examples remain explanatory and are not counted as executed.
- Full quality runner passed: 156 formatted files, 454 annotation-checked files,
  215 protected modules, zero basedpyright errors, mypy 182 files, and both
  contract checkers rejecting all 37 invalid examples.

Coverage: **844/2,671 complete files**, **12,200 declarations in complete files**,
plus **37 partial-file declarations**. Remaining: **1,827 files and 29,924
declarations**. D progress: **121/232 units**, **2,972/5,685 declarations**,
**158/374 complete files**. Across all tracks: **153 documentation units verified**,
**1,219 remaining**. No commit or publication requested. Token usage unavailable.

- [Exact observations and commands](test-results/docstrings-d121-2026-09-20/observations.json)
- [Static proof](test-results/docstrings-d121-2026-09-20/static.json)
- [Examples](test-results/docstrings-d121-2026-09-20/examples.json)
- [Regression log](test-results/docstrings-d121-2026-09-20/regressions.log)
- [Quality log](test-results/docstrings-d121-2026-09-20/quality.log)
- [Inventory reconciliation](test-results/docstrings-d121-2026-09-20/reconciliation.json)
- [Documentation diff](test-results/docstrings-d121-2026-09-20/docstrings.diff)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
