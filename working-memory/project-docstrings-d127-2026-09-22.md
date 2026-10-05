# SQLite initializer docstrings: D127 complete

The explicit next-tranche request authorized **D127: two declarations** in
SQLite/__init__.py. The complete file is reviewed and promoted. **D128 is next**:
15 declarations in SQLite/databasedriver/__init__.py. Earlier broad authorizations
remain paused.

Documented the plugin version export, concrete-driver import location, and lazy
component-version imports. The helper's historical formatting is now explicit:
the database and driver labels are swapped, the application-constants version is
read but omitted, and tuple punctuation is retained with commas and spaces each
replaced by underscores. Existing behavior is unchanged.

Verification:

- Both declarations pass complete-file audit and normalizer checks. Executable
  AST, comments, signatures and non-docstring literals are unchanged; no added
  Ruff findings or whitespace errors. Original baseline hash retained.
- **One regression passed**, no skips: importing the pure SQLite driver with
  APSW blocked. This traverses the edited package initializer.
- **Nine runnable example statements passed** using actual version dependencies
  and isolated preferences/config directories. Examples cover the version export,
  component ordering, suffix and replacement rules. No database is constructed.
- Full configured quality runner passed. Exact commands, terminal exits and
  final source hashes are retained in the evidence below.

Coverage: **850/2,671 complete files**, **12,333 declarations in complete files**,
plus **37 partial-file declarations**. Remaining: **1,821 files and 29,791
declarations**. D progress: **127/232 units**, **3,105/5,685 declarations**,
**164/374 complete files**. Across all tracks: **159 documentation units verified**,
**1,213 remaining**. No commit or publication requested. Token usage unavailable.

- [Exact observations and commands](test-results/docstrings-d127-2026-09-22/observations.json)
- [Static proof](test-results/docstrings-d127-2026-09-22/static.json)
- [Examples](test-results/docstrings-d127-2026-09-22/examples.json)
- [Regression log](test-results/docstrings-d127-2026-09-22/regressions.log)
- [Quality log](test-results/docstrings-d127-2026-09-22/quality.log)
- [Inventory reconciliation](test-results/docstrings-d127-2026-09-22/reconciliation.json)
- [Documentation diff](test-results/docstrings-d127-2026-09-22/docstrings.diff)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
