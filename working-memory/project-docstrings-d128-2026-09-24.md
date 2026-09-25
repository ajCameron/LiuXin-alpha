# SQLite concrete driver docstrings: D128 complete

The explicit onwards request authorized **D128: 15 declarations** in
SQLite/databasedriver/__init__.py, including the nested regex and fallback progress
callbacks. The complete file is reviewed and promoted. **D129 is next**: two
declarations in SQLite_apsw/__init__.py. Earlier broad authorizations remain paused.

Documented query result shapes, lazy FRBR creation, deferred driver setup,
connection tracking, global adapter registration, maintenance callback binding and
thread dispatch, language seeding, file timestamps and the interactive shell.
Dump documentation covers transaction completion and rollback on generator close;
file rebuild documentation covers prefix SQL, preserved user_version, limited
restore callbacks, pending-write loss, replacement and reopen boundaries. Legacy
OperationalError masking via exception.message is explicit and unchanged.

Verification:

- All 15 declarations pass complete-file audit and normalizer checks. Executable
  AST, comments, signatures and non-docstring literals are unchanged; no added
  Ruff findings or whitespace errors. Original baseline hash retained.
- **Seven regressions passed**, no skips: pure-driver import without APSW,
  dump/restore preserving user_version, SQL dump and prefix-restore contracts,
  schema creation, close/reopen, and foreign-key/UDF setup. Driver selection was
  explicitly SQLite; no live PostgreSQL or APSW backend result is claimed.
- **44 runnable example statements passed** using in-memory connections and
  isolated runtime directories. **Six integration example statements explicitly
  skipped**: schema build, background update, standalone nested-regex usage,
  interactive shell, full file rebuild and its nested fallback callback usage.
  Parent connection examples exercise the actual regex function; regressions cover
  schema creation and file rebuilding. Skipped examples are not counted as run.
- Initial examples missed the progress handler's printed zero in two places.
  Expected output and prose were corrected, and an example of rolling back a
  suspended SQL dump was added. Initial evidence is retained separately; all final
  checks use the final source hash.
- Full configured quality runner passed: 156 formatted files, 454 annotation-
  checked files, 215 protected modules, zero basedpyright errors, mypy 182 files,
  and both contract checkers rejecting all 37 invalid examples.

Coverage: **851/2,671 complete files**, **12,348 declarations in complete files**,
plus **37 partial-file declarations**. Remaining: **1,820 files and 29,776
declarations**. D progress: **128/232 units**, **3,120/5,685 declarations**,
**165/374 complete files**. Across all tracks: **160 documentation units verified**,
**1,212 remaining**. No commit or publication requested. Token usage unavailable.

- [Exact observations and commands](test-results/docstrings-d128-2026-09-24/observations.json)
- [Static proof](test-results/docstrings-d128-2026-09-24/static.json)
- [Examples](test-results/docstrings-d128-2026-09-24/examples.json)
- [Regression log](test-results/docstrings-d128-2026-09-24/regressions.log)
- [Quality log](test-results/docstrings-d128-2026-09-24/quality.log)
- [Inventory reconciliation](test-results/docstrings-d128-2026-09-24/reconciliation.json)
- [Documentation diff](test-results/docstrings-d128-2026-09-24/docstrings.diff)
- [Completion plan](../dev-docs/project-docstrings-completion-plan.md)
