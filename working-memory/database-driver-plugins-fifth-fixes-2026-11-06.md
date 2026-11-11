# Fifth driver review mitigations — 2026-11-06

User goal: mitigate the three findings in the fifth review as far as possible.
DDP20–DDP22 are implemented and locally verified. No new commit, push or merge.
This records the original mitigation turn. On **2026-11-07**, the user authorized
committing and pushing the staged work to the existing PR #156 branch.

Worktree: /tmp/liuxin-databases-typing.
Branch: database-driver-plugins-review-2026-11-05.
Starting/published head: 47e089d4fd6179444d687922f7649ef1a7910a88.
PR #156, base database-review. The published head was fully green before these
changes. Fresh CI remains a publication/merge requirement.

Implemented:
- DDP20: shared statement_keyword/has_multiple_statements use the existing
  backend token streams. SQLite row recognition and script preflight share
  SQLite comments/whitespace/BOM handling. PostgreSQL completion, dependent
  reads, handler connections and upgrade callbacks use its nested-comment and
  native-string lexer. Dollar/escaped strings are data; chained statements
  remain refused. The portable handler INSERT OR REPLACE refusal is preserved.
- DDP21: SQLiteCompletionCursor registers weakly with its connection; close()
  finalizes owned cursors before closing the native handle. Includes custom
  cursor factories without retaining abandoned readers. Both dump/rebuild
  routes reject failed closes and surviving WAL/SHM/journals immediately before
  publication, reusing creation.require_no_sqlite_sidecars. No live journal
  is unlinked. Original file survives refusal, reopen is attempted, retry works.
  Caller must still coordinate exclusive access; this is not an interprocess
  lease. Pending writes remain refused, and close still rolls them back.
- DDP22: OPERATOR([schema.]symbol) is opaque to binding classification; nested
  identifier translation still handles backtick schema names. Forty live
  comparisons cover four JSON operators, both binding styles and five forms.

Added 156 mandatory cases:
96 driver-maintenance-completion, 18 driver-sqlite-restore-validation,
2 driver-resource-lifetime, 40 driver-postgres-sql-adaptation.
Manifest 652 -> 808; no removed or reassigned prior cases.
New completion test module is included in formatting and lint.

Final evidence:
- Mandatory ratchet: required_cases=808, fully_passed=808, failures=[],
  pytest_exit_code=0. Selected files: 839 passed in 313.08 s.
- Expanded integration: 460 passed, 29 backend/setting-specific skips in 82.21 s.
- Confidence: 313 passed, 3 skipped, exit zero; telemetry report counts prove
  this because the extra quiet option suppressed pytest's numeric summary.
  Skips: 2 fixture views without usable ID rows, 1 cssutils-absent case with
  cssutils installed. Elapsed monotonic time 290.42 s; wall clock jumped during
  this run, so do not derive duration from its UTC timestamps.
- 121 native driver/adapter doctests passed.
- 58 documentation/link/runner checks passed in 23.97 s.
- Final full quality runner exited zero: 311 format files, 463 annotation
  sources, 255 cycle modules, 1903 layer modules, 25 import budgets, all lint
  and complexity checks; basedpyright 0 errors/warnings; mypy 457 sources;
  95/93 invalid-call probes rejected.

Initial focused run: 394 passes, 1 failure, 10 skips. Restored the explicit
unsupported-REPLACE handler refusal; expanded rerun passed. First quality run
had successful individual checks but a shell parse/read error because its file
was edited while running. bash -n and the full final rerun pass. Do not edit an
active shell runner. Concurrent pytest runs used distinct explicit basetemps;
all mypy caches were outside the checkout.

Evidence prefix: /tmp/liuxin-driver-fifth-fixes-.
Key artifacts: ratchet/ratchet.json, ratchet.log and telemetry;
confidence.log and confidence-telemetry; focused-final.log; quality-final.log;
tooling.log; doctests.log; restore-probe.log; probe.log; pg.json and pg-server.log.
Private PostgreSQL 16 is STOPPED AND REMOVED.

Review/implementation/validation: dev-docs/databases/database-driver-plugins-fifth-review-2026-11-06.md.
Operational docs updated: creation-and-replacement, maintenance-modes,
postgresql-backend, regression-ratchet, README and next_tasks.

Everything is staged in the isolated review worktree. Preserve the original
/mnt/c/dev/liuxin-alpha accumulated source/preferences/data; only its memory
pointer was updated. No skills or subagents used. The full local suite and a
production import trial were not run.
