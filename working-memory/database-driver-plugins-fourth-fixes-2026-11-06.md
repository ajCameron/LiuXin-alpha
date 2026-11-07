# Fourth driver review repairs — 2026-11-06

User goal: fix DDP17–DDP19 and nearby issues that clearly need repair. All three
are implemented and locally verified. This handoff precedes commit/push to the
existing PR #156; inspect remote state before repeating publication.

Worktree /tmp/liuxin-databases-typing; branch database-driver-plugins-review-2026-11-05.
Starting head 11b6f320301b2a8f4631dfb9659c70cc50cf6e38; PR base database-review.
Preserve the original checkout's accumulated source/preferences/data. No skills
or subagents used; no applicable AGENTS found.

Implemented:
- SQLite classifier retains all SQLite non-ASCII identifier characters, combining
  marks, emoji, NBSP names and dollar continuations, and recognizes BOMs and
  leading empty statements. Keep unknown tokens instead of shifting CTE parsing.
  Shared driver and maintenance behavior: begin writes, complete RETURNING,
  preserve streaming reads, reject REPLACE and run prompt work transactionally.
- PostgreSQL temporal literals use grammar context plus actual binding arity.
  Includes TIME/INTERVAL/DATE/TIMESTAMP and aliases, precision, zone modifiers,
  fields, clauses and aliases. Preserve same-named JSON operands. Multiple
  unresolved assignments fail before execution with native-parameter/CAST advice.
  Executemany peeks one row to establish arity, chains it back and streams once.
- PostgreSQL fake returns nine physical index fields, covers partial/expression/
  invalid/nonunique exclusions and INCLUDE separation, checks schema/table scope.
- Full old-head CI completed: 8763 passed, 2 failed, 1078 skipped, 15 xfailed.
  The second failure was missing optional APSW guard in the full-catalogue CTE
  test. Added the guard; absent-module probe skips once. Mandatory gate still
  installs APSW and requires the same node to pass, so no coverage is weakened.

New required cases: 70 SQLite, 33 live PG, 1 formerly failing fixture. Manifest
grows 548 -> 652 without removed/reassigned cases. Final gate: 652 fully passed
setup/call/teardown, zero failures/skips/xfails; selected files 683 passed in
237.57 s. Baseline comparison against 11b6f320 passes.

Final confidence: 313 passed, 3 skipped, 86 warnings in
240.90 s. Skips: two fixture views with no usable ID rows, and CSS-utils-absent
case when CSS-utils is installed. All 121 doctests pass. Documentation/ratchet
tooling: 43 passed in 22.25 s. Full quality passes: 310 format files, 463 annotation
sources, 255 cycle modules, 1903 layer modules, 25 cold imports, lint/complexity,
basedpyright zero errors/warnings, mypy 457 sources and 95/93 negative contracts.

Initial focused run: 415 passed and one new test used nonexistent PG in_transaction;
fixed the assertion to shared transaction_active. The temporal rerun passed 33.
Initial confidence run: 287 passed, 1 failed, 3 skipped, 25 errors because another
concurrent default-root pytest session deleted /tmp/pytest-of-blackjane/pytest-0.
Verified installed pytest skips cleanup locks with retention none. The full
confidence rerun used a unique explicit --basetemp and passed. Use distinct
explicit basetemps for every concurrent run; do not repeat the shared-root mistake.

Evidence prefix /tmp/liuxin-driver-fourth-fixes-: focused.log, temporal.log,
ratchet/ratchet.json + telemetry, ratchet.log, confidence-final.log + telemetry,
quality-final.log, docs-gate.log, doctests.log, no-apsw.log, ci-full-raw.log.
All runtime config/prefs/scratch are isolated; original data stays untouched.
PG16 is stopped/removed (pg.json receipt and pg-server.log retained).

Review and validation: dev-docs/databases/database-driver-plugins-fourth-review-2026-11-06.md.
Runbook now documents temporal ambiguity and batch parameter shape. README and
next_tasks reflect repairs. No full local suite/production import signoff claimed.
Fresh PR checks must complete before merge; no merge is authorized or performed.
