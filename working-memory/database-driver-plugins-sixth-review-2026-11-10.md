# Sixth driver-plugin review — 2026-11-10

User: "Okay then - database_driver_plugins time?" Continued the review on
PR #156 head bab2b682cfa642fd3ecd431c1c395fbac0ecad00, branch
database-driver-plugins-review-2026-11-05, base database-review.
Worktree: /tmp/liuxin-databases-typing. PR is OPEN; all applicable CI is GREEN
at the exact reviewed commit, including full suite, confidence, architecture/
typing, installed wheel and mandatory database ratchet. No merge.

Review: dev-docs/databases/database-driver-plugins-sixth-review-2026-11-10.md.
Two findings remain open; this turn changes review/navigation/handoff docs only.
No production edits, new permanent tests or required-manifest changes.

- DDP23 high: stdlib iterdump truncates TEXT containing an embedded NUL.
  Both SQLite names' public sql_dump replay "before\x00after" as "before".
  SQLite.dump_and_restore returns success and replaces the original with
  the truncated value, while integrity_check still returns ok. BLOB bytes
  survive. SQLite_apsw's native shell rebuild preserves the same TEXT.
  Verified direct_backup preserves the TEXT on both driver names.
  Fix lossless serialization or refuse before publication; do not strip NULs.
- DDP24 medium: PostgreSQL's SQL lexer uses Python \w and str.isspace,
  missing legal combining/emoji/NBSP identifiers and dollar tags.
  Native PG16 accepts all three identifier classes in JSON ? expressions.
  Adapter native parameters get a false mixed-bindings error; portable
  parameters get bound at the operator and fail server syntax.
  Unrecognized dollar tags expose their contents to backtick translation:
  the SELECT succeeds but its literal backticks become double quotes.
  This probe demonstrates returned-value changes, not a persisted PG write.
  Ordinary ASCII and CJK controls work. Quoted identifiers, ASCII/empty
  dollar tags and bound-value workarounds are independently verified.

Evidence: /tmp/liuxin-driver-sixth-review-probe.py and probe.log;
focused.log and telemetry; pg.json and pg-server.log. Prefix on all artifacts:
/tmp/liuxin-driver-sixth-review-.
Probe process uses tests.conftest._redirect_liuxin_runtime_dirs with owned
TemporaryDirectory and MonkeyPatch, so preferences/scratch stay under /tmp.
Native readers/comparisons establish actual stored/returned values.
Private PostgreSQL 16 is STOPPED AND REMOVED.

Focused existing regression selection: 82 passed, 476 deselected, no skips,
24.40 seconds. No new local full suite/quality/808-case run; current remote
checks already pass on unchanged production/test sources. A temporary probe
edit initially misplaced a control block (IndentationError); it was corrected
and the complete final probe exited zero with all findings and controls.
All 22 documentation link/boundary checks passed in 19.52 seconds. The five
review/navigation/handoff files are staged and pass the whitespace check.

Preserve original /mnt/c/dev/liuxin-alpha accumulated source/preferences/data.
Only its current memory pointer should change. Continue in the isolated
worktree. No commit/push in this review turn. Do not create a goal for an
ordinary review and do not spawn agents without explicit authorization.
