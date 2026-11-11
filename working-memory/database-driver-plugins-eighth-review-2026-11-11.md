# Eighth driver-plugin review — 2026-11-11

Follow-up: both findings are repaired; all 1,327 required cases and final quality
checks pass. The private PostgreSQL server is stopped and removed.
Read the [fixes handoff](database-driver-plugins-eighth-fixes-2026-11-11.md).
The original review notes below are historical.

User requested another review. Continue in persistent isolated clone
`/mnt/c/dev/LiuXin-alpha/.tmp/database-driver-review-2026-11-10`, branch
`database-driver-plugins-review-2026-11-05`, HEAD `bab2b682` plus the 23 staged
sixth/seventh-fix files. The origin points to the original local checkout, not
GitHub. No commit, push or merge requested. No implementation changes this turn.

Read [the review](../dev-docs/databases/database-driver-plugins-eighth-review-2026-11-11.md).
Two new open findings, both medium/P2:

- DDP29: connection options can set standard_conforming_strings=off, but the
  lexer still treats ordinary strings as standard-conforming. Literal backticks
  are rewritten to quotes; an independent reader verifies changed committed
  values. Explicit E-strings and bound values are passing controls. Default on
  is unaffected by this repro. Honor or enforce/refuse the lexical precondition.
- DDP28: expression positions after FOR, LEADING, TRAILING, ROWS, VARIADIC and
  DISTINCT ON (...) lose qmark bindings. Six real SELECTs fail through the
  registered driver; the same driver's native %s controls pass. SUBSTRING batch
  insertion reproduces too, without partial writes. Recognize context rather
  than adding unreserved words globally and breaking bare JSON column names.

Existing focused suite: **945 passed, zero skips**, 177.04 seconds. The eight
modules cover the previous fixes, SQLite restore/safety, live PG adaptation,
value parity, completion, adapter and registration. Native SQLite export/replay
checked 10,000 finite float values with zero mismatches; five FTS5 declaration
variants retain row ID/MATCH results. No new permanent tests or manifest changes.
Focused resources: 82.0 MiB peak tree RSS, at least 6.70 GiB available memory,
zero OOM kills. The prior full 1,180-case gate/quality/doctest/integration results
remain valid evidence for the unchanged source; no full gate or remote CI rerun.

Documentation link/navigation and public-boundary checks: **22 passed** in
189.92 seconds. Staged/unstaged whitespace checks pass. All 23 staged files match
their review-start hashes; existing source/tests/configuration are unchanged.
The original checkout's three protected preference/source files, old worktree
index, recovery patch and all 19 recovered files match their saved hashes.
All checks are finished; no processes remain to resume.

Evidence: `/mnt/c/dev/LiuXin-alpha/.tmp/driver-eighth-review-2026-11-11/`.
The report lists the scripts, reproduction outputs and source hashes. Private
PG16 `/tmp/liuxin-driver-eighth-review-pg` is STOPPED AND REMOVED; cleanup receipt
and retained server log are in that directory. All live-engine tests/probes have
finished. The enlarged private lock pool avoided the previous cleanup failures.

Review/navigation/handoff docs only. Existing 23-file staging remains unchanged;
new review docs are intent-to-add for tracked-link checks. Preserve the original
checkout's source/preferences/data, old worktree staging index and persistent
WSL recovery backup. Its working-memory index points here. No agents or skills.
