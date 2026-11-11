# Fourth driver-plugin review — 2026-11-06

User asked whether another review was worthwhile or we were happy. Reviewed
PR #156 head 11b6f320301b2a8f4631dfb9659c70cc50cf6e38, branch
database-driver-plugins-review-2026-11-05, in /tmp/liuxin-databases-typing.
Base remains database-review. The earlier repair goal is complete; this is a new
review-only turn, with no production/test/manifest edits or publication.

Review: dev-docs/databases/database-driver-plugins-fourth-review-2026-11-06.md.
Three open findings:

- DDP17 high: SQLite classifier drops unquoted non-ASCII CTE name 名. Idle
  direct_execute skips BEGIN and RETURNING buffering. A real RAISE(FAIL) trigger
  on the second updated row leaves the first change durable despite the reported
  error; source and quoted "名" controls roll back. RETURNING commits only at
  cursor close for unquoted 名. Both SQLite names reproduce. Fresh maintained
  WEMI connections fail closed with Required prompt maintenance is unavailable;
  no demonstrated prompt bypass. Unconditional executemany BEGIN remains effective.
- DDP18 medium: PostgreSQL typed-literal helper's suffix whitelist omits DAY,
  DAY TO SECOND, WHERE and OFFSET. Four valid TIME/INTERVAL qmark cases fail;
  raw native %s controls all succeed on PG16. Bare SELECT INTERVAL ? works.
  Consider deliberate parameter-style policy instead of endlessly growing the
  keyword heuristic; preserve unqualified JSON operand regressions.
- DDP19 medium: CI confidence test fake returns old 1-column pg_index tuples;
  physical reader now consumes 9 columns. IndexError row[7]. Same test still
  asserts old partial/expression query filters. Update fake shape and behavioral
  assertions, retain real PG coverage, run full confidence stream (CI maxfail=1).
  Locally reproduced exactly: 1 failed in 1.46 s. CI: 1 failed/94 passed/1 skipped.

At 02:10 UTC, PR full suite still running; confidence failed; mandatory ratchet,
typing, wheel, fast/heavy file-format checks passed. Earlier local 548 mandatory
passes are historical validation, not a new rerun or coverage of these inputs.
No full local suite/quality rerun for this documentation-only review.
Documentation checks: 22 passed in 8.74 s; staged whitespace check passed. Five
review/navigation/handoff files are staged, without a new commit or push.

Evidence prefix /tmp/liuxin-driver-fourth-review-:
sqlite.py/.log (real driver durability and installed-maintenance probes),
postgres.py/.log (native/adapted comparison), ci-confidence.log (downloaded job),
confidence-local.log, pg.json, pg-server.log. Scripts isolate config/prefs and
both ptempfiles scratch roots. Disposable PG16 cluster is STOPPED AND REMOVED.
No running database service remains from this review.

Preserve the original /mnt/c/dev/liuxin-alpha accumulated source/preferences/data.
Only add a current working-memory pointer there. Continue work in the isolated
review checkout. New findings are documented, not repaired or in permanent tests.
