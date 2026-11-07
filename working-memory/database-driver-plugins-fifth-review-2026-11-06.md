# Fifth driver-plugin review — 2026-11-06

User requested another review, then asked to resume after an unavailable-content
message. Reviewed PR #156 head 47e089d4fd6179444d687922f7649ef1a7910a88,
branch database-driver-plugins-review-2026-11-05, base database-review.
Continue in /tmp/liuxin-databases-typing. This is review only: no production,
permanent-test or manifest edits, no new commit/push/merge.

Review: dev-docs/databases/database-driver-plugins-fifth-review-2026-11-06.md.

- DDP20 high: maintenance.bound_connection.statement_keyword has narrower
  whitespace/comment handling than the engines and other existing scanners.
  Installed-maintenance SQLite scripts accept form-feed/BOM-prefixed COMMIT;
  a later duplicate insert fails but the preceding source/effect rows are
  durable, on both SQLite names. Plain COMMIT is rejected before changes.
  PG16 configured SQL form-feed/nested-comment COMMIT commits a source row
  without invoking the required prompt handler. Plain COMMIT invokes it;
  deliberately failing handlers roll back plain COMMIT but are never called
  for the prefixed variants. Independent native readers prove durability.
- DDP21 high: SQLite dump_and_restore with WAL plus an unfinished primary
  SELECT cursor returns success but loses a table added by prefix SQL.
  The candidate contains the table; old WAL survives at rename; reopened
  file loses the table despite integrity_check=ok. in_transaction was False.
  Cursor absence/explicit early close preserve the table; SQLite_apsw also
  preserves it in all tested cases. Do not claim a reproduced APSW defect
  or arbitrary catalogue-row corruption. Refuse or establish quiescent owned
  cursors/WAL before replacement; never simply unlink a live WAL.
- DDP22 medium: PG OPERATOR(pg_catalog.?) is misclassified as a parameter.
  Native psycopg2 SELECT %s::jsonb OPERATOR(pg_catalog.?) %s returns True;
  configured adapter raises mixed-parameter error. Qmark operand form binds
  the value inside the operator name and leaves final ? unchanged, failing
  syntax. Unqualified ? controls succeed with both parameter styles.

All applicable remote checks are GREEN at the reviewed head, including:
full suite 8799 passed / 1147 skipped / 15 xfailed in 2056.13 seconds;
confidence; architecture/typing; wheel; file-format lanes; mandatory database
ratchet; Tests Passed aggregate. The new probes demonstrate missing coverage,
not existing gate failures. No local full-suite/quality rerun this review.
Documentation checks: 22 passed in 11.47 seconds; staged whitespace check passed.
Five review/navigation/handoff files are staged without a commit or push.

Evidence prefix /tmp/liuxin-driver-fifth-review-:
sqlite.py/.log, postgres.py/.log, restore.py,
restore.log, restore-confirm.log, restore-controls.log,
ci-full.log, pg.json, pg-server.log.
All runtime config/preferences/ptempfiles scratch stayed under /tmp.
Private PostgreSQL 16 cluster is STOPPED AND REMOVED.

Prior repair goal remains complete; do not create a goal for an ordinary review.
No subagents permitted unless explicitly requested. Keep tests serial with
explicit unique --basetemp: concurrent pytest retention=none previously erased
another run's default temp directory. Keep runtime scratch off C: (1.6 GiB
free at this review). Shared venv: .venv/bin/python.

Preserve the original /mnt/c/dev/liuxin-alpha accumulated source/preferences/data.
Only add a current working-memory pointer there. New findings are documented,
not repaired or added to the permanent regression manifest.
