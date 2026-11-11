# Third driver-plugin review — 2026-11-06

User requested another review after DDP07–DDP10 were fixed. This turn reviews the
current staged tree and adds documentation only. No production/test/manifest
changes, commit or publication. Continue in `/tmp/liuxin-databases-typing`, branch
`database-driver-plugins-review-2026-11-05`, based on `07fc3326` (PR #155 into
database-review). Preserve the earlier staged fixes and original checkout/data.

Review: `dev-docs/databases/database-driver-plugins-third-review-2026-11-06.md`.
Six confirmed open findings:

1. DDP11 high: with an idle SQLite primary and no installed maintenance boundary,
   direct_executemany on WITH ... INSERT never starts a transaction. A generator
   yields one row then raises; DatabaseDriverError is reported but an independent
   reader sees the committed prefix. Both driver names and full WEMI wrappers
   reproduce it. Plain INSERT and explicit-BEGIN controls roll back correctly.
   This qualifies DDP07; no retry occurs and failure is no longer silent. Installed
   maintenance already has an explicit BEGIN workaround, but drivers cannot rely
   on that optional boundary for their own batch guarantee.
2. DDP12 high: PostgreSQL BYTEA reaches SQLite-affinity coercion as memoryview;
   it becomes '<memory at 0x...>'. A current conversion_options row read, changed
   only in format, and written back persists that text over conversion_option_data.
   Confirmed on full PG16 WEMI schema, minimal native table and view; raw reads
   preserve original bytes. Needs backend-aware type preservation and round trips.
3. DDP13 medium: direct_execute exits its transaction before returning the cursor.
   Ordinary INSERT ... RETURNING on both SQLite drivers raises cannot commit
   transaction - SQL statements in progress, rolling back the row. Full wrappers
   reproduce; configured native execute with consumption inside the transaction
   succeeds. Settle result-producing-write ownership without breaking SELECT
   streaming or silently changing ordinary commit semantics.
4. DDP14 medium: direct_delete_book_group opens an owned connection and closes it
   only on success. A real ABORT trigger leaves an active write transaction tracked;
   another connection cannot write an unrelated table. Closing the driver restores
   writing. Both SQLite names reproduce. Public API/tests retain this helper, but
   no application caller found in src; consider retirement or proper cleanup.
5. DDP15 medium: SQLite _get_unique_column_groups only reads index_list, missing
   INTEGER PRIMARY KEY rowid aliases, and rejects a legal quoted index with spaces.
   Physical specs know the constraints, SQLite rejects duplicates, but wrapper
   cardinality is UNKNOWN rather than MANY_TO_ONE. Ordinary unique-index control
   works. Both drivers reproduce. Native/extension layout, not generated-link claim.
6. DDP16 medium: unqualified first ? %s is valid native PostgreSQL SQL but the
   classifier sees FIRST as an expression prefix and falsely reports mixed bindings.
   Raw psycopg2 returns True; payload and quoted "first" controls work in adapter.
   DDP08 qualified-name fix remains correct; this is another input needing a
   deliberate native/portable parameter contract or sufficient parser context.

Prioritize DDP11/DDP12. The review gives precise code locations, controls,
proposed repair boundaries and regression requirements for all six. Structural
cleanup/performance recommendations remain separate; no new implementation is
authorized merely by this review request.

Validation: unchanged mandatory ratchet 400 passed, zero skips/xfails, 129.84 s.
All setup/call/teardown phases passed; baseline comparison 07fc3326. No full-suite
or full-quality rerun (code unchanged), and no production import. All 22 documentation
link/boundary checks passed in 15.04 s; staged diff has no whitespace errors.
New cases are external probes, not yet permanent
regressions. Keep this distinction explicit in any follow-up.

Evidence prefix /tmp/liuxin-driver-third-review-:
sqlite.py/.log, postgres.py/.log, catalogue.py/.log, indexes.py/.log;
ratchet/ratchet.json + telemetry, ratchet.log. Standalone probes isolate config,
prefs and ptempfiles paths. Complete databases use enable_maintenance=False.
No fake connection objects are substituted in SQL/value/locking probes; the
cardinality helper receives a SimpleNamespace holding the real driver.

PG16 at /tmp/liuxin-driver-third-review-pg-8wj8wbus is STOPPED AND REMOVED.
Receipt pg.json and retained pg-server.log use the evidence prefix. All earlier
review clusters are also removed. The root checkout only receives a memory
pointer; its accumulated source/preferences/data remain preserved.
