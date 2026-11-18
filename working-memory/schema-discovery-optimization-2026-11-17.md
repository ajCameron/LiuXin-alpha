# PostgreSQL schema discovery optimization — 2026-11-17

## Final status — complete

The requested bounded schema-discovery optimization is implemented and verified.
The [implementation record](../dev-docs/databases/schema-discovery-optimization-2026-11-17.md)
and [baseline artifact](../dev-docs/databases/schema-discovery-baseline-2026-11-17.json)
record ownership, freshness/transaction semantics, measurements and limits.

- Final selected integration run (session 1229) exited zero: **312 passed, four
  backend-specific skips**, 2,611.70 s. All original 13 new mandatory cases in
  that collection passed setup/call/teardown. Log:
  /tmp/liuxin-schema-discovery-integration.log; audit:
  /tmp/liuxin-schema-discovery-gate.json.
- Final focused gate exited zero: **14/14 new mandatory cases passed** every
  phase, no violations. This includes the last standalone external-DDL lookup
  and explicit owned-connection cleanup assertions, added after broad collection.
  /tmp/liuxin-schema-discovery-final-gate.json and
  /tmp/liuxin-schema-discovery-final-regressions.log.
- Full modern quality passed; updated format scope **404 files**, targeted new
  lint passed. /tmp/liuxin-schema-discovery-quality.log and -format.log.
- Documentation **16 passed**, real Git index hash unchanged; isolated index:
  /tmp/liuxin-schema-doc-index-2fzyu4__. /tmp/liuxin-schema-discovery-docs.log.
- Ratchet/workflow tooling **57 passed**; after updating lint entries, workflow
  **35 passed** again. /tmp/liuxin-schema-discovery-tooling.log and
  /tmp/liuxin-schema-discovery-workflow-final.log.
- Final-source benchmark repeat: **4.205 s**, two fingerprints, 177 connections,
  1,208 statements; 125 tables/views, 44 interlinks, four intralinks. Source and
  harness hashes match the retained artifact. Earlier optimized three-sample
  median is 4.901 s; before is 201.303 s with 8,492 fingerprints.
- Private PostgreSQL stopped; pg_ctl status confirms no server running. All
  validation sessions are complete. No staging, commit or PR; all prior dirty
  work, user drafts, data submodule and stash are preserved.

The broad run's four skips are the live backend's two retained-membership cases
and the two snapshot backends' live-execution-view cases. These are capability
contracts, not unavailable PostgreSQL. The broad run covers the original 13 new
cases; the final 14-case gate covers the last test additions. No claim is made
that the entire updated 2,637-case database manifest was rerun.

The broad integration command selected these modules with pytest -q --tb=short
and RegressionGate restricted to the new invariant:

```text
tests/databases/test_schema_discovery_postgres_live.py
tests/databases/test_driver_wrapper_review.py
tests/databases/test_driver_wrapper_review_postgres_live.py
tests/databases/test_driver_wrapper_features.py
tests/databases/test_driver_wrapper_features_postgres_live.py
tests/databases/test_driver_wrapper_boundaries.py
tests/databases/test_physical_schema_sqlite.py
tests/databases/test_schema_specs.py
tests/databases/test_postgres_link_discovery_live.py
tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py
tests/databases/caches/test_storage_cache_real_db_regressions.py
tests/databases/caches/test_native_field_metadata.py
tests/databases/caches/test_predicate_queries.py
tests/catalog/test_cache_search_execution.py
tests/databases/caches/test_search_differential_postgres_live.py
```

## Historical checkpoints — active statuses below are superseded

Active goal: user accepted the first optimization recommended after the cache
review. Implement bounded reuse within complete schema discovery, retain external
freshness and transaction ownership, measure before/after and verify regressions.

Implementation is in the driver API, shared SQL default, PostgreSQL override and
DriverWrapper.get_schema_spec. PostgreSQL context-local fingerprint reuse has
entry/exit probes and failure cleanup; cached complete specs still require explicit
force_refresh after DDL. The default hook had to be placed in SQLBaseDriver because
concrete drivers do not inherit the abstract API implementation. Both new SQLite
transaction cases now pass after that repair.

Measurements: /tmp/liuxin-schema-discovery-before.json and -after.json. Full native
schema: 125 tables/views, 44 interlinks, four intralinks. Before: 8,492 fingerprints,
8,667 connections, 35,168 statements, 201.30 s. After: two fingerprints,
176–177 connections, 1,204–1,208 statements, 4.84–5.41 s (three samples).
Script: scripts/benchmark_schema_discovery.py. New dev-docs implementation record
and linked review follow-up are written; finalize their validation section.

Validation so far: 11 new live PostgreSQL cases passed. Original portable run:
172 passed and two new cases failed on the missing SQL default; subsequent fixed
SQLite run: two passed. These earlier runs are superseded by the active integration
run. Manifest now adds 14 mandatory cases (2,637 total) with the new invariant
bounded-schema-discovery-preserves-freshness-and-transactions.

Final new-case gate: **14 passed in 39.54s**, all setup/call/teardown passed, no
skips or violations. /tmp/liuxin-schema-discovery-final-gate.json and
/tmp/liuxin-schema-discovery-final-regressions.log. This includes the additional
standalone external-DDL link lookup and explicit owned-connection cleanup checks.
Full modern quality passed (basedpyright zero, mypy 476, negative contracts 95/93,
architecture/import budgets); updated formatting scope passed 404 files. Targeted
new-file lint passed. No runtime changes since these runs.

Active sessions (revalidate handles):
- 1229: final integration with RegressionGate for all 13 new cases; log
  /tmp/liuxin-schema-discovery-integration.log, audit output
  /tmp/liuxin-schema-discovery-gate.json. Includes portable/live wrapper checks,
  full PostgreSQL link discovery and all six PG cache/search differential cases.
- 79055: documentation links using an isolated temporary Git index/objects;
  /tmp/liuxin-schema-discovery-docs.log. Real index hash checked afterward.
- 15918: ratchet/workflow tooling tests, /tmp/liuxin-schema-discovery-tooling.log.
  Earlier attempt used the wrong tooling test filename and collected no tests;
  this corrected invocation supersedes it.

Private PostgreSQL is running on loopback 55439 from
/tmp/liuxin-cc-followup-pg/data. Stop it after all tests using pg_ctl -m fast stop.
Start requires -o '-p 55439 -k /tmp/liuxin-cc-followup-pg -h 127.0.0.1'; default
5432 belongs to another server and the first start attempt safely failed to bind.
No approval rejection occurred.

Remaining: finish checks and resolve failures, validate docs with a temporary
index if using the tracked-target suite (never stage the real index), retain the
measurement artifact/source hashes, final diff review, stop the private server,
record final results and mark the goal complete only after the completion audit.
Preserve all prior dirty work, drafts, stash and data submodule. No commit/PR asked.
