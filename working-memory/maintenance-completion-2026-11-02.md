# Maintenance completion handoff

## Completed — 2026-11-03 (supersedes all historical notes below)

All four requested maintenance follow-ups are implemented and locally verified.
The [completion record](../dev-docs/databases/maintenance-completion-plan-2026-11-02.md)
links implementations, regression evidence and scope limits. Changes remain
uncommitted on `maintenance-plugin-modes`; no commit or PR was requested. Preserve
the pre-existing dirty data submodule, preference changes and user TODO comments.

- Work projection rebuild: bounded fixed-bound scans, durable resume checkpoints,
  source and orphan identities, prompt atomicity, ordinary lazy jobs and explicit
  restart. Process-loss regressions verify committed-page recovery.
- Plugin upgrades: implementation-owned version/migration policy, preview/recheck,
  pending-work drain or explicit replay, old-worker fencing and dependent freshness.
- Delivery: bounded page publication and job-ledger snapshots improve equivalent
  440-obligation drains from 132.70s to 37.69s on SQLite and 209.46s to 67.40s on PG.
- Domain operations: Catalog owns previewed semantic merge, selected discard/
  preserve cleanup with reasons, atomic audit receipts and attributed inherited
  WEMI Tags. Database helpers own physical reference inspection/locking/storage.
  Unsupported ownership/tree policies and incomplete scans refuse apply.

Completed larger trials:

- SQLite: 2,200 Works, 699.01s total, 519.42s concurrent import/delivery, 104.1 MiB
  peak application RSS; all counts/revisions match, zero pending, 4,400 changes and
  8,800 completed obligations pruned.
- PostgreSQL: 660 Works, 774.45s total, 257.13s concurrent import/delivery, 116.2 MiB
  peak application RSS; all counts/revisions match, zero pending, 1,320 changes and
  2,640 completed obligations pruned. Raw report is
  `dev-docs/databases/maintenance-postgres-soak-2026-11-03.json`; analysis and other
  reports are linked from `dev-docs/databases/maintenance-scale-2026-11-02.md`.
- These are synthetic metadata workloads. PostgreSQL server RSS is excluded.
  Pruning permits page reuse; it does not shrink files or prune jobs/audit history.

Final verification (selections overlap; no full repository suite claim):

- Earlier maintenance matrix: 324 passed, 28 backend-specific skips; upgrade
  follow-up 22 passed; publication matrix 162 passed, 28 skips.
- Repair selection: 53 passes/four skips plus three corrected assertion failures;
  final selection passed 51 with one short API docstring failure. The corrected
  API docs/format-contract follow-up passed 28. Details below remain historical.
- `/tmp/liuxin-maintenance-snapshot-final-20261103.log`: **nine passed**, 140.16s.
  Selection: `tests/databases/test_read_transaction.py` and
  `tests/catalog/test_inherited_metadata.py::{test_snapshot_rejects_writes_and_nesting_without_poisoning_connection,test_shared_manifestation_retains_both_works_and_direct_tags}`.
  Confirms completion-hook write rejection, fallback query_only restoration and
  real SQLite/APSW/PostgreSQL snapshots/inheritance. Protection stays enabled
  through transaction completion; owned connections close without restoration.
- `/tmp/liuxin-maintenance-docs-final-20261103.log`: **43 passed**, 77.40s;
  `tests/scripts/test_developer_documentation_links.py` and
  `tests/scripts/test_run_format_checks.py`.
- All **258** selected files pass formatting; scoped lint/whitespace pass.
  Import-cycle gate: 255 protected modules; layer gate: 1,877 modules; all 25 cold
  import budgets pass. Man rendering, API examples and Catalog SQL ownership pass.

All test/benchmark sessions finished. Disposable PG schemas were confirmed absent.
The private cluster at `/tmp/liuxin-maintenance-completion-pg` was stopped and
removed; do not try to resume the old handles or connect to its old socket.
Raw logs/reports remain in /tmp; benchmark scratch was removed by the script.
New files have intent-to-add only; no contents were staged or committed.

Next work is a monitored real SquashFS import, Catalog scalar-write profiling
(historical PG import was already slow before maintenance), and explicit jobs/
audit history retention. Wider inheritance and hierarchy/owned-resource merge
policies need separate decisions. The older physical `maintenance merge` and
transient compatibility queue retain their narrower documented contracts.

## Historical continuation — 2026-11-03

**00:15 UTC update:** the final focused selection finished: 51 passed, one short
exported-Protocol docstring failure. Both docs were fixed; the follow-up API/
format-contract selection passed **28 tests**. All 257 formatting checks passed
and all 25 import budgets passed. PostgreSQL soak is now running in **exec
session10287**, prefix `/tmp/liuxin-maintenance-postgres-soak-20261103` (.json,
.events.jsonl, .stderr), with 300+300+60 Works and max_seconds1800. At 00:15 it had
500 committed Works; no errors. Do not restart it. No heavy tests run alongside it.

A final review found SQLite `read_transaction` restored query_only before
completion hooks ran. Fixed it to keep protection through transaction completion,
restoring only persistent fallback connections afterward; owned connections close
with protection still active. New `tests/databases/test_read_transaction.py` tests
an intentionally writing completion hook (owned/fallback connections) and original
read-only fallback restoration. **Run these three new tests after the benchmark**,
plus existing production snapshot cases if needed. They have not run yet. The
format ratchet now contains **258 files**, and this new test is not intent-to-add
yet. The benchmark uses ordinary transactions, so this read-only fix does not
change its loaded delivery path. Update final evidence and rerun links after
adding its finished PostgreSQL report. Everything else below remains relevant.

The goal remains **active**. Items 1–2 remain verified. Item 3 now has the optimized
PostgreSQL 220-Work report (67.40s drain, all verified) and a completed SQLite
2,200-Work soak (699.01s total, 519.42s drain, all 2,200 count/revision pairs verified,
zero pending and all 4,400 journal changes pruned). Peak application RSS 104.1 MiB;
jobs ledger retained 9.3MB of completed history. Reports and dated analysis are in
dev-docs/databases (`maintenance-*-paged-*`, `maintenance-sqlite-soak-2026-11-02.json`,
`maintenance-scale-2026-11-02.md`). **Larger PostgreSQL soak still needs running**
after tests finish, using 300 historical + 300 captured + 60 concurrent Works,
batch50/page128/drain128, max_seconds1800, output under /tmp with a new 20261103
name. Do not run heavy tests concurrently with performance trials. Private PG
cluster described below remains running; do not recreate it.

Item 4 is now implemented, with final verification underway:

- `catalog/inspection.py`: complete bounded row reader.
- `catalog/retrieval/inherited.py`: all WEMI parent paths, direct Work/Expression/
  Item tags, distinct union with full attributed contributions and physical edge
  rows. No stored copy/cache; final link removal is immediately visible. New
  `macros.read_transaction` owns a backend-enforced read-only snapshot, rejecting
  nesting. Core `catalog.inherited.tags`, CLI `catalog inherited-tags LEVEL ID`.
- `catalog/mutations/repairs.py`: preview/apply merge and selected cleanup, exact
  source/target selection, per-field/link decisions, full physical incoming FK
  and curated polymorphic inspection, stale snapshot fingerprints, explicit
  preserve/discard/reason choices, placeholder protection, atomic audit receipt.
  Scoped hierarchy changes/child transfers and unknown direct owners block apply.
  Typed directed Work links stay distinct; `user_marked_different` is symmetric
  and remapped to canonical ordering; self-links block. Duplicate relationship
  deletion is refused if that relationship table has inbound FKs.
- `SQL/macros/repair_storage.py`: actual SQLite/PG FK inventory (including composites
  and external schemas for refusal), PG source/reference table locks inside a
  serialized transaction, optional `catalog_repair_receipts` creation on explicit
  apply, append/read JSON audit. `db_types/repair_types.py` encodes native PG
  timestamps/decimal/bytes without truncation; fingerprint uses same encoding.
  Operational link datestamps do not count as semantic conflicts.
- `MetadataWriter.merge_entities` delegates to this owner and defaults to preview;
  old partial-transfer helpers removed. Existing merge fixtures choose old target
  precedence explicitly in `tests/catalog/repair_helpers.py`. Unicode identifier
  test explicitly demotes its source primary. Source deletion/target normalization
  and all transfers/audit share the serialized transaction.
- Core `catalog.repair.merge`, `.cleanup`, `.receipt`; legacy
  `catalog.metadata.merge` uses the same policy. Applied repairs reconcile caches;
  previews do not. CLI `catalog repair merge|cleanup REQUEST.json`, default preview,
  `--yes --expected-state HASH` apply, `catalog repair receipt ID`. All open with
  storage and maintenance disabled. Legacy metadata merge now has preview/decision
  file flags and the same expected-state requirement.
- API/operator/man/retirement/navigation docs updated, marked verification in
  progress. New files have intent-to-add only. No contents staged/commit requested.
  Formatting ratchet now includes 11 new code/test/benchmark files (257 total).

Verification this continuation:

- `/tmp/liuxin-catalog-repairs-first.log`: 21 pass/2 skip/4 PG failures exposed
  native timestamp JSON encoding; fixed with typed encoding.
- `/tmp/liuxin-catalog-repairs-second.log`: **53 passed/4 backend skips/3 failures**,
  all failures were the new prompt test expecting MaintenanceFailure while its
  injected plugin correctly propagates RuntimeError. Corrected assertion and
  checked receipt is never published (prompt flush occurs at nested write release).
  All typed/symmetric links, PG raw concurrent-reference fencing, stale plans,
  selected cleanup, unknown references, scoped Genres, old public merge callers
  and CLI cases passed. This was not the full repository suite.
- Final focused tests are currently in **exec session28503**, log
  `/tmp/liuxin-catalog-repairs-final.log`, telemetry prefix same. The first nine
  backend regression cases passed (prompt rollback, real Core merge/receipt,
  rollback after audit insertion). One API documentation test failed because the
  two new exported Protocol class docs were <12 words; both docs are now expanded.
  The rest is finishing. Poll, inspect full result, then rerun that failed doc test.
- Isolated typed PG test `/tmp/liuxin-catalog-repairs-typed.log` passed. Diagnostic
  prompt log still reports old compiled assertion (file edited during that run),
  so use the final selection above. All diagnostic sessions have finished.
- Import-cycle gate passed (255 protected), layer gate passed (1877 modules), all
  **25 cold import budgets passed**, man renders and git diff whitespace passed.
  Scoped lint passes. Formatting had one blank-line failure in the CLI fake
  fixture after lint cleanup; fixed, rerun 257-file gate. `ruff format` previously
  touched two unrelated API files; those cosmetic changes were restored.
- One formatter permission-review request timed out. Subsequent commands and
  index intent-to-add were approved; nothing remains blocked by approval.

Next: finish final focused/docs/format checks, run the larger PG soak, preserve its
JSON and update analysis/completion-plan/retirement statuses accurately, final
acceptance audit all four items, stop/remove only the private PG cluster, update
working memory and mark goal complete only after all this is done. Preserve the
pre-existing dirty data submodule, preference files and user TODO comments. Do not
add a commit or PR unless requested.

---

Active goal: **address all four maintenance follow-ups** (backfill/rebuild, plugin
upgrades, scale evidence, and domain repair policies/operations). Do not mark the
goal complete after only the first two. See the
[acceptance plan](../dev-docs/databases/maintenance-completion-plan-2026-11-02.md).
Work remains uncommitted on `maintenance-plugin-modes`; preserve the pre-existing
dirty data submodule and user TODO comments. No commit/PR was requested here.

## Implemented and verified

- `databases/maintenance/rebuild.py`: Catalog-selected `RebuildDefinition`, bounded
  source and projection-key scans, fixed bounds, atomic metadata checkpoints,
  manual journal invalidations, prompt receipts and ordinary lazy delivery. Rebuild
  invalidates selected cached values even if their revisions are corrupt. Deleted
  source keys become tombstones. Status separates scanning/waiting/complete and
  marks declaration-obsolete checkpoints for explicit restart. No schema migration
  is needed; checkpoints use the existing metadata table.
- `catalog.maintenance.rebuild_work_projections`; Core `maintenance.run` action
  `rebuild`; CLI `maintenance rebuild --max-events N --max-batches N --restart
  --yes`. Restart is only for beginning a fresh scan. Ordinary repeated calls
  resume it. Defaults: one page, 128 identities; ceilings: 128 pages, 500 identities.
- `databases/maintenance/upgrades.py`: `runtime.upgrade(... expected_version=...,
  expected_definition=..., dry_run=True)` with implementation-authored
  `MaintenancePluginUpgrade` policies. Versions must change for changed declarations.
  Old work drains unless replay is explicitly supported; pending dependents and
  mode changes must drain. Apply checks the combined dependency graph, updates
  subscriptions, removes capture only after a table's final subscription, invalidates
  standard cached projections and fences pending replay jobs. Migration callbacks
  have a borrowed database-only connection; everything rolls back together.
  Completed obligations retain old versions. Latest upgrade receipts use metadata.
- Core/CLI `maintenance upgrade PLUGIN --from-version VERSION`, inspected preview
  by default and `--yes` apply; optional `--expected-definition HASH`.
- Shared `maintenance/errors.py` holds `MaintenanceFailure`, re-exported through
  runtime to preserve imports and avoid a runtime/upgrades import cycle.
- Status includes rebuild and upgrade receipts. Old workers/version mismatches
  cannot consume migrated work's retry budget.
- Operator guide, maintenance contract, man page, database tasks/navigation and
  operational CLI documentation updated.

Validation (overlapping selections, not unique totals):

- `/tmp/liuxin-maintenance-completion-regression-20261102.log`: **324 passed,
  28 backend-specific skips**, SQLite/APSW/live PostgreSQL 16, 516.58 seconds.
  Covers maintenance/runtime/process death/writers/Core/jobs/transactions/CLI and
  Catalog no-SQL ownership. This preceded the publication optimization below.
- `/tmp/liuxin-maintenance-upgrade-final.log`: **22 passed** on SQLite/live PG,
  including concurrent writer recheck, dependency-mode rejection and obsolete
  rebuild freshness. This also covers the final errors-module extraction.
- `/tmp/liuxin-maintenance-completion-docs.log`: **22 passed** in links, Catalog API
  documentation and SQL ownership. More documentation was added afterward, so run
  links again before final completion.
- Formatting: 246 scoped files pass. Lint on new/touched maintenance code/tests
  and benchmark passes. Import cycle/layer checks and all 25 cold-import budgets
  pass; rerun architectural checks after the final added modules. Man rendering
  and diff whitespace passed before the latest performance edits.
- Rebuild process-loss tests actually `os._exit(77)` during prompt work or after a
  committed checkpoint, then resume without duplicate/missing pages. They do not
  claim host power-loss guarantees.

## Scale work underway

`scripts/benchmark_maintenance.py` creates disposable data through real Catalog
writes, resumes a rebuild across Core close/reopen, imports concurrently while the
real jobs worker drains, verifies every Work's count/revision, and prunes completed
journal records. It records batch latency, backlog/age, application peak RSS,
catalogue/jobs/scratch sizes and host free space. It is explicitly **synthetic
metadata**, not a SquashFS/file ingest. PostgreSQL server RSS is excluded.

Baseline reports are saved in dev-docs/databases:

- `maintenance-sqlite-baseline-2026-11-02.json`: 100 historical + 100 captured +
  20 concurrent Works; 440 lazy obligations. Drain **132.70 seconds**, total 162.77;
  all 220 verified; peak application RSS 97,784 KiB; final catalogue 4,497,408 bytes,
  jobs 978,944 bytes. Pruning removes journal rows but does not shrink either file.
- `maintenance-postgres-baseline-2026-11-02.json`: same counts, drain **209.46s**,
  total 348.06; all 220 verified; peak 97,508 KiB; catalogue 6,332,416 bytes,
  jobs 978,944 bytes. Historical Catalog scalar writes take 63.01s/100 Works even
  before maintenance installation; this is a separate base-writer cost.

Both baseline reports describe the original per-job publication loop. Small
five-Work smoke report under `/tmp/liuxin-maintenance-benchmark-smoke-20261102.*`
was run alongside tests and is correctness evidence only, not a timing comparison.

Measured overhead prompted two changes, now tested:

- `DurableJobService.drain_maintenance` publishes a page once and refills after its
  budget or when the worker becomes idle. It still bounds individual job passes,
  leaves storage audits queued and refills newly eligible dependency layers.
- `JobRepository.get_runs` looks up at most 500 explicit IDs with one connection;
  the publisher uses that snapshot instead of reopening the ledger per queued row.
  Payloads, job kind, per-obligation claims/receipts and retry semantics are unchanged.
- `/tmp/liuxin-maintenance-publication-tests-20261102.log`: **162 passed,
  28 backend-specific skips**, 127.69s, including actual dependent lazy layers,
  retry/stale-worker cases, bounded snapshots and existing Core workflows.

Current live trial when this handoff was written:

- Optimized SQLite comparison is complete: all 220 Works verified, drain **37.69s**
  versus 132.70s baseline (about 3.5x faster); total 66.30s; peak 97,544 KiB. The
  report is `dev-docs/databases/maintenance-sqlite-paged-2026-11-02.json`.
- The equivalent optimized PostgreSQL trial is now running in exec session
  **70922**. Output prefix
  `/tmp/liuxin-maintenance-benchmark-postgres-paged-20261102` (`.json`, `.events.jsonl`,
  `.stderr`). Poll that handle; do not restart based on an observation timeout.
- After it finishes, evaluate the measured improvement, add a larger/longer soak and a dated
  analysis with machine-readable reports. Do not claim scale work complete from
  only the small baseline comparison.

Private PostgreSQL is intentionally still running for the active goal:
`/tmp/liuxin-maintenance-completion-pg/data`, socket directory
`/tmp/liuxin-maintenance-completion-pg/socket`, port 55435. URL:
`postgresql://blackjane@/postgres?host=/tmp/liuxin-maintenance-completion-pg/socket&port=55435`.
Server log `/tmp/liuxin-maintenance-completion-pg.log`. Each benchmark/test owns
and drops its own schema; do not touch system PostgreSQL. Stop/remove this private
cluster after the final verification. All test sessions above are finished.

Test command pattern: `LIUXIN_TEST_POSTGRES_URL=... PYTHONDONTWRITEBYTECODE=1
.venv/bin/python -m pytest -q -n0 ... --telemetry-dir=/tmp/... --basetemp=/tmp/...
-o cache_dir=/tmp/liuxin-maintenance-next-cache -o tmp_path_retention_policy=none`.
Live socket access and formatter/index writes require auto-reviewed escalation
because of the sandbox/case-mapped WSL mount. No automatic rejection occurred.

## Domain operations remain to implement

The [policy design](../dev-docs/catalog/maintenance-repair-policies.md) records
explicit conflict decisions, complete references, stale-plan rechecks, attributed
WEMI inheritance, selected disposal/retention decisions and durable audit receipts.
It is marked **design in progress**, not implemented functionality.

Important current-code findings:

- `catalog/mutations/metadata_writer.py:970` already has `merge_entities`, but it
  only moves selected WEMI/Agent/Note/identifier links, silently preserves target
  conflicts, skips unsupported specs, and deletes the source. Extend/delegate this
  owner and migrate callers/tests rather than create a second semantic merge path.
- Existing tests include `tests/catalog/test_semantic_catalog.py` (~340 onward)
  and `test_catalog_unicode_operations.py` (~773/828); their implicit target-wins
  cases need explicit decisions under the new policy.
- `StorageColumnSpec` has references_table/references_column fields, but a repo
  search found no constructor populating them. Do not assume this is a complete
  inbound reference inventory. Physical FK introspection must be implemented or
  completed in databases; Catalog remains free of executable SQL. Include
  polymorphic entity_identifiers explicitly and guard unknown references.
- Existing scalar metadata policy declarations live in `metadata/value_policies.py`
  and wrapper column metadata. Normalized keys must keep their current write owner.
- Scoped Genres and typed Work intralinks must be covered; do not reuse the
  physical Maintainer.merge as a complete semantic merge. Cleanup cannot infer
  unwanted records solely from missing links. Physical files are outside these
  catalogue repair operations.

Next: finish performance evidence/remaining optimizations, implement and test the
domain repair operations, update docs/retirement inventory, run relevant final
gates, and audit all four acceptance areas before completing the goal.
