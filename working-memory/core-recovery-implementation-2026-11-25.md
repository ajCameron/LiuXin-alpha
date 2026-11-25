# Core recovery completion — implemented and verified for review

Branch `core-review`, implementation base `e76ebe74`; original checkout/data/stashes
untouched. The user subsequently authorized the focused recovery review, commits
and PR. Jobs commit `72e2ca7e`, database commit `02992f46` and cache commit
`d149a989` precede the Core integration commit containing this note.
Use the established branch author identity for
individual Git commands; global Git configuration has no identity and is unchanged.

Implementation/contracts: `dev-docs/core/recovery-completion-2026-11-25.md` and
`dev-docs/core/recovery-operations.md`. It covers
journaled command IDs/outcomes, catalogue outbox, durable workflows, guarded
recovery, cancellation/deadlines, bounds/events/readiness, native disk adoption,
PG language upgrades, restoration/recreation and cache publication optimization.
Recovery registration uses runtime.database rather than runtime.catalog.db,
preserving lazy Catalog construction for job-only inspection.

Focused review found two additional bugs, both reproduced before correction:
- Generic worker returned success after an elapsed deadline; it now checks after
  serialization and retains the receipt with timed-out state. Four deadline/cancel
  combinations exercise the final boundary.
- An outer batch that catches a child rollback could claim cache=current while
  DIRTY. All three caches reproduced it. Unchanged outer batches now reconcile
  residual dirty state; a refresh failure retains changed=False/committed/dirty.
  Four mode cases and one refresh-failure case cover this contract.
Conversion regressions additionally cover a competing publisher and failure after
publication. No automatic uncertain-effect replay was introduced.

Python `.venv/bin/python`; set PYTHONPATH to the worktree's absolute `src` directory
for subprocess acceptance. Plugin cache env:
`LIUXIN_PLUGIN_CACHE_PATH=/tmp/liuxin-core-review-plugin-cache.json`.

Disposable PostgreSQL is STOPPED after all backend selections passed. Data retained
at `/tmp/liuxin-native-metadata-pg`; port55440, socket
`/tmp/liuxin-native-metadata-pg-socket`; UTF8 database liuxin_core_recovery.
Do not touch system PostgreSQL. No test server cleanup remains required.

## Implementation acceptance audit — complete before focused review

No disposable database server remains running. The later review runs are recorded below.
`/tmp/liuxin-core-acceptance-audit.json` covers all 1,026 collected Core/jobs nodes:
1,022 passed in default runs; four backend-only skips have passing live coverage.
No missing cases and no unresolved failures. The audit retains named result/source
and JUnit hashes. Reproducer: `/tmp/liuxin-audit-core-acceptance.py`.

- Full collection1026 nodes: `/tmp/liuxin-core-final-nodes23.json`.
- Initial partition `/tmp/liuxin-core-broad-final20.xml`:711 passed/1failed,
  stopped on failure. Failed global-search fixture used an unconstrained Mock
  for get_record_count; set return_value=1. Its full service-contract rerun also
  caught documentation drift: synchronized search_global summary and restored
  storage_file_copy param/result docs. No production logic change needed.
- `/tmp/liuxin-core-service-contract23.log`:24 passed, Ruff passed modified owners.
- Remaining partition23 completed:310 passed,1failed,4skipped in277.30s.
  The cold subprocess audit inspection exposed eager Catalog initialization under
  minimal prefs. Recovery registration now classifies the existing database handle
  without constructing Catalog. Regression asserts Catalog remains lazy before
  and after job inspection in the fresh subprocess.
- Final construction25:85 passed171.73s, covering lifecycle/configuration/native
  and observed commands/recovery/workflow delivery after that production fix.
  Final quality25 also passed after the fix. Those baseline sessions are complete.
- The audit combines broad-final20, remaining23 and construction-final25 XMLs.
  This is complete combined evidence, not one uninterrupted green invocation.
- Serial broad9 was superseded:498 passed/3skipped, clean interruption after46m.
  Do not count that as a completed suite.

## Completed evidence (overlaps; do not add totals)

- live-pg10:37 passed in3746.83s. Actual PG native/recovery/language/bulk/restore
  selection plus shared helper/SQLite checks. No skips. Slow NumPy shared/tree
  cases emitted180s faulthandler dumps but completed successfully.
- live-extra20:3 passed98.40s. PG profile reopening (URL and service selectors),
  durable maintenance. Fixture now uses start_jobs=False for its explicit manual
  drain; auto delivery has separate acceptance. This covers3 default-run skips.
- postgres-read-final24:1 passed49.40s. Final real PG native adoption/restoration/
  replay and bounded storage.file.read assertions; covers fourth default-run skip.
  Disposable server stopped automatically after completion.
- quality25:full configured gate exit0:537 formatted files,483 annotation targets,
  268 protected imports,2120 direction modules,31 cold import budgets, lint/
  complexity, basedpyright0errors/warnings, mypy603 files,127/123 negative contracts.
  New code/tests are now listed in lint and format gates. quality-runner18:43passed.
- docs-final25:22passed7.68s after final report/backlog edits; staged and working
  git diff --check passed. legacy16:98passed660s. final-bounds17:23passed.
- cache17:96passed1105s (refresh scopes, metadata workflow, writer transactions).
  FakeMacros now has a null read scope; real snapshot ownership tested separately.
- cache8:74passed420s. storage11:77passed109s. operations10:8passed69.5s.
  read-real16:3passed28s (real SQLite/APSW restoration and bounded content read).
- load-smoke13:all24 backend/cache/local+HTTP cases passed485s, no skips.
- load-warm17:10kWorks/4clients/background ledger read-hash/SQLite/schema_backed/
  HTTP passed92.91s; p95 read.837s,write1.149s,health.061s,cancel.155s; RSS186MiB.
- load-large15:100kWorks passed177.42s; p95 read.686s,write1.061s,health.051s,
  cancel.144s; RSS559MiB. Source hashes and host load averages retained with artifacts.
- Million-Work evidence and the running eight-hour gate are recorded below.
  Windows locking remains untested.
  Scalar reference latency does NOT establish relationship/schema publication
  latency; full NumPy schema refresh remains expensive and is documented.

## Focused review delivery

- review-focused26:101 passed43.74s, all jobs plus workflow/publication regressions.
- review-boundaries26:90 passed300.58s, command recovery, actual subprocess exits,
  HTTP boundaries, durable audit, job waiting and documentation links/boundaries.
- review-quality28:full quality gate passed after both production corrections.
- review-rejections28:5 passed49.87s after the cache correction; docs28:22passed19.08s.
- Updated acceptance-audit28 accounts for all1,032 collected Core/jobs cases:
  1,028 passed plus the same4 backend-only skips covered by live selections.
- review-million28:passed471.48s including construction; 300s measured workload,
  millionWorks/SQLite/schema_backed/HTTP/4clients/background ledger workflow.
  p95upper read.531s,write.743s,health.048s,cancel.109s; RSS4,335MiB.
  3,008reads/752writes/252replays. Lower concurrent host load than smaller runs;
  these observations do not extend the warm-reference SLO.
- review-cache27:85 passed1360.89s, covering metadata extensions and writer
  transactions/concurrency, including all5 new cache-receipt cases. Earlier
  million26 was deliberately interrupted during initial cache construction after
  the receipt bug was found; it is not a passing scale measurement.
- PR base is `cache-metadata-ergonomics` at `21539083` (open PR #214). Existing
  dependency stack is #213 → #215 → #214. Do not merge those PRs implicitly.
- User explicitly requested starting the eight-hour soak now. Detached supervisor
  PID1466605 and worker1466607 started2026-11-25T22:56:29UTC. Verified alive after
  launching; do not start a duplicate. `/tmp/liuxin-core-review-soak29/status.json`
  records state, PIDs and eventual exit/elapsed time; source hashes and final
  report share that directory. Log: `/tmp/liuxin-core-review-soak29.log`.
  It is RUNNING, not passed. Expected finish after06:56UTC26Nov plus setup/cleanup.
  Native Windows verification remains unrun.

Delivery uses `core-review` with PR base `cache-metadata-ergonomics`; inspect the
branch's PR for the remote review/check status. Preserve the original checkout
and stashes. The next operational follow-up is to read the overnight soak result
and record it, then arrange native Windows verification. Do not claim universal
backend latency or broad operational sign-off.
