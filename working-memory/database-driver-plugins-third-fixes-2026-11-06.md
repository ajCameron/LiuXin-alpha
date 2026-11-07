# Third driver review repairs — 2026-11-06

User request: address all six DDP11–DDP16 findings, add regression tests and open a PR.
Implementation and local validation are complete. This handoff is written immediately
before committing and publishing the combined driver repairs. Query remote PR state
for branch database-driver-plugins-review-2026-11-05 before doing any new publication.
Base database-review: 07fc33264aab57e1975fd13688b3c58e59f5e0dc (merged PR155).
Worktree: /tmp/liuxin-databases-typing. Preserve the original checkout's accumulated
source/preferences/data; only its working-memory pointer was changed this turn.

All earlier first/second-review fixes remain included in the PR. Third-pass repairs:
- DDP11 explicit BEGIN for idle batches, including CTE writes. Preserve the documented
  whole-primary-context completion policy; no retries or partial durable prefix.
- DDP12 preserve PostgreSQL native decoded values, normalize binary buffers to bytes.
  Table dictionaries, views and Row reads share the policy.
- DDP13 finish/drain RETURNING statements before commit, return a detached WriteResult;
  SELECTs stream. Shared databases/sql_statements.py classifies CTE outer commands for
  both execution and maintenance, without mistaking CTE names for row operations.
- DDP14 close group cursors/connections on every path; refused deletion rolls back and
  releases its write lock before propagating the error. Both group APIs are retained.
- DDP15 use physical schema facts for SQLite uniqueness, including rowid/composite keys
  and quoted indexes; exclude partial/expression/nonunique indexes.
- DDP16 recognize contextual expression-prefix phrases and JSON attribute operands;
  preserve TIME/INTERVAL typed literals, literals/comments and mixed-binding protection.

Third pass adds 148 mandatory cases, taking 400 to 548; the PR adds 308 over the base 240.
No baseline test identity or invariant membership is removed. Red run: 54 failed, 72 passed;
initial focused: 283 passed in 90.63 s; initial gate: 540 passed in 203.44 s; SQL context: 60 passed in 2.03 s.
Final mandatory: 548 passed in 225.85 s, zero skips/xfails, baseline 07fc3326; all phases pass.
Affected integration: 495 passed, 13 skipped, 2 xfail in 594.13 s, including 22 documentation checks.
Skips: 2 fixture views, 1 PG-shaped SQLite harness, 10 backend-specific maintenance cases.
Xfails: 2 existing legacy scalar-return expectations. Two SQLite timestamp deprecation warnings.
All 121 driver/adapter doctest examples pass. Full quality passes: 310 format files,
463 annotation coverage,255 cycle modules,1903 layer modules,25 cold imports, lint/complexity,
basedpyright 0 errors/warnings, mypy 457 sources, 95/93 negative-contract cases.
Initial mypy inference issue was fixed with an explicit keyword-table annotation.
No full-suite run or production import is claimed. RETURNING buffering costs memory
proportional to returned rows; explicit configured connection/cursor ownership remains
available for callers needing a different lifetime. Macro units own nested transactions.

Evidence prefix /tmp/liuxin-driver-third-fixes-:
red.log, focused.log, sql-context.log, ratchet-final.log, ratchet-final/ratchet.json and
telemetry, quality-final.log, doctests.log, integration.log, integration-selection.json.
Pytest used -n0 and external scratch/telemetry; mypy cache was outside the checkout.
Final gate sampled test-process tree peak 83.2 MiB, host OOM-kill counter delta 0; excludes
separate PG/quality processes. Do not interpret it as a whole-machine memory guarantee.

PG16 at /tmp/liuxin-driver-third-fixes-pg-9x789cir is STOPPED AND REMOVED.
pg.json records cleanup; pg-server.log is retained. All prior review clusters removed.
No tests or service remain running. No subagents or skills used; no applicable AGENTS.

Current docs: dev-docs/databases/database-driver-plugins-third-review-2026-11-06.md
contains fixes/validation; transactions.md documents lifetime policy; next_tasks.md
returns structural consolidation and measured PostgreSQL introspection performance
to the next-work queue. PR combines all 16 reviewed repairs/retirements.
