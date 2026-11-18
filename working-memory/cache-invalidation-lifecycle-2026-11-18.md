# Cache invalidation and lifecycle follow-up

Completed **2026-11-18**. User requested the three remaining cache follow-ups:
small-invalidation performance, CACHE01 write-route/lifecycle audit and CACHE03
backend registry policy. The latest retry continued that complete objective.
No staging, commit, push or PR requested or performed.

## Implementation and ownership

Native query membership sets are private, transferred through the facade lock and
repaired in place. Only changed IDs are evaluated; broad ID/value-index copies are
avoided. The fifty ID/query-pair budget, conservative graph/provider invalidation
and full recovery after failure remain. Public records/results are independent.

Registry canonical names and aliases have immutable process-local bindings.
Identical normalized registration is idempotent; collisions reject atomically.
Publication/lookup use an RLock; imports and constructors run outside it. Failed
loading preserves the intended binding for retry. No replace/unregister facade.

The write-route audit repaired extension-table column writes using an independent
connection (now portable update_rows, joining the caller's transaction), Item-target
replacement invalidating only the new relationship, and migration bookkeeping
failure skipping invalidation of committed rewrites. Item-role deletion now shares
one transaction. Direct external Catalog/SQL mutations still require explicit
invalidation; no Catalog-to-cache callback or global observer was introduced.
Shared-value writers can defer refresh until their value-resolution transaction
closes. Storage serialized units retain authoritative reads during a transaction.

Real route tests use SQLite/APSW and all three backends, with actual transactions
and independent SQL readers. Cache captures include only the route families under
test; full catalogue capture remains covered by existing integration/long lanes.
Synthetic owned-link fixtures must follow canonical alphabetical endpoint naming.

## Final validation

- Core cache/Catalog acceptance: **542 passed, 16 expected skips**, 1223.11s.
  All **250 selected mandatory cases** passed setup/call/teardown.
  Script `/tmp/liuxin_run_cache_lifecycle_core_final.py`; log and artifacts
  `/tmp/liuxin-cache-lifecycle-core-final` (log has `.log` suffix).
- Final real route matrix: **30 passed**, 751.18s; all thirty mandatory phase
  audits passed. Script `/tmp/liuxin_run_cache_lifecycle_routes_final.py`;
  log/artifacts `/tmp/liuxin-cache-lifecycle-routes-final`.
- Two full snapshot soaks: **2 passed**, 1222.74s, 10k rows and 1k mutation/recovery
  steps per schema/NumPy backend. Mandatory phases and final source hashes match.
  Script `/tmp/liuxin_run_cache_lifecycle_soaks.py`; log/artifacts
  `/tmp/liuxin-cache-lifecycle-snapshot-soaks`.
- Full quality passed: 419 files formatted, lint, annotations, import/layer/footprint
  checks, both type checkers and internal positive/negative contracts.
  `/tmp/liuxin-cache-lifecycle-quality-final.log`.
- Tooling: **87 passed, 1 explicitly deselected**, 263.55s,
  `/tmp/liuxin-cache-lifecycle-tooling.log`. The tracked-content documentation-link
  case remains excluded while new files and the user's existing review are untracked.
  A separate working-tree destination audit passed.
- **73 new mandatory cases** (15 invalidation, 16 registry, 30 real write routes,
  12 writer lifetime) raise the manifest to **2,748**, preserving previous groups.

The two ordinary final selections are disjoint: combined **572 passed/16 skipped**,
with **280 mandatory cases**. Superseded exploratory runs were stopped or corrected;
use these final artifacts, not their partial counts. Selected gates do not imply a
full database ratchet run. PostgreSQL, native Windows, full ICU, the real-database
long lane and Calibre soak were not rerun in this pass. Physical replica leases
(CACHE02) and SEARCH02–SEARCH05 remain separate storage/application work.

## Matched measurement

`scripts/benchmark_cache_invalidation.py` measures indexed FakeDB/schema refresh,
not database I/O. Three scales, one/ten/fifty warm queries, five untraced latency
samples and separate allocation samples, independent oracle checks. Before/after
used identical benchmark source and ran without concurrent repository suites.
Saved pre-change modules: `/tmp/liuxin-query-before-invalidation.py` and
`/tmp/liuxin-read-before-invalidation.py`; their hashes match the baseline JSON.
Raw logs/JSON: `/tmp/liuxin-small-invalidation-before` and
`/tmp/liuxin-small-invalidation-after` (each `.log`/`.json`).

At 100k rows/50 warm queries, changing membership fell from **793.307 to 3.833 ms**;
peak additional traced allocation fell from **216,839,176 to 22,768 bytes**.
One-query unchanged membership: **13.599 to 0.157 ms**. Total retained cache memory,
NumPy refresh, relation fan-out, insertion ordering and sorting are separate costs.

[Review and measurements](../dev-docs/caches/invalidation-and-lifecycle-2026-11-18.md)
record the contract and evidence. README, next_tasks, structure, architecture review,
Catalog/cache boundary, regression guide and quality scopes are updated. Registry
line-number allowances moved without adding dependency exceptions.

## Preserve prior work

Branch remains caches-calibre-compat-boundary at c1d2cb74. Prior uncommitted adapter
and pagination changes remain, alongside this pass. Preserve user dirt in
LiuXin_alpha_data, Claude outputs/, the architectural review index and the separate
file-formats review. Existing stashes are untouched. No active test or benchmark
runs remain after the final checks.

## Addition/removal tests follow-up — 2026-11-18

User subsequently requested more add/remove coverage. Added 72 source-model cases
in `tests/databases/caches/test_membership_mutations.py` and six real transaction
cases in `test_write_route_recovery.py`. No runtime changes in this follow-up.

The matrix covers draining/refilling tables, first/interior/last IDs, insertion
ordering, known misses, delete/reinsert with reused IDs, deferred notification
batches, warm predicates/text/counts/pages/field indexes, shared links and both
directions, partial refresh failure and bounded seeded churn. Real SQLite/APSW
transactions cover commit, rollback, same-transaction reuse, failed insertion
batches, delete-all and refill, with independent SQL readers. The real fixture
captures only its extension table; synthetic relationship cleanup explicitly
models cascades and does not test SQL foreign-key behavior.

Final focused acceptance: **174 passed**, no skips, **174/174 mandatory phase
audits** passed, 151.49s. Runner `/tmp/liuxin_run_cache_membership_final.py`, artifacts
`/tmp/liuxin-cache-membership-final`, log with `.log` suffix. Includes all new
cases plus existing small-invalidation, paging and search-stress modules.
Tooling: **64 passed**, 67.78s, `/tmp/liuxin-cache-membership-tooling.log`.
Full quality runner passed: `/tmp/liuxin-cache-membership-quality.log`. Formatting
420 files, lint, annotations, import/layer/footprint checks, both type checkers and
their internal contracts. Final acceptance source hashes still match the tested
cache modules and selected tests. All follow-up runs are complete.

The new invariant `native-cache-membership-add-remove-reuse` adds 78 exact IDs,
raising the mandatory count to **2,826**. Both the pre-task 2,748-case manifest
and HEAD baseline are preserved; pre-task copy is
`/tmp/liuxin-membership-ratchet-before.json`. New test module joins format/lint
scope. Dev-docs review/README and regression guide updated; changed working-tree
links resolve. No full database ratchet, PostgreSQL or gated long-lane rerun.

An exploratory synthetic run corrected a test expectation: projected records
always include their ID column. The final 72-case exploratory run passed before
the audited final selection. An early real-matrix run was stopped to add explicit
same-transaction reuse and narrow its captured schema; use only final artifacts.
No staging/commit/push/PR. Preserve all preceding runtime changes and user dirt.
