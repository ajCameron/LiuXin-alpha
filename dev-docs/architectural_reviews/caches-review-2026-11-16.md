# Cache publication and lifecycle review — 2026-11-16

**Implementation follow-up, 2026-11-17:** CR02 and CR03 are now repaired in
`ReadCache`. Full and partial refresh failures leave DIRTY state, evict query
state and require a complete recovery. Invalidation resolves all dependencies
before mutation; backend failures retain dirty bookkeeping. The original
reproductions below remain the historical evidence. Cross-backend regressions
are in `tests/databases/caches/test_predicate_queries.py`; the
[metadata-search contract](../caches/metadata-search.md) records the final behavior.
This closes these specific findings, not the whole module review.

Status: **initial review complete; CR01 repaired in follow-up, CR02/CR03 open.**
Original reviewed source checkpoint
`2d2a9cc304b5394ab90003b0567c4d06130337f1`. This pass changes documentation and
manual reproduction probes, not production code. It is not whole-module sign-off.

The subsequent [compatibility boundary review](../caches/calibre-compatibility-review-2026-11-16.md)
records the user's next requested focus: identify the legacy cache family for
relocation while preserving native search, field metadata and cache ownership.
The subsequent implementation is recorded in the [cache structure](../caches/structure.md).
It also repairs CR01 with registry-wide publication tickets. The findings and
original probe results below describe the initial checkpoint; CR02/CR03 remain open.

The ownership split remains appropriate: databases owns native persistence,
Catalog owns metadata mutation policy, and caches owns derived state, publication
and recovery. Keep `ReadCache` separate from the Catalog-dependent writable
`Cache`. The first repairs should make publication order and failed refreshes
reliable within those boundaries.

## Scope and evidence

Read `CustomFieldCache`, its transaction completion path and metadata publisher;
the modern facade's lifecycle, writer binding, invalidation and reconciliation;
query-index reuse; and the relevant backend lifecycle and refresh paths. Checked
the existing [Catalog/cache contract](../catalog/catalog-cache-boundary.md).
Backend implementations were read selectively, not exhaustively.

The focused existing suite passed **112 tests, with six capability-specific
skips**. It covers native commit/rollback and publication failure, modern facade
reads/writes, plugin contracts, import boundaries and real-database regressions.
The native publication probe uses real SQLite through both `SQLite` and
`SQLite_apsw`. The other probes use the existing deterministic cache harness;
CR02 injects a late table-read failure and CR03 uses an ordinary invalid field
name. Five probe cases fail as expected and three are outside their backend
scope. These failures demonstrate the findings; they are not passing regressions.

## Findings

| ID | Priority | Finding | Reproduced on |
| --- | --- | --- | --- |
| CR01 | P2 | Reentrant receipt delivery can publish an older custom-field projection over a newer committed projection. | SQLite and SQLite_apsw |
| CR02 | P2 | A failed data refresh can expose mixed table generations while the facade remains READY. | schema_backed |
| CR03 | P2 | Partial invalidation can refresh exact lookups without invalidating text indexes or advancing the generation. | schema_backed and numpy_vectorized |

P2 here means a reproducible correctness gap requiring a focused repair. These
probes do not demonstrate loss of authoritative database rows.

### CR01 — older native metadata publication overwrites newer state

Source: [custom_fields.py](../../src/LiuXin_alpha/caches/custom_fields.py),
lines 35–37 and 66–81; native receipt delivery in
[native_service.py](../../src/LiuXin_alpha/databases/custom_columns/native_service.py),
lines 114–128.

Register a receipt listener which, once, creates a second field through the same
`CustomFieldCache.write()`. Create the first field through that cache. The outer
transaction commits and delivers its receipt before publishing its prepared
metadata. The listener opens and commits a new transaction, publishing both
fields. The original callback then publishes its older, one-field snapshot.

Observed after both successful writes:

```text
durable=['#works.from_listener', '#works.outer']
visible=['#works.outer']
```

This uses the public subscription and cache-write APIs and needs no threads or
publication monkeypatch. A subsequent explicit refresh recovers the projection,
but the successful operations leave no indication that it needs recovery.

Repair in the cache publication owner: order prepared snapshots so an older
completion cannot replace a newer published state, including reentrant receipt
delivery. Preserve outer-commit deferral and savepoint discard. Add retirement,
rename and rollback cases alongside the reproduced creation case. Do not move
metadata projection responsibility back into the native service.

### CR02 — failed refresh leaves mixed data marked ready

Source: [read_cache.py](../../src/LiuXin_alpha/caches/read_cache.py), lines 469–481;
[schema_backed/storage_cache.py](../../src/LiuXin_alpha/caches/cache_plugins/schema_backed/storage_cache.py),
lines 228–239 and `initialize_tables()`.

Warm a text query for `Alpha`, change the backing book title to `Gamma` and a tag
name to `Changed tag`, then call `reload_data()`. Inject failure when reading tags,
after the books table has refreshed. The backend retains its initialized flags;
the facade propagates the error before changing its state or resetting indexes.

The next successful reads return:

```text
state=ready generation=1/1 title='Gamma' tag='Fiction' alpha=(2,)
```

The row is new, the tag and text index are old, and the generation is unchanged.
Holding the facade lock during refresh does not repair the partially applied
state subsequently exposed after an exception. This violates the contract that
readers see one complete generation.

Repair by publishing a complete candidate snapshot, or by making partial failure
explicitly unavailable/dirty until a complete recovery succeeds. Reset query
indexes together with the data they describe. Cover failure after each refresh
phase and failed recovery. The other backend reload paths need their own fault
matrix; this finding establishes the schema-backed fast path only.

### CR03 — invalidation fails between backend and facade bookkeeping

Source: [read_cache.py](../../src/LiuXin_alpha/caches/read_cache.py), lines 862–882;
[query.py](../../src/LiuXin_alpha/caches/query.py), lines 399–414.

Warm the `Alpha` text index, externally change that title to `Gamma`, then call:

```python
cache.invalidate(tables=("books",), fields=("books.not_a_field",))
```

The valid table is marked stale, then field invalidation raises `KeyError`.
Facade dirty bookkeeping and generation advancement are skipped. A subsequent
exact lookup lazily reloads the table, but the retained text index still describes
the old data:

```text
state=ready generation=1/1 title='Gamma' gamma=()
```

The defect is the partial state left after the reported argument error. Either
reject the entire request before mutation or retain a recoverable dirty state
for changes already applied. Ordinary subsequent reads must not disagree in a
supposedly complete generation. Cover both invalid arguments and backend faults;
the existing defensive dirtying used after failed Catalog reconciliation is a
useful precedent.

## Validation commands

Run from the repository root with its development environment:

```bash
.venv/bin/python -m pytest -q \
  tests/databases/caches/test_native_custom_fields.py \
  tests/databases/caches/test_modern_cache_facade.py \
  tests/databases/caches/test_cache_plugin_contract.py \
  tests/databases/caches/test_cache_imports.py \
  tests/databases/caches/test_storage_cache_real_db_regressions.py
```

Result: **112 passed, six skipped**. The skips select snapshot-only or bounded
row-refresh behavior rather than hiding test failures.

The [manual probes](reproductions/caches_2026_11_16.py) deliberately sit outside
normal test discovery. They reuse repository fixtures and assert coherent
publication/read outcomes. Run them explicitly; exit status 1 is expected at
this checkpoint:

```bash
PYTHONPATH=. .venv/bin/python -m pytest -p tests.conftest -q -s \
  dev-docs/architectural_reviews/reproductions/caches_2026_11_16.py --tb=short
```

Expected result: **five failed, three skipped**, corresponding to the backend
matrix above. Convert the relevant probes to permanent regressions with each
repair; do not treat expected failures as a cache sign-off gate.

## Next boundary to review

Fix CR01 first, then CR02/CR03 under
[CACHE01](../caches/next_tasks.md#cache01-make-invalidation-responsibility-explicit-for-every-write-route).
Retain the existing tests for rollback, committed receipts, known misses,
unsupported operations, detached writers and backend capability differences.

Then extend lifecycle verification to retained writers and child objects across
reattachment, partial construction/close failure, real outer-transaction rollback,
and concurrent publication/read behavior. Existing lifecycle tests pass, but
they do not establish all of those boundaries. Registry collision policy and
legacy Calibre caller migration remain separate work items in the cache backlog.

This pass did not run the complete cache/Calibre suite, full project suite, live
PostgreSQL, native Windows tests, concurrency stress or performance benchmarks.
Physical CACHE replica retention remains a storage/ingest concern.

Return to the [cache guide](../caches/README.md).

## Follow-up disposition

The compatibility migration introduced a native cache-owned descriptor registry
with registry-wide publication tickets. Its regressions cover reentrant writes
through one or multiple coordinators, rollback/discard, immutable snapshots,
alias ambiguity and retirement. **CR01 is repaired.** The original manual probe
now passes for both SQLite drivers. CR02/CR03 still produce three failing cases,
with three capability skips; they remain the next native-cache repairs.

See the [implementation validation](../caches/calibre-compatibility-review-2026-11-16.md)
and [current structure](../caches/structure.md). The original evidence above is
retained as the reviewed checkpoint, not the current CR01 status.
