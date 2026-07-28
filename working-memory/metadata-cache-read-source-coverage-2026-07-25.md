# Metadata Cache Read-Source Coverage

Date: 2026-07-25

## External baseline

Run `coverage-2026-07-25-external` completed its pytest and coverage reports:

- pytest: `6 failed, 5408 passed, 61 skipped, 17 xfailed`;
- statements: `121007 / 210469` (`57.49%`);
- branches: `29984 / 73218` (`40.95%`);
- combined: `53.22%`.

The active cache/read-source adapter was selected over low-covered frozen,
generated, vendored, and caller-free code. In the external report,
`src/LiuXin_alpha/metadata/read_sources.py` was at 70%; its focused pre-change
test module covered only 51%, with 71 missed statements and 11 partial
branches.

## Completed tranche

`tests/metadata/api/test_metadata_read_source_api.py` now exercises the full
fallback and snapshot contract:

- strict `CacheAPI` construction and matching attached databases;
- deterministic cache/database schema merging and schema-read failures;
- immutable cache hits, authoritative complete misses, and incomplete misses;
- fallback enabled and disabled for each declared cache gap;
- propagation of unexpected backend corruption instead of silent fallback;
- structured scan, count, exact-search, and explicit query forwarding;
- authoritative empty link results versus unavailable link indexes;
- relation completeness, type-filter forwarding, id-less rows, and row
  adaptation back onto the selected read source;
- complete pass-through behavior for the direct database adapter.

Focused post-change result:

- `36 passed`;
- statements: `162 / 162` (`100%`);
- branches: `54 / 54` (`100%`);
- no missed lines or partial branches.

## Defects and stale contracts repaired

- Injected empty modern cache facades are now loaded by the metadata facade
  before hydration.
- Tk cache-source selection now loads an injected empty composed cache after
  wrapping a raw storage plugin.
- Metadata facade tests now use the application-facing `FakeCacheFacade`
  instead of passing an incomplete raw-storage double across the modern
  `CacheAPI` boundary.
- Tk storage doubles now declare their attached database, capabilities, schema,
  and lifecycle state, and assertions recognize that the session owns the
  composed facade.
- The FRBR smoke reads the canonical
  `dev-docs/column-metadata.md` path rather than the removed
  `docs/development/column-metadata.md` path.

## Validation

- metadata read-source coverage: `36 passed`, 100% statement and branch;
- metadata facade/read-source/hydrator plus full non-display Tk lane:
  `92 passed, 1 skipped` (the skip is missing `tkinter` in this environment);
- read-model regression slice: `3 passed`;
- FRBR generator smoke: `1 passed`;
- compileall: passed;
- `git diff --check`: passed.

## Follow-up: external failure resolved

The real-database storage-cache writer regression reproduces independently:

`tests/databases/caches/test_storage_cache_real_db_regressions.py::test_storage_cache_catalog_writer_round_trips_through_real_database`

After writing the typed value `Ada`, the Catalog receipt is correct but the
reconciled storage relation reads `(None,)` instead of `("Ada",)`. This is a
separate writer/reconciliation defect and was not hidden or widened into this
read-source coverage tranche.

The subsequent Core API completion tranche repaired the defect: schema-backed
cache invalidation now marks dependencies without eagerly refreshing rows
inside the Catalog transaction. The original regression passes
(`1 passed in 48.85s`), and the same field-write/read path also passes through
the real Core/Catalog/Cache acceptance suite. See
[core-api-completion-2026-07-25.md](core-api-completion-2026-07-25.md).

Next verification should be a fresh external full run using the same coverage
runner so the new global baseline is directly comparable.
