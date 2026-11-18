# Metadata search implementation — 2026-11-17

Subsequent user-requested stress testing is tracked in
[the current handoff](cache-search-stress-2026-11-17.md). It adds correctness
repairs, randomized/differential tests, live PostgreSQL coverage and a 100k-record
baseline. The validation below describes the earlier implementation milestone.

Active branch: `caches-calibre-compat-boundary`. User authorized implementation of
the staged metadata-first Calibre-plus plan. No commit or PR requested for this
turn. Preserve the earlier 61-file compatibility migration, user review drafts,
`LiuXin_alpha_data`, and stash `eac312fe33e9a8629b9b99b52d76c2be06fb9646`.
Do not reset or pop anything. Database PR #170 remains separate.

## Implemented

- CR02/CR03: whole-request invalidation validation, DIRTY state before backend
  mutation, full recovery after refresh/invalidation failure, query-state eviction.
- Physical comparison/count/provider predicates and complete Boolean/relationship
  evaluation in native caches. SQL supports the ordinary comparison subset.
- `CatalogSearch.compile()` and explicit `find(..., read_cache=...)`.
- Bounded validation/result LRUs (100/50), fresh live query capture, proven row-local
  membership repair within 50 changed-ID/query pairs. Graph/provider/indexed-text
  results are evicted. Explicit ID invalidation is the current repair entry point.
- Fixed NumPy loading of physical dictionaries and exclusion of identity-less
  views, discovered by populated WEMI integration tests.
- Explicit copied `CalibreSearchAdapter`, CalibreBookRef identities, shared display
  projection, native custom values/aliases, definitions/restrictions, typed queries,
  stable sorts and current pinned template grammar/builtins.
- Templates/composites require a one-use acknowledged trusted-local capability;
  unconditional prominent warning, transitive preflight before pruning, remote
  rejection, permission reset, no persisted permission or implicit global functions.
- Pinned GPL source notices and packaged provenance manifest; wheel member guard.
- New code is included in formatting/lint gates. New mandatory regression group
  has 61 database cases; total manifest is 2,534 (do not mistake that for a full gate run).

The behavior and parity ledger are in
[metadata-search.md](../dev-docs/caches/metadata-search.md). Content FTS, notes,
annotations, application providers, full ICU-locale differential parity and large
library tuning remain explicitly named subsequent milestones. This milestone is
not a claim that the entire Calibre application has been reproduced.

## Validation and artifacts

Python 3.12.3 under `.venv/bin/python`; use this interpreter (system `python3` is
older and cannot parse the current upstream type-alias syntax).

- Broader cache directory + WEMI/Catalog conversion integration: **356 passed,
  24 skipped**, `/tmp/liuxin-metadata-search-regressions.txt`. Skips cover unavailable
  PostgreSQL and backend-specific contracts.
- Subsequent focused predicate/adapter/conversion run: **105 passed, 4 skipped**,
  `/tmp/liuxin-search-final-focused.txt`.
- Final native backend/SQL-comparison regressions: **35 passed, 4 skipped**,
  `/tmp/liuxin-search-final-native-regressions.txt`.
- Final adapter/alias tests: **36 passed**, `/tmp/liuxin-search-final-corrections.txt`.
- Lifecycle, adapter and packaging-tool regressions: **108 passed, 6 skipped**,
  `/tmp/liuxin-search-final-lifecycle-packaging-tests.txt`.
- Full modern quality gate passed after adding the new lint/format targets:
  `/tmp/liuxin-metadata-search-final-quality2.txt`. Explicit native search typing:
  `/tmp/liuxin-search-final-native-types2.txt`.
- **75 targeted cases passed** with `RegressionGate` and zero failures:
  `/tmp/liuxin-search-mandatory.txt`. Of these, **61 database/cache cases** enter
  the database-only mandatory manifest; 14 Catalog cases remain in the separate
  Catalog test suite. The manifest intentionally rejects paths outside database tests.
- Synthetic 10k benchmark: `/tmp/liuxin-metadata-search-benchmark.json`; reproduction
  `python scripts/benchmark_metadata_search.py --records 10000 --repeats 20`.

Package snapshot pointer: `/tmp/liuxin-metadata-search-build-location.txt`.
Builds are outside the checkout because concurrent tests delete newly-created root
artifact directories. `python -m build` is absent, so use the installed
`setuptools.build_meta.build_sdist/build_wheel` with no network or isolation.
The snapshot needs `docs/man` as well as src, licence files, README and pyproject.
Final wheel and source distribution verification passed, including exact current
search sources, GPL notices, provenance, metadata licence expression and isolated
wheel import/query smoke: `/tmp/liuxin-search-package-verified.txt`. Both artifacts
are under the snapshot's `dist/` directory. No build directory remains in the repo.

Documentation/tooling checks: **80 passed**, `/tmp/liuxin-search-doc-check.txt`.
The documentation membership check used an isolated Git index and object store
under `/tmp`; the real index remains untouched. Final template tests, including
nested injected functions and cross-thread permission isolation: **31 passed**,
`/tmp/liuxin-search-template-final.txt`. Final lifecycle checks after releasing
pending result repair state on clear/close: **27 passed, 6 skipped**,
`/tmp/liuxin-search-last-lifecycle.txt`. Final formatting: **397 files clean**;
final native import-cycle guard passed. `git diff --check` is clean.

No live PostgreSQL, native Windows or full-project test run. No source-control
staging, commit or push. Existing uncommitted changes are preserved.
