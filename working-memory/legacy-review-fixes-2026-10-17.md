# Legacy review fixes — 2026-10-17

Scope: resolve all six findings from the follow-up legacy review and add executable
regressions. The user explicitly chose retirement of `library.legacy` and
`library.backend`, with callers migrated. This follows the
[unreachable-code checkpoint](unreachable-code-review-2026-10-16.md).

## Findings resolved

1. **Destructive custom-column refresh.** Malformed JSON used to delete the stored
   definition before checking its owning table, leaving values orphaned. Refresh
   now filters by owner first, validates the complete replacement, and raises
   `DatabaseIntegrityError` without deleting definitions or values. Failed refresh
   retains both existing maps and trigger snippets. Missing backing tables follow
   the same policy. Explicitly marked deletions remain a separate loader operation.
2. **Text/bytes tag cleanup.** The legacy database helper delegates to the shared
   metadata normalizer. Ordinary text and bytes now reach whitespace cleanup,
   comma replacement and case-insensitive deduplication, retaining first spelling.
3. **Python 2 custom-value sorting.** Both custom-value readers use stable
   `key=str.lower` sorting rather than the unsupported `cmp` argument. Sorting a
   returned value does not alter the stored cache string.
4. **Many-to-one role leakage.** Typed projections filter forward and reverse
   relationships, preserve known-but-unlinked identities, and copy maps. Ordered
   variants retain reverse priority order and use the same projection owner.
5. **Unusable many-to-many subfields.** Field projection no longer requires a
   database argument; factories return independent usable tables, preserve role
   filtering and priority order, and retain initialized map factories and link
   bindings. The optional old database argument remains accepted. The ordered
   typed table now correctly advertises `typed=True`; its compatibility factory
   and the factory that drops priority are covered too.
6. **Unimportable Library facades.** Retired `library.legacy`, `library.backend`
   and their unused archive importer. The authoritative `test_db_6` fixture uses
   `CustomColumns` with its existing database connection. In FRBR databases it
   explicitly selects the physical `manifestations` table, avoiding the `books`
   compatibility view. Its duplicate under `src/LiuXin_tests` was removed according
   to the [test source-of-truth policy](legacy-test-source-of-truth-2026-03-16.md).

## Retained contracts and callers

The supported `LiuXin_alpha.library.Library` facade remains the application API.
The Calibre relation tables and custom-column facade remain available and tested.
The retired modules were already unusable because several dependencies no longer
exist; their historical source is retained in Git. No production or fixture import
of either retired module remains. Wheel verification rejects stale copies.

The former AST-only Library identifier test was removed with its implementation.
Executable single-identifier validation and bulk-normalization contracts remain
in `tests/metadata/test_identifiers.py`. Catalog's frozen SQL reference helpers
(`catalog_macros` and `metadata_tools`) and their boundary tests are retained.
The obsolete Library-to-surface exception was removed from the layer baseline.

## Regression coverage

- Real SQLite and SQLite/APSW databases: corrupt display JSON, invalid JSON shapes,
  unrelated owners, failed-refresh snapshots, persisted definitions/values, repair
  and successful refresh, missing backing tables, and migrated fixture definitions.
- Both custom-value readers, ID and positional access, sorted/unsorted and empty
  values; mixed Unicode/text/byte tags and generator input.
- Real cache table and field classes: forward/reverse role filtering, known empty
  and unknown IDs, missing roles, ordered and unordered variants, independent
  snapshots, public `get_subfield`, and compatibility projection factories.
- Wheel regression cases reject both retired Library modules in stale build output.

Validation uses a detached checkout at `06bf503d` plus only this task's explicit
file snapshot, with the same environment and data fixtures. Results:

- Broad cache/Library, custom-column storage, fixture-manager, metadata, wheel-guard
  and Catalog-boundary selection: **464 passed, 12 skipped** in 598.57 seconds.
  Two newly written fixture assertions incorrectly called `len()` on the API's
  iterator; both were corrected to materialize it. No production failure remained
  from this run.
- Final focused run after those corrections and the final projection-container
  changes: **46 passed** in 48.08 seconds. This includes both failed fixture cases
  and the new missing-link-table cases. Pytest's isolated failed-test cache is empty.
- The 12 skips are six explicitly disabled legacy Calibre integration suites and
  six backend-specific cache cases. This does not certify the opt-in legacy suites.
- All four new regression files pass Ruff lint and formatting. Comparison with
  HEAD across all 15 changed source modules finds no new Pyflakes diagnostics.

- Complete quality script passed: 169 formatting targets, 463 file-format
  annotation targets, architecture/import-cycle checks, 21 cold-import budgets,
  lint/complexity, basedpyright, mypy (188 source files), and both internal-contract
  checks (37 invalid examples rejected by each). The architecture inventory now
  contains 1,796 modules and 15 reviewed backward edges.

- A fresh wheel passed content inspection and isolated installed-runtime checks:
  **1,788 Python files and all 403 runtime assets**, with both retired Library
  modules excluded. Catalogue creation/reopening, installed dependency origins
  and HTML-to-EPUB conversion passed.
- All 27 source, test and configuration paths match the verified snapshot.
  The two additional task paths are this handoff and the working-memory index.

All six findings are resolved. Local raw logs are retained under
`working-memory/test-results/legacy-review-2026-10-17/` (gitignored).

The separate [Catalog PostgreSQL signoff](catalog-write-consolidation-2026-10-15.md)
remains pending. This review does not claim live PostgreSQL validation. Concurrent
documentation, storage/plugin comments and data-submodule changes are excluded.
