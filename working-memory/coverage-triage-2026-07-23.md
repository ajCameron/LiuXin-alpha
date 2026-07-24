# Coverage triage and catalog search tranches

Date: 2026-07-23

## Baseline

The external full run is:

- run ID: `coverage-2026-07-23-181804`;
- pytest: `5057 passed, 60 skipped, 17 xfailed`;
- statements: `119093 / 210196` (`56.66%`);
- branches: `28888 / 73086` (`39.53%`);
- combined coverage.py result: `52.24%`.

The XML, raw coverage database, HTML, JSON, and log are complete under
`working-memory/test-results/`.

The wrapper's `.done` file reports exit code `2`, but this happened after
pytest and all coverage reports completed. The already-running shell read
`run_full_test_suite.sh` while that file was being updated for live progress
and encountered an old/new source boundary. The current scripts pass `bash -n`.
Treat the coverage data and pytest result as valid, but do not treat that
particular wrapper marker as a clean runner exit.

## Triage policy

Raw missed-line ranking is dominated by code that should not lead the current
test effort:

- generated Qt resources such as `images_rc.py`;
- frozen, caller-free catalog mutation reference code;
- vendored parsers and date libraries;
- legacy Calibre cache/file-format paths;
- optional or platform-specific backends.

Those paths may still need tests when their feature is being activated, but
testing them first would increase the global percentage without protecting the
current catalog/storage architecture.

The first target was therefore active catalog search behavior. The completed
run showed:

| Module | Combined | Statement | Branch |
| --- | ---: | ---: | ---: |
| `catalog/search/__init__.py` | 21.72% | 27.37% | 11.15% |
| `boolean_search.py` | 14% | 23% | 0% |
| `date_search.py` | 14% | 21% | 0% |
| `numeric_search.py` | 7% | 11% | 0% |
| `utils/text/icu_fallback.py` | 9% line-only baseline | 9% | n/a |

## Added coverage

New behavioral lanes:

- `tests/catalog/test_field_metadata_contracts.py`;
- `tests/catalog/test_field_search_operators.py`;
- `tests/catalog/test_search_core.py`;
- expanded `tests/surfaces/test_acquisition_api.py`;
- `tests/surfaces/test_images_api_contracts.py`;
- `tests/utils/text/test_icu_fallback.py`;
- direct `force_to_bool` cases in `tests/databases/test_utils.py`.

The tests cover:

- two-state and tri-state boolean search;
- numeric presence/comparison/rating/size-suffix/error behavior;
- date precision, comparison, relative aliases, presence, and errors;
- match syntax, hierarchical and regexp matching, key/value search;
- saved-search persistence and no-database behavior;
- LRU eviction and refresh;
- restrictions, subset/virtual behavior, incremental cache maintenance;
- real parser dispatch across text, date, numeric, boolean, count, identifier,
  ID, and all-field searches;
- grouped search aliases, inversion, and recursive-group rejection;
- restricted `all` searches and candidate depletion;
- user categories, nested subcategories, and inverse membership;
- canonical and display-name language searches;
- generic rating, integer, float, presence, and excluded-cover behavior;
- full-library, subset, virtual, and cached restriction branches;
- source-only ICU case conversion, normalization, collation, accent-insensitive
  matching, numeric sorting, and character helpers.

Targeted post-change result:

| Scope | Combined | Statement | Branch |
| --- | ---: | ---: | ---: |
| search coordinator | 100% | 100% | 100% |
| boolean operator | 100% | 100% | 100% |
| date operator | 100% | 100% | 100% |
| numeric operator | 100% | 100% | 100% |
| ICU fallback | 100% | 100% | 100% |
| complete targeted set | 100% | 100% | 100% |

## Field metadata tranche

The raw baseline combined two nearly identical implementations and one retained
Calibre declaration:

| Region | Baseline statements | Baseline branches |
| --- | ---: | ---: |
| active `FieldMetadata` | 52.8% | 27.6% |
| `CalibreFieldMetadata` compatibility class | 35.8% | 9.0% |
| whole module | 45.7% | 18.9% |

One parameterized behavioral contract now exercises both live classes without
duplicating test logic. It covers the mapping facade, aliases and labels,
standard/custom/displayable/searchable classification, custom field creation
and idempotent refresh, series-index companions, record-index routing, dynamic
and grouped categories, serialization restoration, invalid definitions, and
the retained table-name/declaration compatibility helpers.

Targeted result:

| Scope | Statement | Branch |
| --- | ---: | ---: |
| `catalog/field_metadata.py` | `468 / 468` (100%) | `212 / 212` (100%) |

The complete catalog regression lane is now `478 passed in 416.48s` using four
workers.

## Surface boundary tranche

The next tranche covered current user-facing boundaries rather than large
legacy or vendored modules:

- acquisition cover payload coercion and size compatibility forms;
- invalid and missing book responses;
- stored cover, redirect, local-file, and placeholder fallbacks;
- format matching, absent formats, and rows without file identifiers;
- image discovery through both read-model and direct-database paths;
- corrupt/missing relation rows and lookup exceptions;
- MIME/name/storage metadata normalization;
- storage-manager installation, refresh, stale-manager, and failure behavior;
- direct local/remote image targets and HTTP, file-URI, absolute, and relative
  store roots;
- thumbnail initials and escaped/clamped placeholder SVG output.

Targeted result:

| Scope | Statement | Branch |
| --- | ---: | ---: |
| `surfaces/acquisition/api.py` | `80 / 80` (100%) | `38 / 38` (100%) |
| `surfaces/images/api.py` | `151 / 151` (100%) | `64 / 64` (100%) |

The full surface regression lane is `374 passed, 1 skipped in 1004.15s`; the
skip is the existing no-Tkinter environment case. Two multiprocessing
deprecation warnings in text-browser tests are pre-existing and non-failing.

## Storage orchestration tranche

`StorageManager` was exercised as an orchestration boundary rather than as a
collection of isolated helpers. The new behavioral contract covers plugin and
store registration, identifier lookup and removal, metadata-based candidate
selection, read/write routing, bootstrap reports, storage-row coercion,
backend-option construction, designation handling, and error propagation.

Targeted result:

| Scope | Statement | Branch |
| --- | ---: | ---: |
| `storage/store_manager.py` | `582 / 582` (100%) | `300 / 300` (100%) |

## SquashFS reconciliation tranche

The database-backed reconciliation contract now covers archive-path
normalization, traversal rejection, state inference and transitions,
transaction commit/rollback, designation creation and retargeting, legacy
policy fallback, snapshot drift, link/primary-link behavior, duplicate file
rows, reproducibility metadata, build failures, mixed missing/hash-mismatch
publishes, bootstrap refresh failures, invalid publish guards, and public
wrapper delegation.

Targeted result:

| Scope | Statement | Branch | Combined |
| --- | ---: | ---: | ---: |
| `storage/reconcile/squashfs_db_sync.py` | `710 / 732` (97%) | `271 / 302` (90%) | 95% |

The 22 remaining statements are predominantly corrupt-row or
schema-impossible states, unlink failures, and strict post-error guards.
Chasing those with fabricated database states would provide less protection
than the exercised lifecycle and failure contracts.

The complete storage regression lane is now `331 passed, 30 skipped in
104.20s` using four workers. The skips are existing runtime-dependent
async/pickling cases.

## Defects found and repaired

1. Numeric/date `>=` and `<=` queries were consumed by the shorter `>`/`<`
   operator first, leaving an invalid query value. Operators are now matched
   longest-first and parsing stops after the first match.
2. `force_to_bool` used the `unicode_literals` future feature as an
   `isinstance` type, raising `TypeError` for native `None`, booleans, and
   numbers. It now uses the existing `string_types` compatibility tuple.
3. The source-checkout ICU fallback lacked the `Collator` and constant surface
   required by `utils.text.icu`. Primary matching, saved-search sorting, upper
   casing, normalization, and other calls failed when the compiled extension
   was absent. The fallback now provides the consumed extension contract.
4. Unregistering a store by UUID, URL, or name could leave its numeric
   `_store_ids` binding behind. Unregistration now removes numeric bindings
   associated through any alias.
5. Wget backend construction forwarded `max_html_bytes`, an option supported
   only by the native HTML backend, causing a `TypeError`. Wget options now
   contain only fields accepted by that backend.

## Verification

- focused search/operator/utility lane: `148 passed`;
- targeted coverage lane: `100 passed`;
- text/fallback/import lane: `28 passed`;
- final complete catalog lane: `448 passed in 890.72s`;
- post-field-metadata complete catalog lane: `478 passed in 416.48s`;
- acquisition/image focused lane: `51 passed`;
- complete surface lane: `374 passed, 1 skipped in 1004.15s`;
- StorageManager focused lane: `75 passed`;
- SquashFS reconciliation contract: `44 passed`;
- complete storage lane: `331 passed, 30 skipped in 104.20s`;
- search coordinator: `485` statements and `258` branches at `100%`;
- field-search operators: `211` statements and `122` branches at `100%`;
- ICU fallback: `99` statements and `16` branches at `100%`.
- field metadata: `468` statements and `212` branches at `100%`;
- acquisition API: `80` statements and `38` branches at `100%`;
- image API: `151` statements and `64` branches at `100%`;
- StorageManager: `582` statements and `300` branches at `100%`;
- SquashFS reconciliation: `710 / 732` statements and `271 / 302` branches.

## Next evidence-backed targets

1. Run the complete external coverage command again so the next ranking is
   based on the new whole-tree aggregate rather than the now-stale baseline.
2. Select the next tranche from active production boundaries in that fresh
   report, weighting branch gaps and current callers above raw missed lines.
3. Continue excluding generated, frozen-reference, vendored, platform-only,
   and caller-free compatibility code unless a current feature activates it.

A fresh full coverage run is still required to establish the new global
percentage after these changes; targeted results prove the affected behavior,
not the whole-tree aggregate.

## Follow-up

The requested rerun completed as `coverage-2026-07-24-005354`: `57.36%`
statements, `40.75%` branches, and `53.07%` combined, with two unrelated
`bzzdec` timeout failures after `5362` passing tests. The next Catalog Unicode
and operation tranche is recorded in
`catalog-unicode-operations-2026-07-24.md`.
