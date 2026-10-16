# Canonical Catalog write paths — 2026-10-15

Status: active; implementation and completion audit outstanding.

## Objective

Normalize and reduce bibliographic write paths to one Catalog-owned mutation
boundary, without losing supported operations, transaction guarantees, useful
compatibility contracts, or historical data. Convenient repository, field,
container, Core, web, terminal and cache entrypoints may remain as adapters;
they must not independently choose stored values or bypass known validation.

The user authorized implementation after the ownership re-audit. This is not a
request to consolidate read models, configuration, jobs, or unrelated storage
operations. Existing `LiuXin_alpha_data` submodule changes predate this work.

Starting branch: `catalog-write-consolidation`, from `f1bb7622`.

## Verified starting evidence

- `metadata.write` already enters Catalog's `MetadataWriter.write_values`, but
  `CatalogContainerWriter` still creates/updates/deletes Rows and links directly.
- `BaseRepository` provides a write-preparation hook. Exact repositories derive
  label/series keys with approximate matching normalization, while drivers and
  container writes use tag/title profiles. Portable generic inserts/updates
  do not derive keys. Advanced column writers use `database.update_columns`.
- Core `admin.row.create/update/delete` bypasses semantic repositories, including
  generic web forms. The same label can be inserted with inconsistent keys.
- Identity profiles are persisted in `normalized_identities`; existing audit
  and migration macros detect collisions. Driver writes currently use defaults
  rather than consistently consulting those persisted declarations.
- Row compatibility helpers (`catalog.add/ensure/apply/intralink`) remain
  supported; frozen legacy schemas must not be confused with modern FRBR paths.
- Prior re-audit artifacts: `/tmp/lxa-ownership-reaudit.json`,
  `/tmp/lxa-ownership-probe.py`, `/tmp/lxa-ownership-followup.py`.

## Required outcome / completion evidence

1. Inventory all supported bibliographic mutation entrypoints, including
   repositories, coordinated/container mutations, schema-driven field/link
   writers, Row compatibility helpers, application/admin routes and caches.
   Identify shared owners and any explicitly justified compatibility boundary.
2. One preparation policy per persisted column: create, update, field writes,
   container writes and administration derive the same values, including
   partial updates, nulls, Unicode, explicit derived values and missing keys.
   Normalization implementations belong in metadata; schema declarations and
   structural constraints remain database concerns.
3. Generic public entrypoints cannot bypass existing semantic validation
   (especially identifiers), relationship/cardinality checks or transaction
   ownership. Batch failure must not leave earlier mutations committed.
4. Replace independent persistence implementations with delegation to canonical
   Catalog owners. Preserve public receipts, Row-returning contracts, cache
   reconciliation, link metadata and supported legacy capabilities.
5. Current-schema inconsistent values remain auditable and repair remains
   collision-aware. User explicitly says no old databases need importing;
   old-schema upgrade layers are out of scope and should not be added.
6. Regression evidence covers real SQLite and APSW paths, supported PostgreSQL
   contracts, public Core/surface entrypoints and compatibility adapters, plus
   architectural/type/quality checks. Tests must compare different entrypoints
   against the same policy and verify rollback, not merely mirror helpers.
7. Update owning documentation and this handoff. Completion requires a fresh
   path inventory and audit of every item above; a shared helper alone is not
   completion.

## Implementation sequence

- Establish canonical value preparation, respecting schema-selected identity
  policy and historical-data migration boundaries.
- Route public row/field operations and container persistence through the
  repository/mutation owners; close ordinary administration bypasses.
- Converge compatibility adapters and relationship operations, with explicit
  evidence for any legacy-only implementation that must remain.
- Exercise old-data audits/repairs, cross-entrypoint parity and full relevant
  quality checks; inspect remaining direct writes before declaring completion.

## Current state

Implementation is in progress, uncommitted on `catalog-write-consolidation`.
Repository, driver, Row and portable macro writes now share declared identity
preparation. Physical-table repository lookup routes Core administrative CRUD
and Catalog column writes through semantic repositories. Container row writes
now use those repositories. The Row-only container fallback has been removed;
the in-memory test backend now implements the portable persistence contract.
Pure profiles live in `metadata.text_identity`; historical text search functions
delegate there without pulling the metadata package into cold API imports.

User steering: assume there are no old databases to import; otherwise follow
the recommended full-text Unicode identity policy. The user strongly dislikes
compatibility layers and shims. Full-text NFKC/casefold/whitespace is the current
schema default. An old-profile upgrade branch was started and then removed in
response to this direction. Do not reintroduce old-schema upgrade machinery or
dual-mode new write adapters. Ordinary audit/repair remains useful for current
schema inconsistencies. No user database has been migrated.

Validation so far: new pure identity and Core route tests 6 passed; real SQLite
and APSW cross-entrypoint normalization tests 4 passed. Portable macro, writer
factory, owned-row, Core route and pure identity suite: 57 passed, 1 skipped
(`/tmp/lxa-write-contracts-second.log`). Five old compact-tag expectations were
updated with counterexamples proving distinct spaced/unspaced full-text keys.

Additional implemented work:
- `CatalogRepositories` moved to `catalog.repositories.group` with one shared
  construction/binding method. It remains importable from `catalog.catalog`.
- Library CRUD, column batches, table-value link creation and owned-row writes
  delegate to repositories. Batch preparation precedes the first mutation.
- Agent alias serialization moved into `_prepare_write`; immutable exact entities
  enforce mutability in that shared hook. Unused fuzzy-derived storage fields
  were removed from matching specs.
- `catalog.relationships.CatalogRelationships` owns repository, container and
  Core/Row relationship writes. `catalog.row_mutations` adapts Row requests and
  receipts; helpers in add/ensure/apply/intralink delegate there. Public helper
  workflows now use the available outer macro transaction via `atomic_write`.
- Container writes no longer swallow persistence failures. Tests now verify
  rollback instead of expecting partial writes and error reports. Fake fixture
  ISBN placeholders were replaced with checksum-valid ISBNs.
- Container series writes populate the identity source column; identifiers use
  curated normalization for reconciliation instead of generic case folding.
- Driver/wrapper identity iteration supports table filtering. Macro lookups
  prefer identity declarations over conflicting column-comparison metadata.
- Existing identity repair validates persisted declarations without discarding
  them on an unrelated missing key, and clears changing keys transactionally
  before backfill to permit swaps under unique indexes.

Recent validation: schema/Library suite 49 passed; focused relationship/Core/
container suite 57 passed; after removing the Row-only fallback, updated container
and Core metadata tests 41 passed (`/tmp/lxa-container-current-contract.log`).
Quality first run exposed an eager metadata import into protected APIs, corrected
by deferring only the value-policy implementation. Second quality run raced a
module move; rerun after code stabilizes. Undefined-name check currently passes.

The original test sessions and `/tmp` logs did not survive the restart. Fresh
verification now records logs and command/exit JSON under
`working-memory/test-results/catalog-write-2026-10-15/`.

## Commit/test checkpoint — 2026-10-16

The first commit checkpoints pure text identity policy, schema declarations,
driver/macro preparation and collision-aware repair. The fresh foundation suite
passed **148 tests, 3 skips** (`identity-final.log`), covering SQLite/APSW schema
contracts, PostgreSQL unit contracts, pure text policies and portable macros.
Live PostgreSQL portable macros passed **1 test** (`postgres.log`) against an
isolated temporary cluster. The new repair regression verifies unique-key swaps
and rollback when an enclosing transaction fails. Both complete quality runs
passed, including architecture/import budgets, basedpyright and mypy.

Catalog routing remains a separate pending commit. Fresh broad validation began
with 1,227 passes, 38 failures and 14 skips; socket restrictions account for part
of the failures. Fixes now use transaction-local repository reads for Row
receipts, distinguish legacy table shapes, preserve inherited identifier
ownership, and update stale key expectations. Historical-ambiguity tests seed
inconsistent keys using raw SQL instead of bypassing canonical public writes.
The final Catalog rerun and complete path audit are still pending at this
foundation checkpoint. The pre-existing data submodule is excluded.

Remaining: inspect broad failures and rerun affected cases on final code; live
PostgreSQL parity (available tests use LIUXIN_TEST_POSTGRES_URL); verify cache and
surface routes, relationship priorities/cardinality and alias preservation;
update stale docstrings and owning architecture docs; full quality gate and
fresh complete path audit. Do not reintroduce removed old-profile migration code.
The data submodule remains untouched. No completion claim for the full lane.
