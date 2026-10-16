# Canonical Catalog write paths — 2026-10-15

Status: active; implementation checkpointed with green tests; completion audit outstanding.

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

## Implementation carried forward from 2026-10-15

The pre-restart implementation was uncommitted on `catalog-write-consolidation`.
The 2026-10-16 checkpoint below supersedes the old test/session status.
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

## Foundation commit/test checkpoint — 2026-10-16

Commit `5f085a13` checkpoints pure text identity policy, schema declarations,
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

## Catalog routing checkpoint — 2026-10-16

The second commit checkpoints the shared repository composition, semantic CRUD
routes, Row adapters, container reconciliation and relationship owner. Fresh
testing exposed and fixed transaction-local Row receipt reads, legacy table
shape selection, flattened identifier ownership and relationship priorities.
The Row adapter retains whole-column highest/lowest semantics through a portable
`get_column_extrema` read on the active transaction; it ignores legacy priorities
for unordered links. Tests cover consecutive uncommitted links, global extrema,
unlink survivor priorities, aggregate rollback and identifier validation.

Final durable results in `working-memory/test-results/catalog-write-2026-10-15/`:

- `broad-final`: **1,266 passed, 14 skipped**. Catalog, portable APIs, Core,
  caches, Library, selected surfaces, documentation boundaries/links and metadata
  standardization. HTTP/RPC tests ran outside socket sandbox restrictions.
  Skips cover disabled legacy-cache suites, backend-specific cache behavior,
  the PostgreSQL-shaped SQLite pg_temp limitation and the separately run live PG
  test. One existing unregistered `catalog` mark warning remains.
- `relationships-final`: **36 passed, 1 skipped**, after the final relationship
  fixes; includes the full cross-entrypoint file and portable macro suite. These
  new cases were added after broad-suite collection. The skip is pg_temp on the
  PostgreSQL-shaped SQLite harness.
- `projection-final`: **35 passed** for the full container/hydrator real-backend
  regression selection, including inherited ASIN/ISBN ownership.
- `identity-final`: **148 passed, 3 skipped** for pure identity, portable APIs,
  PostgreSQL unit and SQLite/APSW schema/lifecycle contracts.
- `postgres`: **1 passed** on live PostgreSQL, rerun after adding the extrema
  transaction/rollback regression. This tests portable macros, not every public
  Catalog route on PostgreSQL.
- `quality-relationships`: complete `scripts/run_type_checks.sh` **passed** after
  the last implementation change: formatting, annotations, architecture, import
  budgets, lint/complexity, basedpyright, mypy and both internal contract checks.
  New-file Ruff checks and `git diff --check` also passed.

The live test used an isolated temporary PostgreSQL cluster on port 55453 with
no TCP listener, not a user database. No old-schema migration or data import was
performed. The pre-existing `LiuXin_alpha_data` submodule changes remain excluded.

Remaining before declaring the whole consolidation lane complete: a fresh full
entrypoint inventory and completion audit against the seven requirements above,
including any still-independent writes and missing public PostgreSQL parity.
Do not reintroduce old-profile upgrade machinery. Tests and commits are a green
checkpoint, not a claim that this broader audit has been completed.
