# Catalog fitness review

Status: implementation complete and verified, 2026-07-22.

Practical facade/repository examples and matching semantics are documented in
the [Catalog API usage guide](catalog-api-usage.md).

## Review-time verdict

The catalog package is not yet fit for its complete declared purpose as the
metadata-aware WEMI persistence layer. The normalized writer subsystem is a
strong foundation, but the package currently combines:

- an implemented low-level writer and update pipeline;
- public semantic repositories, matchers, retrieval services, and mutations
  which are still scaffolds;
- active Calibre-compatible search and field-metadata code with Python 2
  failures;
- older metadata mutation helpers and catalog macros; and
- API protocols which do not consistently describe the concrete objects.

The package should not be considered complete merely because its catalog test
suite is green. At the time of this review all 307 catalog tests passed, but
300 exercised writer/update behavior, six were import/protocol smoke tests,
and one enforced the no-executable-SQL boundary. There were no behavioral
tests for semantic repositories, matching, retrieval, mutations, search, or
field metadata.

## Findings

### 1. Semantic facade is publicly exposed but not implemented

`Catalog` eagerly constructs repository, matching, retrieval, and mutation
groups. Their public concrete methods contain 37 `NotImplementedError` paths:

- generic repository CRUD;
- Work, Expression, Manifestation, and Item traversal and matching;
- Agent and Identifier resolution, matching, linking, and listing;
- Title and Note attachment and listing;
- Work, Agent, and Identifier candidate matchers;
- WEMI bundle retrieval and projections; and
- coordinated metadata attachment and entity merging.

The package documentation nevertheless presents these services as normal
usage. Several examples name methods which are not present on the API or
implementation. A composition root must expose working capabilities, or mark
experimental capabilities explicitly; shallow construction is not a useful
implementation contract.

### 2. Shared-value writer is not atomic end to end

`CatalogTableValueLinkWriter.build_update()` resolves replacement and addition
values with `ensure_table_value()` before it validates cardinality. Each ensure
uses its own committed database transaction, while the final link replacement
uses a later transaction.

Consequences:

- an invalid singular-cardinality request can commit new destination rows and
  then raise before applying any link;
- a later resolver or link failure can leave earlier destination rows behind;
- a bulk raw-value write opens one ensure transaction per distinct resolution;
  and
- `build_update()` is observably mutating despite being the inspectable
  planning half of the base writer lifecycle.

Singular cardinality is also checked independently inside replacements and
additions. A request which replaces a source with destination A and adds
destination B is accepted by the writer even though its combined result is
invalid.

The normalized link replacement itself is atomic. The defect is the larger
raw-value-to-destination-to-link operation. The durable fix is a portable
database operation which resolves or creates destination values and replaces
the affected links in one transaction. Cardinality and all caller-shape
validation must happen before that operation begins.

### 3. Active search contains reproducible Python 2 failures

The Calibre cache imports `catalog.search.Search`, so this is a live path.
The review reproduced:

- identifier key/value search failing on `dict.iteritems()`;
- LRU search-cache iteration failing on `dict.iteritems()`; and
- `populate_all_locations()` failing on undefined `VERBOSE_DEBUG`.

The language branch contains another `iteritems()` call. Restricted all-field
search catches every exception and silently returns partial results, while
cover search is explicitly skipped as broken. None of this behavior has
catalog tests.

The present `Search` object is coupled to Calibre cache internals, preference
state, cache invalidation, and private cache methods. It should either be
owned by the Calibre cache compatibility package or be replaced by a catalog
search service with an explicit repository/read-model dependency.

### 4. Field metadata has broken public mapping behavior

Both field-metadata classes inherit `dict` but store live records in a separate
ordered mapping. Consequently a populated instance reports length zero, is
false in Boolean context, and produces an empty inherited `copy()`.

Additional reproduced failures include:

- builtin `label_to_key()` lookup raising for labels such as `title`;
- `fm_from_dict()` calling `iteritems()` on ordinary mappings; and
- Calibre-compatible `itervalues()` and `custom_iteritems()` calling Python
  2-only methods.

The main and Calibre-compatible implementations duplicate most of the same
code and have already drifted. The replacement should implement the
`collections.abc.Mapping` contract over one canonical implementation, with a
small compatibility specialization only where behavior genuinely differs.
Field definitions describe metadata meaning and should ultimately live under
metadata or a clearly named compatibility package rather than the semantic
persistence layer.

### 5. Protocols do not match concrete object shapes

`CatalogRepositoriesAPI` inherits every entity repository protocol, implying
that the group itself implements all entity methods. The concrete group instead
contains `works`, `expressions`, `manifestations`, `items`, `agents`,
`identifiers`, `titles`, and `notes` attributes.

All three matcher protocols require `exact()`, while their implementations
omit it. The Agent repository protocol and implementation disagree on the
`list_for_wemi()` signature. Runtime protocol checks are shallow enough for
the top-level `CatalogAPI` smoke test to pass while the repository group and
matcher children fail their own contracts.

Catalog is not included in the configured strict mypy or basedpyright scopes.
The package exposes `py.typed`, so its public protocols must be treated as
shipping behavior rather than informal notes.

### 6. Multiple mutation architectures remain active

Catalog currently has three mutation surfaces:

1. normalized `Catalog.write*()` writers;
2. the unfinished coordinated `catalog.mutations.writer`; and
3. legacy `metadata_tools` and `catalog_macros` used by library and cache code.

`catalog_macros` annotates its argument as `CatalogAPI` while accessing raw
database internals such as `metadata_sql`, `ensure`, `driver.conn`, and
`get_row_from_id`. Several helpers commit directly, preventing callers from
composing them into a larger transaction. Legacy mutation code also uses broad
exception handling and `NotImplementedError` for ordinary input-validation
failures.

These paths need a named compatibility boundary and migration plan. New code
must not add another mutation surface.

### 7. Cardinality is being used as an ownership declaration

The schema-driven factory selects `CatalogOwnedRowOneToOneWriter` for every
one-to-one link. `StorageLinkSpec` records cardinality but cannot declare
whether the destination row is owned by its source or independently shared.

One-to-one cardinality does not imply ownership. The current choice is safe
only where every one-to-one relation happens to have owned-row semantics.
Ownership must become explicit schema metadata before the factory is treated
as a general catalog policy engine.

## Strengths to preserve

- `BaseCatalogWriter` has a small, reusable build/apply lifecycle.
- Update objects snapshot caller-owned containers and expose immutable
  inspection views.
- `LinkUpdateLink` provides endpoint/type/priority data, a mapping interface
  over extras, and lazy destination-value loading.
- Link type, allowed-type registry, duplicate identity, and link capability
  guards are layered across writer, update, and portable macro boundaries.
- `LinkUpdate` composes mixed operations into one atomic bulk link replacement.
- Owned one-to-one create/update/link/unlink is performed in one portable
  transaction.
- Cache reconciliation is downstream of canonical database persistence.
- The catalog contains no executable SQL literals; database execution remains
  in the database package.

## Implementation acceptance criteria

The catalog becomes fit for its declared purpose when all of the following are
true:

1. Every public concrete repository method has database-backed behavior and
   behavioral tests.
2. WEMI traversal follows the actual FRBR schema: Work/Expression and
   Expression/Manifestation link tables, plus the Manifestation/Item foreign
   key.
3. Titles and identifiers follow the actual storage model rather than assuming
   writable legacy compatibility views.
4. Matchers return deterministic, explained `MatchResult` values and expose
   the methods promised by their protocols.
5. Bundle retrieval and projections are coherent, display-neutral read models.
6. Mutation policy validates levels, existence, payload columns, merge
   direction, and self-merge before any write.
7. Coordinated attachment and merge operations preserve links and are atomic
   at the database boundary.
8. Shared raw-value link writes validate before mutation and resolve/create
   destinations in the same transaction as link replacement.
9. Search and field-metadata public operations have focused behavioral tests
   and no Python 2 mapping calls.
10. Protocols match concrete shapes and catalog is included in strict static
    checking.
11. Legacy mutation surfaces are quarantined, migrated, or explicitly
    deprecated; direct commits do not remain in catalog helpers.
12. Ownership is represented independently of link cardinality.

## Implementation notes

Implementation proceeds bottom-up: a narrow database adapter for generic row
and link operations, concrete repositories, semantic matching, coherent
retrieval, coordinated mutation, then compatibility cleanup. Repository code
must use database APIs or portable macros and must not introduce executable SQL
into catalog.

Behavioral tests must exercise real database fixtures where transaction,
constraint, traversal, or row-shape behavior matters. Fakes are appropriate
only for deterministic policy isolation.

## Implementation outcome

The catalog facade is now fit for its declared core purpose: schema-backed
WEMI persistence, matching, traversal, retrieval, and coordinated mutation.
The older Calibre-oriented helpers remain a deprecated compatibility boundary,
not a second architecture for new work.

All 37 review-time concrete `NotImplementedError` paths now have behavior or
have been replaced with the appropriate input/database exception. The only
remaining `NotImplementedError` statements under `catalog` are the intentional
abstract hooks on `BaseCatalogWriter`, `CatalogValueWriter`, and
`CatalogLinkWriter`; no instantiable catalog service reaches them.

### Acceptance result

| Criterion | Result | Implemented contract |
| --- | --- | --- |
| Concrete repositories | Pass | Generic validated CRUD is implemented once in `BaseRepository`; every Work, Expression, Manifestation, Item, Agent, Identifier, Title, and Note operation has database-backed behavior. |
| Schema-accurate WEMI traversal | Pass | Work/Expression and Expression/Manifestation use discovered many-to-many links; Manifestation/Item uses `item_manifestation_id`. Both directions are covered. |
| Real title and identifier storage | Pass | Titles update their owning WEMI columns rather than the read-only `titles` view. Identifiers use entity-owned rows in `entity_identifiers`. |
| Deterministic matching | Pass | Work, Agent, curated and raw Item Identifier, WEMI, and exact-default value-entity matchers expose stable explained decisions. Tags, Labels, Genres, Subjects, Series, Languages, Ratings, Comments, Synopses, Notes, and Annotations default to exact matching; approximate value policy is explicit opt-in. |
| Coherent retrieval | Pass | Bundles select a deterministic WEMI path and aggregate deduplicated Agents, identifiers, titles, notes, and relationship metadata. Projections remain display-neutral. |
| Mutation validation | Pass | Policy and repository validation reject invalid levels, IDs, self-merges, absent rows, malformed attachment groups, unknown columns, and invalid relationship roles before persistence escapes the transaction. |
| Atomic attachment and merge | Pass | Both enter one portable outer transaction. Merge transfers WEMI relationships, Agents, notes, identifiers, and Manifestation-owned Items before deleting the source. Target relationship metadata wins when an identity already exists. |
| Atomic shared-value writing | Pass | Build is pure. Destination lookup/creation, combined cardinality validation, and link replacement occur inside one transaction; deletion remains find-only. |
| Search and field metadata | Pass | Python 2 mapping calls and the undefined debug branch are removed, restricted all-field search no longer hides arbitrary failures, and both field-metadata implementations expose truthful mapping behavior. |
| Public protocols and static scope | Pass with repository caveat | Protocol composition/signatures match the concrete facade, nested runtime checks pass, catalog is in the configured strict scopes, and isolated strict mypy passes all 63 selected catalog modules. Repository-wide mypy/basedpyright remain red on imported legacy code; see notes below. |
| Compatibility mutation boundary | Pass | `Catalog` is the sole production composition root for the retained `add`, `ensure`, `apply`, and `intralink` tools. Database/library facades remain removed, direct commits were removed from catalog helpers, and new semantics are directed to repositories or normalized writers. |
| Explicit ownership | Pass | `StorageLinkSpec.destination_owned` is independent of cardinality. The factory uses owned-row behavior only when declared or explicitly overridden, and rejects owned plural links. |

### Durable behavior contracts

Repository persistence is routed through the portable macro API. That API now
provides generic row reads, inserts, updates, deletes, and a nestable
transaction context. A real outer transaction owns a dedicated connection;
nested macro operations share it, and failure rolls the whole unit back. Small
legacy adapters which expose only a persistent connection retain supported
savepoint behavior without having that connection closed.

WEMI bundles are deliberately paths rather than complete subgraphs. Where a
many-to-many level offers several candidates, the repository's stable ordering
selects the first. Relationship rows remain available through `_catalog_link`
metadata and the bundle's `links` collection so consumers can make a richer
selection later without changing persistence semantics.

Ordered relationship creation assigns the next available source-side priority
when callers omit one. During a merge, an already-existing target relationship
is authoritative for type, priority, and extras; this avoids inventing a new
ordering for an identity the target already owns.

Logical title IDs are their owning WEMI IDs. `TitleRepository.create()` creates
a Work for compatibility with generic repository callers; level-aware code
should use `add_for_wemi()`. Identifier rows are independently owned. Assigning
an identifier which is already owned by another entity copies the row rather
than stealing it.

The schema-driven shared-value writer stores unresolved value references in
its inspectable update. Those references do not touch the database during
build. Application resolves each distinct value once inside the transaction,
validates the final replacement, and exposes normal persisted link rows as its
result.

### Verification evidence

- `python3 -m pytest -q tests/catalog`: latest full catalog run, 391 passed in
  825.07 seconds, including the extended entity-matching policy.
- `python3 -m pytest -q tests/catalog/test_additional_entity_matching.py tests/catalog/test_matching_policy.py tests/catalog/test_semantic_catalog.py tests/catalog/test_catalog_imports.py`: 84 passed in 127.42 seconds.
- `python3 -m pytest -q tests/catalog/test_semantic_catalog.py tests/catalog/test_field_metadata_and_search.py`: 16 passed in 89.39 seconds on the final semantic implementation.
- `python3 -m pytest -q tests/databases/api/test_portable_macros.py tests/databases/api/test_macros_api_signature_parity.py`: 23 passed and one PostgreSQL-shaped SQLite limitation skipped in 9.41 seconds.
- `python3 -m pytest -q tests/databases/api/test_portable_macros_real_db.py`: 8 passed in 30.91 seconds across SQLite and APSW.
- Focused strict mypy with imported legacy modules skipped: no issues in 63 catalog source files.
- Catalog compilation, runtime protocol checks, the no-executable-SQL AST guard, and `git diff --check` pass.

## Notes after implementation

The configured full-project static checks are not a green repository gate yet.
Strict mypy follows selected modules into legacy packages and currently reports
26,426 errors across 694 files. Basedpyright reports 2,574 errors and 2,602
warnings across the pre-existing metadata/renderer scopes plus imported legacy
dependencies. Catalog's isolated strict mypy surface is clean, but eliminating
that broader baseline is a separate repository programme and should not be
misrepresented as catalog work.

Search and field metadata remain physically located under `catalog` for
compatibility. Their reproduced Python 3 failures are fixed and tested. The
modern ownership decision is now recorded in
`catalog-cache-boundary.md`: Calibre-cache search moves with the remaining
Calibre compatibility code under `utils`, while modern cache reads, indexes,
invalidation, and reconciliation remain owned by `StorageCacheAPI`. Any future
repository-backed Catalog search service would be a separate semantic query
API rather than the owner of Calibre cache state.

The compatibility packages remain in place. Database/library initialization no
longer imports or injects their facades; `Catalog` alone composes the retained
row-oriented metadata tools. They must not receive new semantics, which belong
behind repository, coordinated-mutation, or normalized-writer contracts.
