# Native metadata access

Implemented **2026-11-18**. The bound
[MetadataAPI](../../src/LiuXin_alpha/caches/metadata/service.py) implements
[MetadataAPIInterface](../../src/LiuXin_alpha/catalog/api/metadata_access.py)
under the existing `caches.metadata` package. Catalog owns validation and persistence;
the adapter coordinates metadata reads, mutations and cache freshness.

The implementation is grouped under `caches.metadata` as of **2026-11-20**:

- `cache.py`: `CatalogCache`, requiring and borrowing a loaded generic cache.
- `writers.py`: schema-directed Catalog writer binding and physical dependencies.
- `service.py`: the bound `MetadataAPI`, WEMI services and coordinated mutations.
- `reads.py`: complete reads and detached metadata projections.
- `read_operations.py`: complete composed reads, capture lifetime and whole-operation fallback.
- `record_reads.py`: shared bulk reads, exact lookup and literal suggestions.
- `search.py`: semantic search execution, exact pages and provenance.
- `shared.py`: Agent and vocabulary mutations and shared edit snapshots.
- `relationships.py`: WEMI credits and Work Tag/Series conveniences.
- `trees.py`: vocabulary traversal and coordinated hierarchy mutations.
- `records.py`: identifiers, text attachments and Item-scoped annotation services.
- `custom_fields.py`: native custom-field administration and reconciliation.

The package exports `CatalogCache` and `MetadataAPI`. As of **2026-11-21**,
`CatalogCache(catalog, cache=core)` is the explicit metadata-aware facade. The
former `Cache.metadata` and core writer methods are removed; the core remains
metadata-independent. `MetadataAPI(catalog)` still supports uncached operation,
and its explicit optional-cache constructor remains available.
The metadata contracts remain under `catalog.api`. The cache-wide descriptor registry and its
publication coordinator remain in `caches.field_metadata` and
`caches.custom_fields`.

## Composition

```python
from LiuXin_alpha.caches.cache import create_cache
from LiuXin_alpha.caches.metadata import CatalogCache, MetadataAPI
from LiuXin_alpha.catalog.catalog import Catalog

cache = create_cache(database, "schema_backed")
catalog = Catalog(database)
metadata = CatalogCache(catalog, cache=cache)

created = metadata.works.create({"title": "The Metadata Garden"})
work = metadata.works.require(created.result)
matches = metadata.search.find("work.title:Garden")

# Explicit composition can reuse an existing Catalog and select fallback once.
metadata = CatalogCache(catalog, cache=cache,
                       allow_database_fallback=True)

# The same interface is available without a cache.
uncached = MetadataAPI(catalog)
```

`CatalogCache` requires a loaded `Cache` or `ReadCache` for the same database
instance as Catalog. It implements `MetadataAPIInterface` and exposes entity
services directly: `metadata.works`, `metadata.search`, `metadata.vocabulary(...)`
and the other existing members. `metadata.cache` returns the exact borrowed core
for physical queries and explicit lifecycle operations. No second snapshot,
index set or lock is created.

Database fallback is disabled by default and selected once at construction.
Every operation checks attachment and readiness again, including calls through
retained entity services and schema writers. The composing caller closes the core
and database; the facade owns neither resource. Direct Catalog writes continue
to need explicit invalidation of snapshots. CoreServices constructs `CatalogCache`
when a cache is configured and `MetadataAPI` otherwise.

Schema-directed writes have moved to `metadata.create_writer(...)`,
`metadata.write(...)` and `metadata.write_one(...)`. Generic `CacheAPI` now exposes
`reconcile(receipt, tables=..., links=..., fields=...)`; the upper layer supplies
those physical dependencies after interpreting Catalog's result. See the
[Catalog/cache boundary](../catalog/catalog-cache-boundary.md) for the architecture
and migration rationale.

Schema writer guards cover value resolution and transaction acquisition as well
as persistence and reconciliation. A successful write inside an enclosing owned
macro transaction makes the cache dirty for every thread, including live-backed
caches. Reads, reloads and competing writers reject pending publication; the
owning thread can continue writing. Commit or rollback releases the guard, and
the next read refreshes the recorded dependencies. Savepoint rollback keeps the
guard until the outer unit completes. Closing or clearing during that period
does not let a completion callback reopen the cache. Borrowed raw transactions
are rejected because their completion cannot be observed.
Before a read, reload or write starts, the coordinator acquires the database lock before retaining
the cache lock. It never waits for an enclosing database owner while holding the
cache lock that owner needs for its first mutation or completion. Pending cache
publication is checked before that wait so competing operations still reject promptly.
This is a coarse per-handle boundary, including for reads that could otherwise use
only a snapshot. Clear and close retain their independent in-memory lifecycle lock.

## Core metadata coverage

Expanded **2026-11-21**. Named properties cover every metadata repository whose
records have independent identities; embedded titles are edited through their
WEMI owners. The interface is a semantic boundary, not a mirror of every SQL
relation. Physical link tables, workflow state, storage, provenance infrastructure
and schema bookkeeping do not receive generic metadata CRUD services.

| Domain | Entry point | Operations and identity policy |
| --- | --- | --- |
| Works, Expressions, Manifestations, Items | `works`, `expressions`, `manifestations`, `items` | Existing native hydration, CRUD, edit preconditions and batch reads |
| Agents, person/organisation aggregates | `agents` | CRUD, typed aggregate creation and explicit rename; names alone never imply reuse |
| Tags, Labels | `tags`, `labels`; `ensure_tag(text)`, `ensure_label(text)` | Named CRUD; ensure uses exact normalized identity, retaining punctuation |
| Genres, Subjects, Series | `genres`, `subjects`, `series`; corresponding `ensure_*` | Named CRUD; ensure accepts `parent_id`, with `None` explicitly selecting a root; schema uniqueness still applies |
| Ratings | `ratings`; `ensure_rating(value, out_of=..., source=...)` | CRUD; ensure requires a scale and a nonblank source to avoid accidental reuse across contexts |
| Languages | `languages` | Detached reads, bulk reads, lookup and suggestions; seeded constants expose no mutation methods |
| Curated identifiers | `identifiers` | CRUD, `list_for_wemi`, `list_for_agent`, `link_to_wemi`, `link_to_agent`; assignment may copy a row and return a different ID |
| Item identifier observations | `item_identifiers` | CRUD and `list_for_item`; require a live Item and nonblank raw scheme/value; preserve observed spelling |
| Notes, Comments, Synopses | `notes`, `comments`, `synopses` | CRUD, atomic `add_for_wemi`, and `list_for_wemi`; attachment creates fresh text under Catalog cardinality rules |
| Annotations | `annotations` | CRUD and `list_for_item`, optionally filtered by user/kind; require a live Item, kind and anchor; equal text is not automatically reused |
| Logical titles | WEMI scalar updates | Titles belong to Works, Expressions or Manifestations; no independent title identity or writable compatibility view is introduced |
| Native custom fields | `custom_fields` | Existing schema, value, revision and migration services |

`vocabulary(kind)` remains the dynamic selector for the same named vocabulary
services. Its language writes still raise Catalog's read-only error. Other record
reads return detached `SharedMetadataRecord` values containing the kind, integer
identity and public field aliases. Text attachment lists contain distinct records
in ID order; use WEMI hydration for relationship order and link attributes.
All owner-scoped reads require an existing owner, distinguish unavailable cache
coverage from an empty result, and share one captured read source.

For native SQL live storage, that source loads data on demand within the read
transaction. Point lookups capture individual identities; filters and pickers
load the populations they inspect; relationship reads capture their physical link
table. The temporary view belongs to one operation and is never reused to hide
external changes from a later call. A caller's managed transaction retains its
own isolation and completion ownership. See the
[capture measurements](metadata-followups-2026-11-22.md) for remaining costs.

```python
tag = metadata.ensure_tag("History")
metadata.tags.link_to_work(entity_id=tag.result, work_id=work_id)
note = metadata.notes.add_for_wemi(
    level="work", entity_id=work_id, text="Check the publication date."
)
metadata.annotations.create({
    "item_id": item_id,
    "kind": "bookmark",
    "anchor_type": "page_offset",
    "anchor_start": "12",
})
observations = metadata.item_identifiers.list_for_item(item_id)
```

Ensure operations use Catalog's authoritative exact matcher under a serialized
writer reservation. They never decide to create from a cached miss, apply fuzzy
reuse, merge Agents, or overwrite an existing row's spelling/attributes. Reuse
returns `changed=False` and does not advance the cache generation. Ambiguity or
conflicting evidence raises before insertion. `metadata.batch(...)` supplies the
required serialized boundary; an ordinary caller-owned transaction cannot be
upgraded for ensure and is rejected. Cooperating callers share that reservation;
arbitrary external SQL writers remain outside the application locking contract.
Direct Catalog/SQL writes still require their own cache invalidation, including
when a subsequent ensure reuses the externally created row.

Core-record writes retain the existing receipt, pending transaction, rollback and
post-commit reconciliation contracts. Catalog validates Item-owned writes even
though the physical schema permits incomplete placeholder rows. Matching and
normalization stay in Catalog; the generic cache remains metadata-independent.
This extension does not optimize Catalog's exact matcher or the conservative
full-refresh write policy.

## Reads and writes

`works`, `expressions`, `manifestations` and `items` provide `get`, `require`,
`get_many`, `read_for_edit`, ID-ordered `list`, `create`, `update` and `delete`. Reads return the existing native
metadata containers. Identity, relation targets and nested link payloads are copied
through the containers' serialization contract; returned values do not retain live
database Rows. Editing a value is an in-memory operation, not a write.

`search.find` compiles Catalog's native search semantics and returns
`MetadataReference` values. A subsequent hydration is a separate read. Cache
operations hold the owning cache lock for the whole page or metadata projection.
Live backends supporting `query_view` capture their read state once per operation,
avoiding repeated catalogue rebuilds for every relationship. Composed metadata
reads now borrow the caller's managed transaction or own a read-only snapshot,
closing its connection on completion. This also prevents a standalone PostgreSQL
search from retaining table locks that block later custom-field DDL. Composed
reads reuse the driver's schema-discovery scope, validating its fingerprint at
entry and exit instead of probing for every hydrated row. A live capture
still reads the complete catalogue, and hydration traverses related metadata;
these operations do not have the cost of an individual cached-row lookup.

Known root misses return `None`; `require` raises `CatalogNotFoundError`. Partial
or unavailable cache reads raise unless fallback was explicitly enabled. A known
miss never triggers fallback. Corrupt reads and missing referenced records raise.
The existing Catalog hydrators now offer opt-in strict error propagation; their
older best-effort callers keep their previous behavior.

Write payloads use Catalog's public aliases. The public entity protocols now take
the corresponding `WorkCreate`/`WorkUpdate`, `ExpressionCreate`/`ExpressionUpdate`,
`ManifestationCreate`/`ManifestationUpdate` or `ItemCreate`/`ItemUpdate`
[TypedDicts](../../src/LiuXin_alpha/catalog/api/metadata_payloads.py). Optional keys
preserve omitted-versus-cleared intent: omission retains an existing value, while
`None` requests NULL subject to schema constraints. Types describe portable scalar
aliases; Catalog still validates dynamic callers and physical constraints at runtime.

`mutations` implements
`create_wemi_stack`, `attach_metadata`, `replace_metadata`, `link_wemi` and
`unlink_wemi`, including the existing distinction between omitted and cleared
attachment groups. It delegates those semantics to Catalog inside one supported
macro transaction. There is no forwarding escape hatch to raw repository writes.

The initial adapter conservatively refreshes the complete snapshot after a
committed semantic write. Live backends advance their query generation. Targeted
refresh is a later optimization requiring evidence that every affected identity,
relationship and projection is covered.

## Batch reads and writes

`works.get_many(ids)` hydrates one captured read source and returns values in the
requested order. Duplicate IDs remain duplicate positions, with independently
detached values. Missing roots occupy `None` positions; `missing="raise"` instead
rejects the entire call with `CatalogNotFoundError`. Unavailable or corrupt data
never becomes a missing result. An empty request avoids capturing the catalogue.
The same method is available on all four WEMI families. Live capture cost is paid
once per call, although each distinct root still requires its own hydration.

`metadata.batch(callback)` runs dependent edits in one owned transaction and
performs one final full schema/data reconciliation. The callback receives the
same public interface and can use creation results in later writes:

```python
def create_and_describe(api):
    work_id = api.works.create({"title": "A new work"}).result
    api.mutations.attach_metadata(
        level="work", entity_id=work_id, data={"notes": ["Initial description"]}
    )
    return work_id

receipt = metadata.batch(create_and_describe)
```

Child receipts remain `pending`; the aggregate receipt reports commit and cache
reconciliation. An escaping callback exception rolls back the whole batch. A
caught failed child operation has its own savepoint, so the callback can choose
to continue with other edits. Nested batches compose; an external managed outer
transaction retains commit/rollback ownership. Cache reads after edits remain
deferred until that outer unit completes. Use returned IDs rather than reading
an uncommitted cache. Invalidations may advance the generation more than once;
the guarantee is one final refresh, not one generation increment.

## Optimistic editing

`read_for_edit(id)` opens a consistent database snapshot and returns a
[MetadataSnapshot](../../src/LiuXin_alpha/catalog/api/metadata_revisions.py):
detached native metadata, public scalar `fields`, and a matching `revision` token.
The explicit scalar mapping includes writable aliases not represented by older
identity containers. This operation deliberately reads authoritative storage,
even when ordinary metadata reads use a cache snapshot.
Inside an existing managed transaction, it instead borrows that transaction and
inherits its isolation and visibility. It does not upgrade the caller's isolation.

```python
observed = metadata.works.read_for_edit(work_id)
metadata.works.update(work_id, {"title": "Edited title"},
                      expected_revision=observed.revision)
```

`update` and `delete` accept optional `expected_revision`. For compound changes,
use `metadata.batch(callback, expected=(observed.revision, ...))`. Every token is
checked before invoking the callback. A changed or deleted root raises
`MetadataConflictError` with expected and actual tokens; a mismatched entity token
is invalid input. Checks always use the database, including when a direct Catalog
write has left the cache stale. Tokens cover the complete root row and hydrated
relationships. They are content fingerprints, not durable edit counters: changing
and restoring the same complete content can produce the same token. They are
scoped to the originating catalogue and current API encoding, and are not cache
generations. Custom values retain their separate field-wide native revisions.

The optional conditional-write path reserves the database writer before comparing
content. SQLite already reserves a writer for owned macro writes. PostgreSQL
reserves catalogue tables in a deterministic order until the transaction ends;
this also blocks uncooperating row writes and relationship inserts. PostgreSQL
preconditions require READ COMMITTED isolation; a pre-existing repeatable-read
snapshot is rejected. This coarse initial reservation prioritizes correctness
and can reduce write concurrency. It does not reserve external file bytes.

## Shared identities and custom fields

`metadata.agents` provides detached public-field reads, CRUD, person/organisation
creation and explicit name/sort-policy renaming. `metadata.vocabulary(kind)`
selects `tags`, `labels`, `genres`, `subjects`, `series`, `languages` or `ratings`.
These services retain Catalog's normalization and deletion rules; Languages stay
read-only seeded constants. Tags and Series additionally expose `link_to_work`
and `unlink_from_work`, preserving retained membership metadata. Shared edits
reconcile affected WEMI projections and search. No implicit matching or merging
is introduced. Detailed Agent subtype editing remains with Catalog's existing
aggregate services.

`metadata.custom_fields` exposes native field definitions, stored values, atomic
patches, queries, inventories, shared-value rename/delete, migration planning and
application, retirement, purge planning/application and audit. It preserves native
`FieldDefinition`, `OwnerRef`, `FieldPatch`, `ChangePlan` and field-wide revision
semantics. Those native reads use authoritative native-service snapshots. Writes
return a `MetadataWriteReceipt` containing the native result; schema changes
perform a full cache reload. A plan is revalidated by the native service when it
is applied. Retirement retains values; applying an explicit purge permanently
removes them. Composite template evaluation remains Catalog-owned.

Native service listeners can fail after SQL commits. The transaction owner now
reports that condition as `CommittedTransactionError`; the metadata adapter
retains its completed result in `MetadataReconciliationError` and marks any bound
cache dirty. This is a committed write, including without a cache, and must not
be repeated merely because publication failed.

## Core consumer workflow

`CoreServices.metadata` lazily binds the interface to Core's existing Catalog,
cache and fallback policy. Four named routes exercise native search/display/edit:

| Route | Result |
| --- | --- |
| `metadata.search` | Typed-filter search page, exact total and provenance |
| `metadata.read_many` | Ordered native metadata mappings, including missing slots |
| `metadata.read_for_edit` | Authoritative metadata, scalar fields and revision |
| `metadata.update` | Partial scalar edit with optional revision precondition |

The read routes require the existing `read` transport capability and updates
require `write`. Core reports `metadata_conflict` with both tokens, and preserves
the receipt and actual durability flag for `metadata_reconciliation_failed`.
These writes do not pass through Core's older reconciler for a second refresh.
The existing item-centred `metadata.get`/OPF payload remains a separate contract;
other Catalog endpoints have not yet migrated to this interface.

## Picker queries and shared bulk reads

Added **2026-11-22**. All shared record readers support `get_many(ids,
missing="none")` and `lookup(fields, limit=100, offset=0)`. Bulk reads preserve
input order and duplicate slots, copy each returned value independently, and
return `None` for confirmed misses. `missing="raise"` rejects an incomplete set.
Lookup accepts public scalar aliases, ANDs them, and returns every matching
candidate in ID order. Unknown fields, incompatible scalar types and conflicting
aliases fail before reading the record population. `None` means SQL NULL, so
`genres.lookup({"name": "History", "parent_id": None})` selects root Genres;
omitting the parent key searches every parent scope. Lookup never chooses or
creates an identity.

Agents, Tags, Labels, Genres, Subjects, Series and Languages additionally support
`suggest(text, mode="prefix", limit=20, offset=0, filters=None)`. Modes are
`exact`, `prefix` and `contains`. Suggestions use Catalog's literal Unicode and
whitespace normalization and each field's case policy, preserve punctuation and
accents, and include parsed Agent aliases. Empty normalized text returns no
matches. Candidate ordering uses normalized display text, original display text
and ID; aliases influence matching, not the displayed identity. Ratings and
context-owned records have lookup but no suggestion method.

```python
page = metadata.agents.suggest("Le Guin", mode="contains", limit=20)
scoped = metadata.genres.suggest("hist", filters={"parent_id": parent_id})
records = metadata.tags.get_many((tag_id, missing_id, tag_id))
```

Lookup and suggestion return `SharedMetadataPage`, containing `records`, an
exact `total_count`, `offset`, `limit`, `has_more`, `source`, `consistency` and
`generation`. Zero limit still computes the total. These operations currently
scan the shared table population; a small result limit is not a bound on scan
cost. Separate calls do not pin a snapshot across offset pages.

## Hydrated search pages

All four WEMI services expose `search(query="", filters=(), sort=(), limit=100,
offset=0)`. `works.search(...)` returns
`HydratedMetadataSearchPage[WorkMetadataAPI]`; the other levels return their own
metadata types. Each page includes the existing reference page fields plus a
`records` tuple aligned with `references`.

```python
page = metadata.works.search("work.title:Garden", limit=25)
for reference, work in zip(page.references, page.records):
    assert reference.entity_id == work.work.work_id
```

Selection, exact count, ordering and hydration share one captured source under
the metadata read lock and transaction. If an unavailable cache capability
triggers explicitly enabled fallback, the entire operation restarts against the
database and reports database provenance. It never combines cached references
with database hydration. Temporary captures close on success and failure.
Confirmed misses, corrupt rows and dirty/pending state are not fallback signals.
The existing `metadata.search.find` and `metadata.search.query` remain the lighter
reference-only entry points.

## Owner-oriented memberships and credits

`works.list_tags(work_id)` and `works.list_series(work_id)` return detached
`MetadataMembership` values: `member`, `link_type`, and `priority`. Use
`add_tag(work_id, tag_id=...)` for an existing Tag, or
`add_tag(work_id, text="History")` to ensure and attach in one transaction. Supply
exactly one selector. A failed attachment rolls back a Tag created by that call.
`remove_tag`, `set_tags`, `add_series`, `remove_series` and `set_series` use
existing IDs. Empty replacement clears membership; duplicate IDs are rejected.
Retained memberships preserve every type, priority, physical link identity and
provenance field. Removing membership keeps both endpoint records.

Series links can carry several types for the same pair. These methods operate
on the membership set; priorities are storage ordering values, not volume numbers
or a preferred-Series ranking. Input order to `set_tags` or `set_series` does not
reorder retained links. Genre and Subject services do not expose Work membership.

Every WEMI service supports `list_credits`, `add_credit`, `remove_credit` and
`set_credits`. Credits contain a detached Agent record, an explicit `role`, and
`priority`. Add/remove require an existing `agent_id`; no name matching occurs.
`set_credits(entity_id, role="aut", agent_ids=(first_id, second_id))` replaces only
that role in the supplied display order. Removing an author's credit does not
remove the same Agent's editor credit. Empty replacement clears that role only.
Catalog allocates automatic priorities under its metadata writer reservation,
using a free range across the physical link table so a shared Agent can be
credited on multiple owners. Explicit priorities still obey storage constraints.

Credit insertion and replacement require a schema whose link identity includes
both endpoints and the role. Incompatible schemas raise `CatalogMutationError`
before either operation writes. Newly created PostgreSQL schemas now use that
same role identity as SQLite and declare `series_parent_position` for tree
operations. Existing PostgreSQL schemas are not migrated by this change; they
need a separate schema upgrade before using the affected features.

These methods return the standard persistence/cache receipt. Add/remove Tag or
Series return its ID; replacement returns the supplied ID tuple. Credit writes
return the WEMI `MetadataReference`. No-op requests report `changed=False` and do
not publish a new cache generation. Every owner-oriented write accepts the
owner's optional WEMI `expected_revision`.

## Shared record edit preconditions

Mutable shared services expose `read_for_edit(id)`, returning a
`SharedMetadataSnapshot` with detached `value`, scalar `fields` and a
`SharedMetadataRevision`. Update, delete and Agent rename accept this token as
`expected_revision`. Languages have neither edit snapshots nor write methods,
including through `vocabulary("languages")`.

The shared digest covers the complete stored root row, including fields not
exposed by public aliases. It excludes incoming relationships, subtype rows and
descendant rows. It is a content token, not an edit counter: restoring the same
content can restore the same digest. It is separate from existing WEMI token
classes and cache generations. Use tokens only with the bound catalogue where
they were obtained. Hierarchy movement changes a node's stored row, but a shared
token does not assert that an entire tree or all its attachments are unchanged.
`delete_subtree` has no whole-tree revision parameter.

```python
observed = metadata.tags.read_for_edit(tag_id)
metadata.tags.update(tag_id, {"name": "Historical fiction"},
                     expected_revision=observed.revision)
```

Preconditions read authoritative storage under the writer reservation. Stale
cached values cannot approve a stale edit; a changed or deleted row raises
`MetadataConflictError` before mutation. `batch(..., expected=(...))` accepts
mixed shared and WEMI tokens. To protect both a Tag rename and its Work membership
edit, include the Tag token and the Work token. Existing rollback, pending
transaction and post-commit reconciliation error contracts apply unchanged.

## A complete metadata editing workflow

This uses only public metadata operations; the caller composes and owns the
Catalog/cache resources as described above.

```python
work_id = metadata.works.create({"title": "The Metadata Garden"}).result
agent_id = metadata.agents.create_person({"name": "Garden Writer"}).result
series_id = metadata.ensure_series("Garden Library").result

metadata.works.add_tag(work_id, text="Gardening")
metadata.works.add_series(work_id, series_id)
metadata.works.add_credit(work_id, agent_id=agent_id, role="aut")
candidates = metadata.tags.suggest("gard")
page = metadata.works.search("work.title:Garden")
observed = metadata.works.read_for_edit(work_id)
tag = metadata.tags.read_for_edit(candidates.records[0].entity_id)

def edit(api):
    api.tags.update(tag.value.entity_id, {"name": "Practical gardening"})
    api.works.set_tags(work_id, (tag.value.entity_id,))
    api.works.set_credits(work_id, role="aut", agent_ids=(agent_id,))
    return work_id

receipt = metadata.batch(edit, expected=(observed.revision, tag.revision))
```

## Vocabulary trees

Genres, Subjects and Series expose `HierarchicalVocabularyMetadataInterface` on
their named properties and on literal `vocabulary(...)` selections. Catalog owns
the tree rules and durable writes; the metadata facade supplies cached reads and
reconciliation. WEMI graph construction remains on `mutations`.

```python
from LiuXin_alpha.catalog.api.metadata_trees import VocabularyTreeInput

path = metadata.genres.ensure_path(("Fiction", "Science fiction", "Space opera"))
fiction, science_fiction, space_opera = path.result

created = metadata.subjects.create_tree(VocabularyTreeInput(
    {"name": "History"},
    children=(VocabularyTreeInput({"name": "Ancient history"}),),
))
root_id = created.result[0]
branch = metadata.subjects.subtree(root_id)
metadata.genres.move(space_opera, parent_id=fiction, before_id=science_fiction)
metadata.genres.reorder(parent_id=fiction, entity_ids=(science_fiction, space_opera))
```

`ensure_path` takes literal name components, never a separator-delimited string.
It reuses exact matches within each parent scope, preserves reused attributes and
ordering, and returns component IDs. `create_tree` creates every supplied node
fresh and returns preorder IDs. Nested payloads cannot specify parent or position;
the input structure supplies them. Both operations are atomic, default to root
scope (`parent_id=None`), and accept `max_nodes=10000`. Series names remain globally
unique: a Series in another branch is a conflict, never an implicit move.

`roots(limit=100, offset=0)` and `children(id, limit=100, offset=0)` return ordered
pages. `ancestors(id, max_nodes=10000)` returns root-to-parent records, excluding
the starting node; `subtree(id, max_nodes=10000)` includes the selected root and
returns depth-first preorder. Records are detached. An overflow raises
`HierarchyLimitError` without a partial result. Missing parents and cycles raise
integrity errors. Cached traversals capture one source; explicit fallback reruns
the entire traversal against the database. Known misses do not cause fallback.
The node budget bounds traversal and output. Native live views load the hierarchy
tables used by the traversal on demand; the budget does not bound those underlying
table populations.

Sibling reads sort by stored position, nulls last, then ID. Structural writes
compact affected siblings to zero-based positions. New children append; input
child order is preserved. `move(id, parent_id=..., before_id=None)` appends unless
a destination sibling is named. `reorder` requires the complete sibling set,
including root ordering when its parent is None. Ordinary `create` and `update`
share parent validation and cycle checks; position aliases are insertion indices.
Stored `full` labels and `tree_id` values remain caller-supplied metadata and are
not authoritative paths or automatically maintained ancestry. Moving a node does
not rewrite imported labels. Series membership priorities remain separate from
Series sibling positions.

Deletion is deliberately explicit and tighter than the physical schema:

- `delete(id, detach=False)` accepts leaves only; children are neither cascaded
  nor promoted. Move children first or use `delete_subtree`.
- `delete_subtree(id, detach=False, max_nodes=10000)` removes a complete bounded
  subtree and returns deleted IDs child-first.
- Existing metadata attachments block both operations. `detach=True` removes
  declared relationship rows atomically, retaining their other endpoint entities.
  Unsupported references and references to attachment rows block deletion.
  Reference inspection also has a 100,000-row safety budget.

All writes retain normal receipts, batch/outer-transaction ownership and committed
refresh-failure handling. No-op path reuse, moves, reorders and equal-value edits
report `changed=False`. Direct Catalog writes still require explicit cache
invalidation. These contracts do not migrate stored rows or change schema-level
foreign keys; physical database access remains a separate lower-level API.

Validation on **2026-11-21**: all 47 mandatory tree regressions passed, covering
the three vocabularies, four read modes, SQLite/APSW mutations and independent
database handles. Four gated stress smoke cases passed; ordinary runs skip them.
The combined affected-runtime and documentation/tooling checks passed 203 distinct
cases. Formatting, lint, architecture/import budgets, mypy, basedpyright and both
negative-contract suites passed. Full-size stress and live PostgreSQL were not run.

## Search requests and pages

`search.find()` remains the ID-ordered reference convenience. The richer
`search.query()` accepts immutable
[request values](../../src/LiuXin_alpha/catalog/api/metadata_search.py) and returns
a page with references, an exact total before pagination, offset/limit,
`has_more`, result level, execution source, consistency and generation:

```python
from LiuXin_alpha.catalog.api.metadata_search import (
    MetadataSearchFilter, MetadataSearchRequest, MetadataSearchSort,
)

request = MetadataSearchRequest(
    query="author:Herbert",
    level="work",
    filters=(MetadataSearchFilter("year", "gte", 1960),),
    sort=(MetadataSearchSort("title"),),
    limit=25,
)
metadata.search.validate(request)
page = metadata.search.query(request)
```

Filters are ANDed with the existing Boolean text query. Text filters use Catalog's
locations, aliases and normalized `contains`/`eq` semantics. Their operands are
literal values, including quotes, parentheses, colons and leading `=` characters.
Numeric filters add `id` and `year` on the result entity, or an explicitly scoped
`work.year`, `expression.id`, etc. Items have no own year. Supported comparisons
are `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, integer-tuple `in`, and boolean `is_null`.
Numeric fields belong to the typed filter vocabulary; this does not add numeric
operators to the existing text parser.

`search.fields(level=...)` discovers filters and aliases; `search.sort_fields()`
discovers scalar ordering keys. Sorting uses the returned entity's own value,
with no inherited title, preferred edition or author aggregation. Missing values
sort last in either direction; ascending entity ID breaks complete ties. An
explicit ID sort may choose descending IDs. An empty sort is ID ascending.
Text ordering compares normalized text and then original text before the ID tie.
Independent filters may match different reachable entities, just as existing
Catalog Boolean terms do. WEMI traversal never turns through a shared child to
borrow a sibling Work's metadata.

Validation checks syntax, levels, operands, sorting and paging without reading
schema or rows. `MetadataSearchValidationError.parameter` identifies the failing
input, such as `filters[0].value`. Execution still checks backend availability and
reference integrity. A zero-sized page still computes the total; an offset past
the end returns an empty page with that same total. Hydration remains explicit.

Cached pages carry `source="cache"` and the owning cache's generation. Uncached
execution and explicit fallback carry `source="database"`, `consistency="live"`
and `generation=None`. Fallback reruns the whole request; it never combines cached
references with a database count. A known empty result does not trigger fallback.
Partial or unavailable execution can use the bound fallback policy; corruption,
dirty transactions and failed recovery remain errors.

On databases with normalized SQL search, the existing macro selects all matching
root rows, then the shared query engine orders/counts/pages that population. This
avoids duplicating collation and missing-value rules but retains memory
proportional to the matches. Other drivers use one ephemeral database capture;
native live cache views load the tables and relationship routes the query uses
on demand. Both paths can still load complete populations, so this is not
bounded-memory SQL pagination. Database execution
uses the metadata owner's read scope; separate page
requests do not pin a shared snapshot. A generation is an invalidation counter,
not a database revision.

## Receipts and recovery

Every write returns `MetadataWriteReceipt`, separating persistence from cache
visibility:

| Situation | Persistence | Cache |
| --- | --- | --- |
| Owned transaction completes without a cache | `committed` | `not_configured` |
| Owned transaction completes and cache reconciles | `committed` | `current` |
| Caller owns an enclosing managed transaction | `pending` | `deferred`, or `not_configured` |
| Persistence completes but refresh fails | `committed` | `dirty`, carried by `MetadataReconciliationError` |

`changed` records an applied write request, not a comparison of whole metadata
graphs. Supplied updates and replacements can report true even when their values
already match. Empty entity updates and absent relationship removals report false.
The result and cache generation are return-time evidence; pending receipts never
become committed in place.

Deferred writes mark even a live cache dirty. Reads and explicit refreshes refuse
to publish while the outer managed transaction is open, including from other
threads. Completion releases this guard after either commit or rollback; the next
read recovers the cache. Rolling back an inner savepoint retains the guard until
the outer transaction finishes. Completion never reopens a cleared or closed cache.
Borrowed raw transactions are refused before mutation because their completion
cannot be observed through the macro boundary. Transaction capability errors do
not silently reduce atomic operations to partial writes.

`MetadataReconciliationError.receipt` retains the successful Catalog result and
its persistence status. A failed refresh after commit reports `committed`; a
failed reconciliation inside the caller's transaction still reports `pending`.
Do not repeat a committed write. The next successful cache refresh recovers the
dirty state; repeated refresh failures remain visible.
Catalog or SQL writes made outside this adapter still need explicit invalidation.

## Scope and validation

The **2026-11-21** core-record expansion adds [25 mandatory cases](../../tests/databases/caches/test_metadata_core_records.py).
They pass across uncached, schema-backed, database-backed and NumPy caches, with
SQLite/APSW coverage for contextual CRUD and independent-handle concurrency.
Checks include normalized reuse, ambiguity, parent/scale/source scope, detached
records, owner validation, rollback, pending receipts, post-commit recovery and
explicit fallback. The current manifest requires **2,983 cases**.
The affected Catalog suite passes **89 cases**, documentation/tooling checks pass
**38**, and the full quality runner passes both type checkers, positive/negative
contracts, formatting, lint and architecture guards. No live PostgreSQL or long
soak lane was rerun for this expansion.

[Behavior tests](../../tests/databases/caches/test_metadata_api.py) cover the same
typed consumer with no cache and each native backend, detached values, CRUD,
attachment replacement, hierarchy links, transaction commit/rollback, failed
refresh, unavailable reads and retained bindings. The
[static contracts](../../tests/typing/internal_contracts.py) check both the
standalone adapter and `CatalogCache` against the public protocol.

The [search tests](../../tests/databases/caches/test_metadata_search.py) add
SQLite/APSW and native backend parity, exact totals and stable sorting, numeric
and literal text filters, WEMI path isolation, discovery without data access,
structured validation errors, complete-page fallback and transaction guards.
All 44 search cases are mandatory. That slice raised the manifest to 2,918 cases;
the extensions below preserve those requirements and raise it to **2,956 cases**.

The shared metadata fixture skips APSW cases when that optional driver is absent.
The mandatory database CI lane installs APSW and requires those cases to pass;
the ordinary full-suite lane can still run without it.

The **2026-11-19** extension adds 38 mandatory cases covering ordered batch
hydration, atomic edits, shared metadata, native custom-field lifecycles,
authoritative edit tokens, independent writers and the Core consumer workflow.
Six additional PostgreSQL cases are gated by `LIUXIN_TEST_POSTGRES_URL`; they use
disposable schemas and cover the four cache modes, blocking an independent raw
SQL writer during a checked edit, and rejecting an older repeatable-read snapshot.
Final validation on **2026-11-19**:

- The combined metadata, search, custom-field, transaction and Core regression
  suite passed **192 tests**; all **149 selected mandatory cases** passed setup,
  call and teardown.
- After the schema-read scope improvement, a current-source follow-up passed
  **39 tests**, with all 39 mandatory phase audits passing. Its 539 saved source,
  test and manifest hashes match the final tree. This run overlaps the broad run;
  its count is not additional unique coverage.
- All **six live PostgreSQL 16 tests** passed, including raw-writer blocking and
  isolation rejection. The disposable test server was shut down afterward.
- Formatting, lint, architecture/import checks and both type checkers passed
  (mypy: 554 files), including 115 basedpyright and 111 mypy rejected invalid
  examples. The tooling run passed 279 checks with one initial untracked-link
  failure; after tracking the new paths, all 16 documentation checks passed.

Local evidence is retained under `/tmp/liuxin-metadata-extensions-validation` and
`/tmp/liuxin-metadata-extensions-current`, with PostgreSQL output in
`/tmp/liuxin-metadata-postgres-final.log`. These temporary artifacts are not part
of the portable documentation. These are correctness checks, not performance
benchmarks or whole-module sign-off. Older results below describe their original
slices, not these additions.

Search validation on **2026-11-18**: **84 tests passed** across the new search
cases, retained metadata read/fallback contracts and existing Catalog search
regressions. All **47 selected mandatory cases** passed setup, call and teardown;
459 saved source/test/manifest hashes match the validated tree. Formatting,
lint, dependency checks and both type checkers passed (mypy: 533 files), as did
the corrected negative typing fixtures (113 basedpyright / 109 mypy cases).
All **49 tooling/documentation tests** passed. Developer navigation covers 224
guides and 1,918 local links in the proposed snapshot. Run artifacts are retained
locally at `/tmp/liuxin-metadata-search-final`; those temporary files are not part
of the portable documentation or repository.

Adapter validation on **2026-11-18**: the combined metadata, cache lifecycle, hydration and
transaction-publication run passed **249 tests**, with six expected existing
backend-capability skips. All **61 selected mandatory cases** passed setup, call
and teardown, including the **47 new metadata cases**. That run covered the
previous 2,874-case manifest. Transaction coverage uses
SQLite and APSW, with cross-thread deferral and nested rollback exercised against
each native cache backend.

The adapter's configured quality runner passed formatting, lint, annotation coverage,
dependency/import checks, basedpyright and mypy (530 files), plus both negative
type-contract lanes. All **49 tooling/documentation tests** passed. The final run
retains matching hashes for 464 source, test and manifest files. These are
correctness results, not performance measurements or live PostgreSQL validation.

The native Core workflow now consumes this interface; other Core semantic handlers
retain their existing transport and receipt conventions. Matching and merges remain
separate Catalog services. The transaction guard now also protects schema-directed
writer deferral across threads; see the [follow-up review](metadata-followups-2026-11-22.md).
This is not whole-cache or whole-Catalog sign-off.
See [next tasks](next_tasks.md).



## Ergonomics validation, 2026-11-22

The implementation and focused follow-ups pass **393 distinct tests**: 39 new
mandatory ergonomics cases, 274 existing cache regressions, six existing Catalog
credit checks, four gated smoke workloads, five live PostgreSQL cases and 65
documentation/tooling cases. A final 22-case documentation follow-up overlaps that
last group. PostgreSQL covers uncached reads, all three cache backends and an
independent shared-token writer; its disposable cluster was stopped afterwards.

The regression manifest retains all 3,030 previous requirements and adds the 39
new cases. Formatting, lint, architecture and both type checkers pass; negative
typing contracts reject 127 basedpyright and 123 mypy examples. The full-size
10,000-record/1,000-Work soak was not run during that initial expansion. The later
capture-optimization follow-up passes the unchanged full workload in all four
modes, with 12 relationship cycles and 24,000 individual relationship reads per
mode. See the [final measurements and validation](metadata-followups-2026-11-22.md)
for timings, source scope and remaining performance costs. These are correctness
results, not a whole-module sign-off.

Validation artifacts are `/tmp/liuxin-ergonomics-*`; `validation.json` records
unique passing cases and final source fingerprints. The broader cache run began
before the final Agent guard and PostgreSQL schema corrections; focused credit
and live-backend tests validate those final changes. Existing PostgreSQL schemas
can now use the explicit, separately invoked
[native catalogue upgrade](../databases/postgres-catalog-upgrade.md).

Return to [caches](README.md) or the [ownership contract](../catalog/catalog-cache-boundary.md).
