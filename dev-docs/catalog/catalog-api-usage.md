
# Catalog API Usage Guide

Reviewed **2026-10-20**. Current repositories, relationship ownership and retrieval boundaries were checked. 
See [next tasks](next_tasks.md) for remaining work; older run counts retain their original dates.

This guide is the practical entry point for LiuXin's in-process Catalog API.
It complements the structural protocols in `LiuXin_alpha.catalog.api`.

## Constructing the facade

`Catalog` borrows an open LiuXin database. It composes repositories, matchers,
retrieval helpers, mutation services, and compatibility metadata tools over the
same database and identity policy.

```python
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.catalog.api import CatalogAPI

catalog: CatalogAPI = Catalog(db)
```

Closing or discarding `catalog` does not close `db`.

## Write-validity boundary

Every public Catalog mutation must establish semantic validity itself. Callers
may validate early for better feedback, but Catalog does not treat surfaces,
declarations, annotations, or prior workflow steps as trusted proof.

The layers cooperate:

- `metadata` supplies pure normalization and value validation;
- Catalog validates complete operations and relationships before persistence;
- database constraints retain structural, referential, uniqueness, and
  concurrency-safe enforcement.

The supported boundary consists of `catalog.mutations.writer.*`, public
repository CRUD and specialised mutations, and the advanced schema-driven
`catalog.create_writer`/`write`/`write_one` trio. Higher-level Library, Core,
and application operations remain inside the contract only by delegating to
those routes; their own checks are never the sole guard.

`catalog.add`, `ensure`, `apply`, and `intralink` remain supported Row-oriented
compatibility routes and are deprecated for new code. For Catalog-owned tables,
`catalog.row_mutations` adapts them to the same repository preparation and
relationship owner. Non-Catalog tables retain low-level adapters. Direct use of
`catalog.db`, drivers, raw macros, direct SQL, private helpers, or the private
writer-host `_apply_*` methods is outside the semantic guarantee. Cache writers
delegate to Catalog and own their subsequent reconciliation.

As of **2026-10-19**, shared preparation covers repository CRUD, schema writers,
container writes and the Catalog-owned Row routes. This is broader than the
initial curated-identifier slice, but it does not invent validators for fields
without declared rules. See the [write-validation contract](catalog-write-validation-contract.md)
for current limits and the [migration record](catalog-legacy-mutation-migration.md)
for the live compatibility boundary.

## Choosing an API area

| Need | Entry point |
| --- | --- |
| Read/create/update/delete one entity family | `catalog.works`, `catalog.items`, etc. |
| Decide identity without writing | `catalog.matching` |
| Search WEMI entities and attached metadata | `catalog.search.find()`; see [WEMI search](catalog-wemi-search.md) |
| Project/update Expression-centred Calibre fields | `catalog.calibre.project()` and `write_field()`; see [conversion policy](../calibre_compat/expression-conversion-policy.md) |
| Read a coherent WEMI slice | `catalog.retrieval.bundles` |
| Hydrate metadata from a database/cache or overlay OPF onto persisted metadata | `catalog.retrieval.metadata` |
| Read immediate WEMI adjacency or a bounded descendant graph | `catalog.retrieval.hierarchy`, `catalog.retrieval.graph` |
| Build a display-neutral summary/title | `catalog.retrieval.projections` |
| Create/link WEMI entities or attach/replace several metadata groups | `catalog.mutations.writer` |
| Check whether a semantic mutation is eligible | `catalog.mutations.policy` |
| Perform a schema-driven field/link write | `catalog.create_writer`, `write`, `write_one` |
| Work with legacy database `Row` objects | `catalog.add`, `ensure`, `apply`, `intralink` |

The convenience properties and grouped repositories are identical objects:

```python
assert catalog.works is catalog.repositories.works
assert catalog.identifiers is catalog.repositories.identifiers
```

Code which receives a repository name dynamically should use the validated
registry rather than `getattr`:

```python
repository = catalog.repositories.for_name("work")
assert repository is catalog.works
```

Both singular and plural public names are accepted. Unknown names raise
`KeyError`.

## WEMI levels

The four bibliographic levels answer different questions:

- **Work** — the abstract intellectual creation, such as *Frankenstein*.
- **Expression** — a language, revision, translation, narration, or other
  realization of a Work.
- **Manifestation** — an edition/publication embodiment with carrier, format,
  publisher-facing edition data, pagination, or region.
- **Item** — one owned, observed, or managed copy, including inventory,
  acquisition, source, path, and condition.

Creating a connected path with repository identity safeguards looks like this:

```python
from LiuXin_alpha.catalog.api import MetadataCandidate

work_id = catalog.works.match_or_create(
    MetadataCandidate({
        "title": "Frankenstein",
        "canonical_title": "Frankenstein; or, The Modern Prometheus",
        "original_year": 1818,
    }, source="manual")
)

expression_id = catalog.expressions.match_or_create(
    work_id,
    MetadataCandidate({
        "label": "1818 English text",
        "language_id": english_language_id,
    }),
)

manifestation_id = catalog.manifestations.match_or_create(
    expression_id,
    MetadataCandidate({
        "edition_statement": "Penguin Classics",
        "pub_year": 2003,
        "carrier_type": "ebook",
    }),
)

item_id = catalog.items.match_or_create(
    manifestation_id,
    MetadataCandidate({
        "inventory_code": "EBOOK-0042",
        "source": "manual import",
        "source_path": "Frankenstein.epub",
    }),
)
```

The contextual methods create and link only when the decision is a genuine
non-match. They return an existing ID on a safe match and raise on ambiguity or
conflict.

When the caller already knows that all rows are new, one operation creates a
complete path and returns every ID:

```python
created = catalog.mutations.writer.create_wemi_stack(
    work={"title": "Frankenstein"},
    expression={"label": "1818 English text"},
    manifestation={"edition_statement": "First edition"},
    items=[{"inventory_code": "COPY-0001"}],
    origin="manual-import",
)
```

Existing adjacent WEMI rows can be related without reaching into link-table
details:

```python
receipt = catalog.mutations.writer.link_wemi(
    parent_level="work",
    parent_id=work_id,
    child_level="expression",
    child_id=expression_id,
    primary=True,
    origin="curation",
)
removed = catalog.mutations.writer.unlink_wemi(
    parent_level="work",
    parent_id=work_id,
    child_level="expression",
    child_id=expression_id,
)
```

Only Work-to-Expression, Expression-to-Manifestation, and
Manifestation-to-Item edges are valid. Setting a primary link demotes any
previous primary at that parent; Item ownership uses its foreign key and does
not accept priority or origin metadata.

## Repository inputs and outputs

Repository writes accept concise public aliases and writable storage column
names:

```python
work_id = catalog.works.create({"title": "Frankenstein"})
catalog.works.update(work_id, {"original_year": 1818})
```

Returned mappings use storage column names:

```python
work = catalog.works.require(work_id)
print(work["work_title"])
print(work["work_original_year"])
```

Use `get` when absence is normal and `require` when it is an error:

```python
maybe_work = catalog.works.get(work_id)          # RowMapping | None
existing_work = catalog.works.require(work_id)  # raises CatalogNotFoundError
```

Relationship traversals return mappings with link-specific data under
`"_catalog_link"`:

```python
for expression in catalog.expressions.list_for_work(work_id):
    priority = expression["_catalog_link"]["priority"]
```

## Matching and safe creation

Matching returns `MatchResult`; it is not a nullable-ID lookup:

```python
decision = catalog.matching.works.best(
    MetadataCandidate({"title": "Frankenstein"})
)

if decision.is_match:
    work_id = decision.entity_id
elif decision.requires_resolution:
    # "ambiguous" and "conflict" must be resolved by policy or a person.
    show_choices(decision.alternatives, decision.evidence)
else:
    # Only "no_match" makes independent creation safe.
    work_id = catalog.works.create({"title": "Frankenstein"})
```

| Decision | Meaning | Automated creation |
| --- | --- | --- |
| `match` | One entity was safely selected | Reuse it |
| `no_match` | No qualified existing entity | Allowed |
| `ambiguous` | Several entities remain plausible | Stop and resolve |
| `conflict` | Decisive evidence contradicts itself | Stop and resolve |

`repository.match_or_create(...)` implements this table. It raises
`CatalogAmbiguousMatchError` or `CatalogMatchConflictError` for unresolved
decisions; each error retains the decision as `error.result`.

Reusable value entities such as Tags and Genres match exactly by default:

```python
tag_id = catalog.tags.match_or_create(
    MetadataCandidate({"value": "Gothic"})
)
exact = catalog.matching.tags.exact("gothic")
```

Approximate reuse is deliberately opt-in with `use_policy=True`.

## Work tag and Series memberships

Added **2026-10-30**. `catalog.tags` and `catalog.series` share the
`WorkMembershipRepositoryAPI` contract. `entity_id` means a Tag ID when called
on `catalog.tags`, and a Series ID when called on `catalog.series`; `work_id`
always identifies a Work. These operations do not resolve Calibre rays or
Expression IDs. The current schema provides no direct Tag links for the other
WEMI levels, Agents or Series.

```python
novel_id = catalog.works.create({"title": "The Novel"})
introduction_id = catalog.works.create({"title": "Introduction to The Novel"})
tag_id = catalog.tags.match_or_create(MetadataCandidate({"name": "Gothic"}))
series_id = catalog.series.match_or_create(
    MetadataCandidate({"name": "Annotated Classics"})
)

# Both Works can share a Tag without a priority collision.
catalog.tags.replace_for_works(
    {novel_id: [tag_id], introduction_id: [tag_id]}
)
assert catalog.tags.is_linked_to_work(entity_id=tag_id, work_id=novel_id)
tags = catalog.tags.list_for_work(novel_id)
assert tags[0]["tag_id"] == tag_id

catalog.series.link_to_work(entity_id=series_id, work_id=novel_id)
catalog.series.replace_for_work(work_id=novel_id, entity_ids=[series_id])
assert catalog.series.list_for_work(novel_id)[0]["series_id"] == series_id

# Removing one Work's membership preserves the Tag and the other Work's link.
catalog.tags.unlink_from_work(entity_id=tag_id, work_id=novel_id)
assert not catalog.tags.is_linked_to_work(entity_id=tag_id, work_id=novel_id)
assert catalog.tags.is_linked_to_work(entity_id=tag_id, work_id=introduction_id)

# Empty replacement clears memberships; it does not delete the Series row.
catalog.series.replace_for_work(work_id=novel_id, entity_ids=[])
assert catalog.series.require(series_id)
```

All endpoints must exist. A missing Work or Tag/Series raises
`CatalogNotFoundError`; an existing unlinked Work returns an empty sequence
or a false membership check. IDs must be nonnegative integers, excluding bool;
replacement accepts iterables, materializes them and rejects duplicate IDs.
Every Work and target in a batch is validated before the first mutation, and
late failures roll back the complete batch. Writes compose with an enclosing
macro transaction. Link/entity listing also uses one macro transaction.

Membership means an endpoint pair across all link types. Adding an existing
pair is a no-op. Replacement preserves all retained links, including their
IDs, type, priority and provenance; removal clears every type for that pair.
New links have the default null type. These APIs do not edit individual Series
link roles. They do not delete shared rows or change Series parent hierarchies.

Input order does not reorder retained links. Physical priorities order Works
within each Tag/Series, and new links are allocated in that direction. Listing
returns the macro reader's priority-descending/ID order with `_catalog_link`
metadata; multiple typed Series links can repeat a Series row. This order does
not select a preferred Series or supply a volume number. Those are separate
[follow-up tasks](next_tasks.md#cat06-replace-the-retired-metadata-sql-capabilities).

## Agents and credits

Agents represent people and organisations. A credit is a relationship with a
role and optional priority:

```python
agent_id = catalog.agents.match_or_create(
    MetadataCandidate({
        "name": "Mary Shelley",
        "type": "person",
        "aliases": ["Mary Wollstonecraft Shelley"],
    })
)

catalog.agents.link_to_wemi(
    agent_id=agent_id,
    level="work",
    entity_id=work_id,
    role="author",
    priority=1,
)

credits = catalog.agents.list_for_wemi(level="work", entity_id=work_id)
role = credits[0]["_catalog_link"]["type"]
```

## Curated and observed identifiers

`catalog.identifiers` stores curated logical identifiers owned by a WEMI
entity or Agent. Schemes and values are normalized before comparison:

```python
from LiuXin_alpha.catalog.api import IdentifierCandidate

identifier_id = catalog.identifiers.match_or_create(
    IdentifierCandidate(
        identifier_type="ISBN-13",
        value="978-0-14-143947-1",
        source="publisher metadata",
    )
)
assigned_id = catalog.identifiers.link_to_wemi(
    identifier_id=identifier_id,
    level="manifestation",
    entity_id=manifestation_id,
    priority=0,
)
```

An Identifier row has one owner. Assigning an already-owned logical value to a
different entity copies it so the first owner's row remains intact.

For edit forms and import snapshots, replace and project the complete curated
mapping directly:

```python
catalog.identifiers.replace_for_wemi(
    level="manifestation",
    entity_id=manifestation_id,
    identifiers={
        "isbn13": "978-0-14-143947-1",
        "doi": "10.1000/example",
    },
)
primary = catalog.identifiers.primary_values_for_wemi(
    level="manifestation",
    entity_id=manifestation_id,
)
```

The projection is keyed by normalized scheme and retains the caller-facing
stored value. Historical/non-primary rows remain available through
`list_for_wemi`.

To select one primary value while retaining alternatives, use the scoped
operation. Equivalent values reuse the owned row; invalid ISBN checksums fail
before writes. `None` clears the selected scheme, and generic `isbn` clearing
removes both lengths:

```python
catalog.identifiers.set_primary_for_wemi(
    level="manifestation", entity_id=manifestation_id,
    scheme="isbn", value="978-0-14-143947-1",
)
catalog.identifiers.clear_for_wemi(
    level="manifestation", entity_id=manifestation_id, scheme="isbn",
)
catalog.works.set_original_language(work_id, language_id)
```

Work original language is separate from Expression language. The Row-compatible
`catalog.apply.primary_language(language_row, work_or_title_row)` delegates to
that Work operation. These operations compose inside `db.macros.transaction()`.
The former `db.metadata_sql` identifier and primary-language methods are retired;
see the [ownership decision](catalog-database-ownership.md).

`catalog.item_identifiers` instead stores raw observations on one Item:

```python
observation_id = catalog.item_identifiers.match_or_create(
    item_id,
    IdentifierCandidate("source-id", "vendor-record-42"),
)
```

The same observation on another Item is a distinct record.

## Logical titles and notes

Titles are not independent writable rows in the current schema. The Title
repository projects title-bearing columns from their WEMI owner:

```python
catalog.titles.add_for_wemi(
    level="expression",
    entity_id=expression_id,
    data={"title": "Frankenstein (revised text)"},
)
preferred = catalog.titles.preferred_for_wemi(
    level="expression",
    entity_id=expression_id,
)
```

Works, Expressions, and Manifestations have different title-bearing columns.
Items do not own title columns.

Notes are reusable entity rows with explicit WEMI links:

```python
note_id = catalog.notes.add_for_wemi(
    level="work",
    entity_id=work_id,
    data={"text": "First published anonymously."},
)
```

Titles, Notes, Synopses, and role-scoped Agent credits also provide explicit
replacement methods. Empty Note, Synopsis, or Agent ID sequences clear the
selected relationship set; `None` clears a logical title or owned Comment.

Annotations are always Item-scoped. List them without open-coded table
predicates:

```python
highlights = catalog.annotations.list_for_item(
    item_id,
    user_id=user_id,
    kind="highlight",
)
```

## Bundles and projections

An Item has one Manifestation foreign key, but that Manifestation can link to
several Expressions and those Expressions to several Works. Its bundle selects
one preferred path and can contain all four WEMI rows:

```python
bundle = catalog.retrieval.bundles.for_item(item_id)
assert bundle.work["work_id"] == work_id
assert bundle.expression["expression_id"] == expression_id
assert bundle.manifestation["manifestation_id"] == manifestation_id
assert bundle.item["item_id"] == item_id
```

Each root chooses one deterministic path using the
[Catalog selection policy](catalog-wemi-selection.md). Complete WEM paths precede
partial paths; primary flags, ascending priorities and IDs rank candidates, matching
SQL `wemi_primary_rays_v` for a Work with complete rays. The lowest Item ID wins on
the chosen edition; Item availability does not rank editions. Repository lists
retain their descending-priority order. A bundle is not an exhaustive ancestor or
descendant graph, and incomplete paths can leave levels absent.
Bundles also collect attached Agents, Identifiers, Titles, Notes, and
relationship records from the selected path. The
[worked WEMI examples](../databases/wemi-link-examples.md) show matching SQL/bundle
outputs and explain why one copy can appear on several paths. Metadata containers'
primary-relation accessors answer a separate, relation-local question; see the
selection contract before using them as a preferred-path projection.

Use hierarchy retrieval for one generic step in either direction:

```python
children = catalog.retrieval.hierarchy.children(
    level="work",
    entity_id=work_id,
)
parents = catalog.retrieval.hierarchy.parents(
    level="item",
    entity_id=item_id,
)
```

Use graph retrieval when every descendant is needed:

```python
graph = catalog.retrieval.graph.for_work(
    work_id,
    max_expressions=100,
    max_manifestations=500,
    max_items=1000,
)
if graph.truncated_levels:
    request_another_view(graph.truncated_levels)
```

The result retains all selected entities and structural edges. Explicit limits
make it safe for direct and RPC use, and `truncated_levels` prevents callers
from mistaking a bounded result for a complete Work graph.

Projections make catalog-semantic choices without rendering a UI:

```python
display_title = catalog.retrieval.projections.display_title(
    level="work",
    entity_id=work_id,
)
summary = catalog.retrieval.projections.item_summary(item_id)
```

HTML, terminal formatting, localization, and protocol response objects remain
surface responsibilities.

## Coordinated mutations

`attach_metadata` accepts direct fields plus reserved semantic groups:

```python
catalog.mutations.writer.attach_metadata(
    level="work",
    entity_id=work_id,
    data={
        "fields": {"original_year": 1818},
        "title": "Frankenstein",
        "agents": [
            {"name": "Mary Shelley", "role": "author", "priority": 1}
        ],
        "identifiers": [
            {"scheme": "wikidata", "value": "Q150827", "priority": 0}
        ],
        "notes": ["First published anonymously."],
    },
)
```

The operation checks attachment shape and selected IDs before writing, but deeper normalization can still fail during persistence. It uses the portable macro transaction when the supplied database exposes one; legacy handles without that capability can leave partial effects. Agent entries require a `role`; identifier entries require either `identifier_id` or scheme/type plus value.

Use `replace_metadata` when the supplied groups are authoritative snapshots:

```python
catalog.mutations.writer.replace_metadata(
    level="work",
    entity_id=work_id,
    data={
        "fields": {"original_year": 1818},
        "title": "Frankenstein",
        "agents": [
            {"agent_id": mary_id, "role": "author"},
        ],
        "identifiers": {"wikidata": "Q150827"},
        "notes": ["First published anonymously."],
        "comments": {"text": "Curator reviewed"},
        "synopses": [{"text": "A modern Prometheus story."}],
    },
)
```

Omitted groups remain unchanged. Present empty groups clear their complete
relationship set. Title replacement clears all title-bearing columns for that
WEMI level before applying the replacement; explicitly supplied direct
`fields` are applied last and therefore win if they overlap.

Merging moves supported metadata/relationships to the target and deletes the
source:

```python
if catalog.mutations.policy.can_merge(
    level="work",
    source_id=duplicate_id,
    target_id=canonical_id,
):
    catalog.mutations.writer.merge_entities(
        level="work",
        source_id=duplicate_id,
        target_id=canonical_id,
    )
```

Policy checks are advisory and side-effect-free; writers validate again inside
their transaction. A caller must not treat a successful policy probe as a
validation token that allows the writer to skip its own checks.

## Core direct and RPC operations

Core exposes the same conveniences as named transport-stable operations. The
same payload works through the local `CoreRuntime` and `RemoteCoreClient`.
`core_client(runtime=...)` returns that runtime; there is no separate
`LocalCoreClient` wrapper. Transport-stable describes encoding, not authorization:

```python
runtime.command(
    "catalog.metadata.replace",
    {
        "level": "work",
        "entity_id": work_id,
        "data": {"identifiers": {"wikidata": "Q150827"}},
    },
)
graph = remote.query(
    "catalog.graph.get",
    {"work_id": work_id, "max_items": 1000},
)
```

The convenience operation names are:

- `catalog.wemi.link` and `catalog.wemi.unlink`
- `catalog.metadata.replace`
- `catalog.hierarchy.list` and `catalog.graph.get`
- `catalog.identifiers.primary-values`
- `catalog.annotations.list`

## Generic schema-backed writes

`create_writer`, `write`, and `write_one` are useful when code deliberately
targets the schema-defined storage shape. They are supported public Catalog
entry points, not permission to call the prepared-update seam directly:

```python
catalog.write_one(
    "works",
    "work_canonical_title",
    work_id,
    "Frankenstein; or, The Modern Prometheus",
)
```

Prefer repository or mutation methods when a semantic operation exists. They
make identity, ownership, relationship, and transaction rules clearer.

The writer selected by these methods performs its declared adaptation and
validation before using Catalog's private `_apply_*` host methods. Those
underscored methods and prepared update objects are implementation details and
are intentionally absent from `CatalogAPI`.

## Field metadata containers

Field metadata describes fields themselves—storage mapping, datatype,
multiplicity, display hints, categories, and search aliases. It does not hold
the metadata values for one Work or Item.

```python
from LiuXin_alpha.catalog.field_metadata import FieldMetadata

field_metadata = FieldMetadata()
title_description = field_metadata["title"]
sortable = field_metadata.sortable_field_keys()
search_target = field_metadata.search_term_to_field_key("authors")
```

Custom keys normally use `#label`; dynamic user/search categories use their
configured category keys. Convert user-facing labels explicitly:

```python
internal_key = field_metadata.label_to_key("mood", prefer_custom=True)
label = field_metadata.key_to_label(internal_key)
custom = field_metadata.custom_field_metadata(include_composites=False)
```

`CalibreFieldMetadata` adds result-record index assignment for
Calibre-compatible query rows. `fm_from_dict` reconstructs serialized
custom/dynamic state over the built-in standard field definitions.

## Row-oriented compatibility helpers

The older metadata helpers return or consume database `RowAPI` objects:

```python
work_row = catalog.add.work(work_title="Frankenstein")
author_row = catalog.ensure.creator_blind("Mary Shelley")
catalog.apply.creator(
    resource_row=work_row,
    creator_row=author_row,
    creator_role="author",
)
```

Use this surface when integrating an existing Row-oriented workflow. It remains
supported while it is migrated, but is deprecated for new code. New
ID/mapping-oriented code should start with repositories.
