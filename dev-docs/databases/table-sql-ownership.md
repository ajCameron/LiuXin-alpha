# Table-owned SQL helpers

Implemented **2026-10-30**, extended **2026-10-31**. `db.metadata_sql` has
**65 operations in 38 mixins**:
one for each of the **36 physical main tables**, the existing helper-table
`FeedsSQLMixin`, and shared `NormalizationSQLMixin`. Catalog, storage and backup
callers use the table readers while retaining their interpretation and policy.

## Ownership convention

Keep one mixin per physical main table for its specialised queries and writes. Name it after that table: `WorksSQLMixin`, `ExpressionsSQLMixin`, and
so on. A read spanning several tables belongs to the mixin for its returned
destination rows; a write belongs to the table it changes. Generic row/link
mechanics stay in portable macros. Shared normalization maintenance stays in
`NormalizationSQLMixin`, backed by the existing declarations and macro owner.

Catalog chooses relationships, validates semantic IDs and source existence,
applies matching/selection policy and renders results. SQL helpers consume
physical IDs and schema descriptors. Storage owns source-byte effects. No
helper chooses a singular book, preferred Expression or primary Manifestation.
See the [ownership decision](../catalog/catalog-database-ownership.md).

The initial WEMI slice has now been extended across the inventory. Helpers
provide bounded relationship reads, grouped foreign-key reads or existing table
writes; none are empty compatibility placeholders. Generic SQL stays shared in
portable macros. Further methods should address a concrete query or caller, with
query-count or equivalent evidence. A table mixin does not authorize new semantic
APIs, byte mutations, unused-record cleanup or a new normalization rule.

## Current WEMI readers

| Returned table / mixin | `db.metadata_sql` entry point | Existing Catalog caller |
| --- | --- | --- |
| `works` / `WorksSQLMixin` | `get_works_for_expression(expression_id)` | `expressions.list_works()` |
| `expressions` / `ExpressionsSQLMixin` | `get_expressions_for_work(work_id)` | `expressions.list_for_work()` |
| `expressions` / `ExpressionsSQLMixin` | `get_expressions_for_manifestation(manifestation_id)` | `manifestations.list_expressions()` |
| `manifestations` / `ManifestationsSQLMixin` | `get_manifestations_for_expression(expression_id)` | `manifestations.list_for_expression()` |

Each method returns a tuple of `LinkedTableRow(link, row)`. `link` is the existing
`LinkRow`, retaining endpoint IDs, priority, type and extra columns. `row` is a
fresh shallow dictionary, or `None` for a missing destination, including a NULL
endpoint. Repeated destinations remain separate results with independent row
dictionaries. Low-level readers do not require the source to exist; an absent
unlinked source returns an empty tuple. Catalog retains its source checks and
raises `CatalogNotFoundError` for missing sources or destinations.

The generic reader `db.macros.get_linked_rows(spec, primary_id)` reads links
once, fetches distinct destination IDs with `get_rows_by_ids(table, ids)`, and
reconstructs results in link order. Physical endpoint/link tables and discovered
ID columns are required. Booleans and projection rays are rejected as requested
IDs. `get_rows_by_ids` omits missing IDs, collapses repeated requests, preserves
first-request key order and binds at most 500 IDs per query. Inputs and results
are materialized; this is not a streaming API. `get_linked_rows_bulk(spec, ids)`
performs the same work for many sources, deduplicating destination fetches across
the entire request. `get_link_rows_bulk()` now also bounds every explicit-owner
query to 500 bindings, including any type filter.

Ordering and type filters follow the existing link reader: descending priority
where present, then destination ID. The omitted `LINK_TYPE_UNSET` filter means
all types; explicit `None` selects NULL only on typed relationships. Current
Work–Expression and Expression–Manifestation links are **untyped**, so any
explicit type filter still raises `InputIntegrityError`.

Reads reuse the current connection and see pending writes in the caller's
transaction. They neither start an independent snapshot nor commit. Without
an enclosing transaction, concurrent changes can still occur between link and
destination reads. The [efficiency follow-up](sql-efficiency-2026-10-31.md)
records query-index migrations separately from these read contracts.

## Additional read families and migrated callers

Linked-row readers use `get_<table>_linked_from(source_table, source_id)` for
Agents, Comments, Notes, Synopses, Genres, Subjects, Labels, Languages, Ratings,
Images, Folders, Devices, Stores, Digital Assets, Composite Digital Assets, Tags,
Series and Files. Each resolves a declared schema relationship; it cannot invent
an absent edge. They share the WEMI reader's link-order, type-filter, missing-row
and transaction contracts. Catalog's relationship seam dispatches to these
owners, including existing credits, annotations and Work membership readers.

Foreign-key readers use `get_rows_for_ids(table, column, owner_ids, where=...)`.
They materialize and validate every requested integer ID, collapse repeated
requests, retain empty groups, and order each group by physical row ID. They
bind at most 500 values per query, including extra filters. NULL filters use
`IS NULL`; other filters compare stored values exactly. The method returns a
mapping of requested IDs to row tuples, not an existence assertion for owners.

Catalog's Item, Item Identifier, Entity Identifier and Annotation readers now
filter by their owner in SQL. Entity Identifier reads always include the stored
entity-type discriminator so equal IDs at different WEMI levels remain separate.
Annotation filters and Catalog validation stay at their existing boundaries.
Agent subtype and organisation graph batches expose physical rows without
selecting an author, publisher, active parent or hierarchy traversal policy.
File derivation readers preserve legacy file provenance; they do not substitute
for Digital Asset derivations or interpret SquashFS replicas as derived assets.

The Core storage policy scan reads Assets once, counts Replica groups by raw
presence/integrity/mode labels and fetches distinct explicit policies in batches. Core still
interprets health labels, modes, counts and missing-policy defaults. This remains
the existing row-based assessment, separate from the storage manager's effective
policy and cached record envelopes. Backup workflow enumeration similarly reads
selected workflow rows once and batches their sources; the backup repository
still decodes references, ordinals, checkpoints and intent.

`db.macros.get_rows_page()` returns `(rows, complete)` in physical-ID order. It
uses exact equality filters and SQL `LIMIT`/`OFFSET`, materializing at most
`limit + 1` rows to determine completeness. A zero-length page is complete only
at or beyond the end. Core's `storage.resource.list` uses this bounded read;
its public input conversion and record shape are unchanged. Lightweight adapters
without this macro retain their existing in-memory fallback. Pages do not pin
a snapshot against concurrent changes between requests. Catalog's base `list()`
also uses SQL pages. `list_after(last_id, limit=...)` and
`iter_rows(batch_size=...)` use the macro's `after_id` cursor to avoid repeatedly
walking an offset during long scans. A cursor and nonzero offset cannot be mixed.

Browse enriches only the visible Work page and batches relationships through
the selected database read source. Category counts use
`count_works_linked_from()` and retain relationship multiplicity; its result also
exposes distinct Work counts and missing endpoints. Cache sources retain their
own read/fallback rules. Legacy Row traversal batches destination payloads while
preserving its existing link order and duplicate Row results.

Existing cache-backed hydration and legacy Row adapters retain their read-source
contracts. Having every table represented does not mean every database read has
been migrated, nor that all catalogue/storage policy gaps are resolved.

## Worked example: a novel and its introduction

Given an open `db`, create two Works and their Expressions, embodied together
in one Manifestation. Semantic creation/linking stays with Catalog:

```python
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.catalog.relationships import CatalogRelationships

catalog = Catalog(db)
relationships = CatalogRelationships(db)
with db.macros.transaction():
    novel = catalog.works.create({"title": "The Novel"})
    introduction = catalog.works.create({"title": "Introduction to The Novel"})
    novel_text = catalog.expressions.create({"label": "English novel text"})
    introduction_text = catalog.expressions.create({"label": "English introduction"})
    edition = catalog.manifestations.create({"edition_statement": "Annotated edition"})
    relationships.link("works", novel, "expressions", novel_text)
    relationships.link("works", introduction, "expressions", introduction_text)
    relationships.link("expressions", novel_text, "manifestations", edition)
    relationships.link("expressions", introduction_text, "manifestations", edition)

# Both embodied texts survive the batched read.
texts = catalog.manifestations.list_expressions(edition)
assert {row["expression_id"] for row in texts} == {novel_text, introduction_text}
assert catalog.expressions.list_works(novel_text)[0]["work_id"] == novel
assert catalog.expressions.list_works(introduction_text)[0]["work_id"] == introduction

# The opposite directions use the same table-owned readers.
assert catalog.expressions.list_for_work(novel)[0]["expression_id"] == novel_text
assert catalog.manifestations.list_for_expression(novel_text)[0]["manifestation_id"] == edition

# Direct SQL helpers retain physical link data without adding Catalog fields.
records = db.metadata_sql.get_expressions_for_manifestation(edition)
assert {record.row["expression_id"] for record in records} == {novel_text, introduction_text}
assert all("_catalog_link" not in record.row for record in records)
assert all("_catalog_link" in row for row in texts)
```

This does not collapse the introduction into the novel or choose one Expression
to represent the edition. See [WEMI link examples](wemi-link-examples.md) for the
underlying graph and [batch contracts](table-sql-batches.md) for writes/repair.

## Physical main-table inventory

Checked against the generated SQLite fixture and the current
`Database.categorize_table()` rules: **36 physical tables** are classified as
main. The name-based classification also admits **22 views**; those are excluded
here using their actual relation types. The field guide's 67 explicit tables
also include helpers and other categories, so its count has a different scope.

| Physical table | Table mixin | Supported query/write |
| --- | --- | --- |
| `agents` | `AgentsSQLMixin` | Batched destination rows for credits/relationships |
| `annotations` | `AnnotationsSQLMixin` | Item groups with optional exact user/kind filters |
| `asset_replicas` | `AssetReplicasSQLMixin` | Groups by Asset or Store; Core policy scan |
| `backup_policies` | `BackupPoliciesSQLMixin` | Distinct explicit policy ID batches; Core policy scan |
| `backup_workflow_outputs` | `BackupWorkflowOutputsSQLMixin` | Groups by workflow; backup result persistence reads |
| `backup_workflow_sources` | `BackupWorkflowSourcesSQLMixin` | Groups by workflow; declaration enumeration |
| `backup_workflow_state` | `BackupWorkflowStateSQLMixin` | Groups by workflow; checkpoint reads |
| `backup_workflows` | `BackupWorkflowsSQLMixin` | ID-ordered workflow rows with optional exact status |
| `comments` | `CommentsSQLMixin` | Batched WEMI attachment destinations |
| `composite_digital_assets` | `CompositeDigitalAssetsSQLMixin` | Batched declared relationship destinations |
| `devices` | `DevicesSQLMixin` | Batched declared relationship destinations |
| `digital_assets` | `DigitalAssetsSQLMixin` | Batched declared relationship destinations |
| `entity_identifiers` | `EntityIdentifiersSQLMixin` | Owner groups scoped by stored entity type; Catalog |
| `expressions` | `ExpressionsSQLMixin` | Work/Manifestation relationship destinations; Catalog |
| `file_derivations` | `FileDerivationsSQLMixin` | Legacy lineage groups by parent or child File |
| `files` | `FilesSQLMixin` | File metadata batches/deletion and relationship destinations |
| `folders` | `FoldersSQLMixin` | Batched declared relationship destinations |
| `genres` | `GenresSQLMixin` | Batched declared relationship destinations |
| `human_agents` | `HumanAgentsSQLMixin` | Subtype groups by Agent reference |
| `images` | `ImagesSQLMixin` | Batched declared relationship destinations |
| `item_identifiers` | `ItemIdentifiersSQLMixin` | Identifier groups by Item; Catalog |
| `items` | `ItemsSQLMixin` | Item groups by Manifestation; Catalog |
| `labels` | `LabelsSQLMixin` | Batched declared relationship destinations |
| `languages` | `LanguagesSQLMixin` | Batched declared relationship destinations; no mutation API |
| `manifestations` | `ManifestationsSQLMixin` | Expression relationship destinations; Catalog |
| `notes` | `NotesSQLMixin` | Batched WEMI attachment destinations |
| `org_agent_relations` | `OrgAgentRelationsSQLMixin` | Organisation edge groups by parent or child Agent |
| `org_agents` | `OrgAgentsSQLMixin` | Subtype groups by Agent reference; Catalog validation |
| `ratings` | `RatingsSQLMixin` | Batched declared relationship destinations |
| `replication_policies` | `ReplicationPoliciesSQLMixin` | Distinct explicit policy ID batches; Core policy scan |
| `series` | `SeriesSQLMixin` | Vocabulary batches and relationship destinations |
| `stores` | `StoresSQLMixin` | Batched declared relationship destinations |
| `subjects` | `SubjectsSQLMixin` | Batched declared relationship destinations |
| `synopses` | `SynopsesSQLMixin` | Batched WEMI attachment destinations |
| `tags` | `TagsSQLMixin` | Vocabulary batches and relationship destinations |
| `works` | `WorksSQLMixin` | Expression relationship destinations; Catalog |

`feeds` is classified as a helper table and retains `FeedsSQLMixin`.
Normalization is a shared capability, not a new main table or per-table rule
owner. Generated link tables are resolved from schema descriptors. Compatibility
views such as `books` and `titles` get no physical-table mixin.

## Validation

[WEMI reader regressions](../../tests/databases/test_metadata_sql_wemi_reads.py)
cover all four Catalog directions on SQLite/APSW, source and destination errors,
link metadata/order, caller-owned transactions and physical-table validation.
Reading a Work with 1,001 Expressions uses **three destination SELECTs**,
plus one source and one link SELECT, instead of one SELECT per destination.
Schema-introspection queries are outside this count. The
[portable macro tests](../../tests/databases/api/test_portable_macros.py) also
exercise typed/NULL filters, repeated destinations, independent result mappings
and dangling integer references on SQLite and a PostgreSQL-shaped SQLite host.
The shaped host is not evidence of live PostgreSQL execution.

First WEMI-slice validation on **2026-10-30**: the database/Catalog run passed **196 tests
with one expected skip**; a separate worked-example, ownership and retirement
run passed **41 tests**. The skip is the existing shaped-host `pg_temp` case.
API signature checks, focused lint/formatting, the dependency-direction guard
and `git diff --check` pass. The touched Catalog common-types module retains its
pre-existing lint diagnostics with none added by this slice. Full-suite and live
PostgreSQL testing were not performed. Scratch files and telemetry stayed under
`/tmp`.

The [all-table regressions](../../tests/databases/test_metadata_sql_tables.py)
check the complete physical inventory/API, actual columns and relationship
schemas, identifier scope, bounded owner reads, NULL filters, rollback visibility,
page materialization and the Core/backup callers. A 1,001-owner identifier lookup
with its scope filter uses three data SELECTs. The small policy-scan fixture uses
one SELECT each for Assets, Replicas, replication policies and backup policies;
workflow enumeration uses one workflow read plus bounded source reads.

Final all-table verification on **2026-10-31**: **482 distinct selected test
cases passed, with one expected skip**, combining the broad regression run and
focused reruns. The broad run initially passed 473 cases and exposed five
failures: two retirement assertions still required the old Folders module name
to be absent, and three hydration tests used an in-memory macro fixture without
the earlier `delete_link_pair` contract. Those assertions/fixtures are updated;
all five cases pass on rerun with their rollback and result assertions retained.
Four additional cases cover subtype/lineage direction and qualified owner/page
SQL. The skip remains the PostgreSQL-shaped SQLite host's missing `pg_temp`.

The selected coverage includes database/macros, Catalog relationships and
hydration, storage ownership and backup persistence. Focused lint, formatting,
API signatures, the dependency-direction guard and `git diff --check` pass.
There are no new backward dependency edges. Full-suite and live PostgreSQL
execution remain untested for this change. Scratch databases, telemetry and
the combined case results stayed under `/tmp`.
