# Legacy maintenance retirement and capability inventory

Date: **2026-11-02**. Implemented on the maintenance branch following the review
of `databases.maintenance.legacy`. This is a record of removed behavior and
requirements for possible replacements, not a promise to restore every feature.
The deleted implementations remain recoverable through Git history.

## What changed

Removed `maintenance/legacy.py`, its package re-exports, the creator-specific
transient plugins, and the SQLite/SQLite_apsw `TA_UPDATE` callback and driver API
method. The unused `run_ta_update_after_each_change` preference was removed from
defaults and both shipped INI files. Existing user configuration may still
contain that key; it no longer enables work.

Duplicate inspection now belongs to
[`catalog.duplicates`](../../src/LiuXin_alpha/catalog/duplicates.py), reached by
the existing Core query and CLI command. Ambiguous legacy Genre lookup refuses
automatic repair. The unsupported cleanup receipt lives directly in Maintainer.

The projection plugins also delegate their physical queries to
`databases.maintenance.projection_sql`; Catalog retains subscription, payload and
freshness policy. Queries continue to use the runtime's bound transaction connection.

No catalogue schema migration is performed. The generated WEMI views, physical
merge service, normalized-key writers and the new prompt/lazy runtime remain
separate supported features. Pre-WEMI databases with stored SQL referencing
`TA_UPDATE` need an explicit migration before use; the removed callback is not
silently emulated. Fresh WEMI schemas contain no such trigger dependency.

## 1. Title aggregates and inherited metadata

**Current replacement implemented and locally verified, 2026-11-03:**
`catalog.retrieval.inherited.tags` computes attributed tags down every WEMI path
in a bounded read-only snapshot. A novel and introduction retain separate Work
contributions; empty results clear immediately because no copied aggregate is
stored. Agent/series/genre propagation and localized ancestry strings remain
separate policies. See [the current scope](../catalog/maintenance-repair-policies.md).

Removed entrypoints: `run_ta_updates`, `ta_trigger`,
`populate_ta_creators_tags`, `populate_ta_title_tags`,
`populate_ta_series_tags`, `populate_ta_series_aggregate`,
`populate_ta_genre_aggregate`, `populate_ta_identifiers_aggregate`, and
`populate_ta_publishers_aggregate`. Driver entrypoint: `direct_run_ta_update`.

The intended capabilities were:

- Union tags from a title's creators, direct title links, and series ancestors.
- Build ordered series, genre and publisher ancestry strings. Paths used `:`
  and multiple paths used ` & `.
- Group identifiers by stripped, uppercased scheme, retaining sets of values.
- Rebuild these values asynchronously after source changes.

The implementation depended on `creators`, title-era link tables, aggregate
columns and dictionary APIs absent from the current driver. Its daemon thread
had no completion handle, durable retry, transaction boundary or shutdown drain.
Several passes wrote only nonempty results, leaving stale values after the last
source link disappeared. The SQLite callback also compared a boolean preference
to string values and passed a driver lacking the required host methods.

**If needed again:** Catalog should define each projection's WEMI level and
provenance. Decide which credits, ancestors and editions contribute; a
Manifestation embodying several Works must not silently collapse their metadata.
Keep structured values until presentation rather than persisting separator-based
strings. Specify ordering, duplicate handling, identifier scheme rules, cycles
and clearing on empty results. Use declared dependencies and the existing durable
lazy runtime for caches; add explicit backfill, versioning and freshness receipts.
Use prompt only where a source write must fail if the projection cannot finish.
The current Work revision/expression-count plugins are examples, not replacements
for all these aggregates.

## 2. Creator sorting and rename maintenance

**Capability restored under current owners, 2026-11-02:**
`catalog.agents.rename` and `catalog.agents.repair_sort_names` now provide atomic
Agent name/sort maintenance. Core exposes `catalog.agent.rename` and
`catalog.agent.sort.repair`; the CLI provides `catalog agents rename` and
`catalog agents repair-sort`. The retired creator plugins and schema stay removed.

This was selected because current Agent fields, the metadata name formatter and
the Catalog write transaction already establish its ownership and storage model.
Duplicate inspection is already restored. The current repair work adds explicit
semantic merge and selected cleanup policies, plus WEMI tag attribution. It does
not restore automatic consolidation or implicit propagation across Agent/series
relationships.

The new default preserves nonblank sort values, including literal `none`, because
historical rows cannot distinguish manual values from derived ones. Missing/blank
sorts are derived. Explicit `sort_mode="derive"` regenerates sorts, or rename can
accept a nonblank override. Persons use the existing formatter with an explicit
comma/nocomma/copy method; organisations, groups and pseudonyms keep display order.
No locale or family-name order is inferred for these latter types. Copy or an
explicit sort supports person names for which surname inversion is inappropriate.

Renaming changes only canonical name and sort, leaving identity, subtype details,
aliases and WEMI credits intact. Batch repair changes only sorts for 1–500 selected
IDs, preserves canonical spelling, and validates the entire selection before any
write. Apply reads current rows under writer/row locks; previews are advisory.
Write and prompt-handler failures roll back the complete unit. This uses ordinary
Catalog writes and requires no journal installation or background worker. Core
and CLI preview by default; repository methods mutate unless `dry_run=True`.
See [operator examples](../../docs/maintenance.md#agent-names-and-sort-repair).

Validation: **88 tests passed**, with two SQLite skips for PostgreSQL-only row
locking, across the Agent/Catalog/Core/SQL/metadata/CLI selection on SQLite,
SQLite_apsw and live PostgreSQL 16. This covers atomic failure, manual overrides,
prompt rollback, concurrent edits and previews. Another **23 documentation checks**
passed. A separate installed-CLI smoke verified persisted rename/repair, unchanged
preview/reserved rows and no implicit jobs ledger. Formatting, lint, import/layer
checks and cold-import budgets pass; this was not a full repository test run.

Removed: `ensure_creators_sort`, `create_creator_insert_update_trigger`,
`direct_ensure_creators_sort`, `direct_set_original_one_row_creator_sort`,
`CreatorSortMaintenancePlugin`, and `CreatorRenameMaintenancePlugin`.

These attempted to fill or reset `creator_sort`, install name-change SQL
triggers, and process queued creator renames. The variants disagreed about NULL,
empty strings, text `none`, and preservation of explicitly supplied sort values.
Rename could persist before sort repair failed, and the plugin swallowed errors.
Neither plugin was registered by default before removal.

**Replacement policy:** Catalog owns Agent edits; metadata owns pure name formatting.
The existing `metadata.author_names.author_to_author_sort` formatter remains.
The implementation above distinguishes person and other Agent types, exposes
explicit sort overrides and keeps rename/sort updates in one transaction.
Locale-specific collation and persisted derivation provenance remain future work.
Derived optional
search/sort caches can use lazy maintenance with visible
freshness. Do not recreate a second bibliographic writer behind a generic queue.

## 3. Duplicate discovery and destructive consolidation

**Current replacement implemented and locally verified, 2026-11-03:**
`catalog.mutations.repairs.merge` now owns an explicit preview/apply policy; the
existing `MetadataWriter.merge_entities` delegates to it. Full reference scans,
scalar/link conflict decisions, typed/directed identity, stale-state checks and
transactional audit receipts replace partial transfer and silent target priority.
Scoped-tree changes and unsupported owned references block apply. Candidate
discovery never invokes this operation automatically. See
[the implemented scope](../catalog/maintenance-repair-policies.md).

Removed database entrypoints: `find_duplicates`, `fix_duplicates`,
`repoint_intralink_row`, `_do_intralink_merge`, `_smart_merge_rows`,
`_do_one_table_link_update`, and `_smart_merge_link_rows`.

The old finder grouped the entire table in Python, included singletons, and its
default comparison imported a missing symbol. Core could supply only strings,
while every non-default comparison was called as a function.

The repair chose the smallest ID, transferred interlinks/intralinks, repointed
tree children and deleted other records. It had no enclosing transaction and
conflated relationships sharing a neighbour regardless of type or direction.
Its timestamp rule could prefer older payloads, it did not consolidate scalar
record metadata, and an early `continue` could leave duplicates after moving links.
`_smart_merge_rows` had no caller. Review reproductions demonstrated:

- Same-named Genres under different parents collapsed into one, violating scope.
- `inspired_by` and `fanfiction_of` Work relationships collapsed into one type.
- A deletion failure left earlier link transfers committed.
- A successful return left two Works while transferring an Expression link.

**Discovery retained:** `catalog.duplicates.find_duplicates` is read-only.
The Core query remains `maintenance.duplicates.find`; the command remains
`liuxin maintenance duplicates TABLE COLUMN`. The result is now a list of
`{value, scope, row_ids}` groups, with only groups containing two or more IDs.
It reports comparison policy and scanned row count. This replaces the old
value-to-ID-set mapping so identical values in distinct scopes stay distinct.

Default `identity` comparison reads the database's current declarations,
including persisted overrides and removed declarations. It recomputes comparison
keys from the selected values without repairing stored keys. When no declaration
exists, display text uses NFKC, collapsed whitespace and casefold. Explicit
`nocase` uses NFC/casefold without trimming; `exact` retains stored text. All modes
retain declared scopes, even for derived-key columns. NULL and whitespace-only
values are ignored. Matching Work titles are review candidates, not entity identity.

The reader uses the existing portable, physical-table page API. It accepts
equality `where` filters and loads at most `max_rows + 1` rows in one query.
The default budget is 10,000, with a ceiling of 100,000; exceeding it returns
`scan_limit_exceeded`, never an incomplete duplicate report. The bound is in rows,
not bytes. Views and non-text values are refused. A scalable external grouping
or SQL aggregation plan for larger selections is future work; cross-page groups
must not be lost if pagination is introduced.

**Destructive repair is not retained.** A replacement needs an explicit Catalog
merge policy, preview and conflict receipt. Include declared identity scopes,
NULL parents, scalar-field provenance, typed/directed/symmetric relationships,
link priorities, tree cycles, owned resources and downstream invalidation. Use
one transaction for all transfers and deletions, and test injected failures at
each boundary. Never infer that equal title text proves two Works are identical.
Automatic whole-table consolidation is not an acceptable fallback for lookup.

The existing `Maintainer.merge` remains a distinct, atomic **physical interlink**
operation. It is not a substitute for a complete semantic merge: scalar metadata,
intralinks and tree relationships are not combined by that service.

## 4. Unused-row cleanup and unfinished entrypoints

**Current replacement implemented and locally verified, 2026-11-03:** Catalog now previews
selected discard/preserve decisions with reasons, complete inbound references,
outgoing-ID checks and conservative placeholder protection. Apply requires a
matching state fingerprint and writes a durable receipt atomically. It does not
infer a global garbage set, recursively remove owned resources or change files.
The implementation and exclusions are in the
[repair policy](../catalog/maintenance-repair-policies.md).

Removed functions: `clean`, `direct_merge`, and `do_view_startup_tasks`.
`direct_merge` always raised `NotImplementedError`; `do_view_startup_tasks` was a
no-op. `clean` never implemented deletion: selective mode was unsupported and
unrestricted mode either did nothing for books/titles or raised after inspection.
Their removal does not remove a working cleanup or direct-merge implementation.

The supported `maintenance clean` command still returns
`capability_unavailable` without changes, now directly through Maintainer.

**If needed again:** define an owner-specific orphan policy. Unlinked Works,
Agents and vocabulary may be intentional records; lack of a generic interlink is
not sufficient evidence for deletion. Account for direct FKs, intralinks, tree
children, provenance, user preservation flags and ownership. Require a candidate
preview, a bounded execution plan, transaction/recheck semantics and audit receipts.
Use existing physical bulk-delete primitives only after those decisions.

## 5. Calibre-shaped view and trigger bootstrap

Removed: `do_pre_view_startup_tasks`, `direct_create_tag_browser_news`, and
`direct_create_meta_2_view` (plus creator trigger helpers listed above).

These attempted to build a News-tag browser and a Calibre-shaped `meta2` view
with title/author/tag/series/publisher/rating/identifier/language/file columns and
custom fields. No external production caller remained. The templates depended
on old creator/link/file columns and UDFs, included duplicate `formats` aliases,
and assumed title/book IDs were interchangeable. Error handling referenced an
unimported `pprint` name.

**If needed again:** use Catalog read models and the explicit WEMI-to-Calibre
projection policy. Define aggregation and counts across multi-Work Manifestations,
multi-Expression Works, files and custom fields. Keep localization in presentation,
bind values, and make SQL/schema ownership and upgrades explicit. The generated
`books` and `titles` compatibility views remain; these read-only projections do
not restore the obsolete schema or justify writing through it.

## Suggested restoration order

1. Add scalable duplicate-candidate inspection only when bounded scans become a
   real operational limitation; preserve complete groups and explicit scopes.
2. Verify the new explicit Catalog merge preview/apply policy against the
   reproduced failures above. Automatic consolidation remains out of scope.
3. Add specific projections when a consumer needs them, using
   [maintenance modes](maintenance-modes.md) and existing mutation owners.
   Agent rename/sort maintenance is now restored as described above.
4. Extend selected cleanup only through additional reviewed ownership policies.
   Additional compatibility views remain separate; neither is an implicit startup task.

See [Catalog/database ownership](../catalog/catalog-database-ownership.md),
[physical SQL batches](table-sql-batches.md), and the
[operator maintenance guide](../../docs/maintenance.md).
