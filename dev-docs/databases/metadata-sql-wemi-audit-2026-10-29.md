# Metadata SQL: Calibre assumptions and WEMI migration

Reviewed **2026-10-29** against the current packaged schema, generated SQLite
fixture, Catalog repositories and direct callers. This is a follow-up to the
[Catalog/database ownership decision](../catalog/catalog-database-ownership.md).

## Physical-table SQL boundary — 2026-10-30

**Current state: `databases.metadata_sql` is retained for SQL over real tables.**
It exposes **62 operations in 38 mixins**: one for each of the 36 physical main
tables, `FeedsSQLMixin`, and shared `NormalizationSQLMixin`. The completed
[table inventory](table-sql-ownership.md) records relationship/owner batches and
the migrated Catalog, Core storage and backup readers.
`MetadataSQLAPI` and the
`db.metadata_sql` lifecycle binding are present. Compatibility facades such as
`CMTitlesMacrosMixin`, singular book/title interpretations and projection rays
belong to Catalog, not this SQL layer.

The distinction is responsibility, not whether the code is old or uses a mixin.
Concrete queries, joins, bulk operations and measured SQL accelerators may live
in databases when they address real tables/columns and explicit physical IDs.
They must bind inputs, preserve backend support and participate in the shared
transaction. They must not choose a WEMI owner or write projection views.
The helpers delegate to portable macros. The same-day batch extension reduces
stored-key lookup queries, shares atomic row mutation machinery and adds scoped,
data-only normalization repair using existing declarations. See the
[contracts and worked examples](table-sql-batches.md); no second normalization
policy is introduced.

| Capability | Owner and supported entry point |
| --- | --- |
| Physical WEMI relationship reads | `get_works_for_expression()`, `get_expressions_for_work()`, `get_expressions_for_manifestation()` and `get_manifestations_for_expression()`. Destination rows are fetched in batches while links retain their order and metadata; Catalog validates and renders the results. See the [table ownership inventory](table-sql-ownership.md). |
| Titles, author sort and timestamps | Explicit WEMI repositories; Calibre-facing writes use `catalog.calibre.write_field()` with an Expression reference or validated ray. `Catalog.works.update()` remains the explicit Work operation. |
| Identifiers and original language | `catalog.identifiers` and `catalog.works.set_original_language()`. |
| Physical Tag/Series vocabulary rows | Scalar lookup and Tag ensure/deletion, plus `get_tag_ids_from_values()`, `get_series_ids_from_values()`, `ensure_tags()`, `ensure_series()`, `delete_tags_by_id()` and `delete_series_by_id()`. Shared canonical rules apply. Row deletion can cascade links; it is not unlinking one Work's membership. |
| Semantic Tag/Series matching and Work memberships | `catalog.tags` / `catalog.series` repositories and membership methods, including atomic `replace_for_works()`. |
| Recorded file name/size/deletion | `db.metadata_sql.set_file_name()`, `set_file_size()`, `set_file_size_and_name()`, `update_files_bulk()`, `delete_file_by_id()` and `delete_files_by_id()`. IDs address `files.file_id`; byte count is `file_size_bytes`. No physical files are moved or removed. |
| Declared comparison-key maintenance | `audit_normalized_keys()` and `rebuild_normalized_keys()`, scoped by physical table, display columns and row IDs. Repair detects collisions and changes keys only; schema migration remains an explicit separate macro. |
| Feed rows | `db.metadata_sql.add_feed()`, `update_feed()`, `delete_feed()` and `set_feeds()`. Stored scripts are text, not executed recipes. |
| Cover flags, book paths, path substitution, conversion options | Obsolete entry points remain retired. Asset/replica selection, relocation and conversion ownership remain policy work in [CAT06](../catalog/next_tasks.md#cat06-replace-the-retired-metadata-sql-capabilities). |

The nine helpers targeting projection views, obsolete path columns or unresolved
conversion ownership remain removed, along with the earlier missing-table
methods. The unused `catalog.catalog_macros` facade also remains removed.
Neither the old `CM*` class names nor `SeriesTitleLinkMacros` are exported.
`series_mixin.py` contains physical Series lookup/ensure/deletion, without title links.

The cover writer refuses nonempty writes before database/cache effects; an empty
mapping remains a no-op. Legacy format removal also refuses before effects:
its old flow deleted file rows and then invoked a retired inventory reload.
Mapped filename/size callers already use portable macros directly, and persist
before changing their maps. An enclosing transaction rollback still requires
caller cache reconciliation. Restoring table SQL does not restore that cache's
unsupported inventory behavior.

`set_feeds()` materializes title/script pairs before deletion and atomically
replaces all feed rows. It composes with an enclosing macro transaction. The
following equivalent low-level recipe illustrates that same transaction owner:

```python
def replace_feed_rows(db, feeds):
    # This is an auxiliary-row recipe, not a Catalog API.
    rows = [{"feed_title": title, "feed_script": script} for title, script in feeds]
    with db.macros.transaction():
        ids = [(row["feed_id"],) for row in db.macros.get_rows("feeds")]
        db.macros.bulk_delete_in_table("feeds", "feed_id", ids)
        for row in rows:
            db.macros.insert_row("feeds", row)
```

Generic row deletion uses `db.macros.delete_row()` or `delete_rows_by_id()`;
generic batches use `update_rows()`. The table helpers accept
integer IDs or batches of IDs/one-cell bindings where documented. All schema
constraints/cascades still apply; none of these calls chooses retention policy
for WEMI attachments, assets or replicas.

Regression checks verify the physical tables and actual columns, concrete/API
signature parity, database rebinding/close, rollback, shared canonical lookup
and absence of retired compatibility methods. Catalog's executable-SQL ban and
the database-to-Catalog dependency guard remain in place.

Validation on **2026-10-30**: the broader run passed **191 tests** on SQLite
and APSW, including every restored operation, schema/signature checks, lifecycle,
Catalog ownership/conversion, refusal boundaries, mapped file writes and the
feed recipe. A final six-test lifecycle run passed after checking custom-helper
rebinding explicitly. The first focused run passed 52 tests and exposed two
incorrect normalization-column expectations in the new schema test; those
assertions were corrected against the DDL before the successful broader run.
The dependency-direction guard, Ruff lint/formatting for restored modules/tests,
and `git diff --check` pass. Full-suite and live PostgreSQL runs were not performed.
Test scratch files and telemetry stayed under `/tmp`.

The subsequent bulk/normalization extension passed **171 tests, with one expected
skip**, including 42 new SQLite/APSW batch cases. Coverage includes bounded
lookup counts, late-failure rollback across chunks, NULL identity scopes,
selected-row collisions, key swaps and schema/display preservation. The skip is
the existing PostgreSQL-shaped SQLite harness without `pg_temp`. Lint passes for
the changed implementation/API files and new test module; the dependency guard
retains its reviewed baseline. The [batch guide](table-sql-batches.md) records
input materialization, table-scan costs and the absence of a live PostgreSQL run.

The subsequent [WEMI relationship-read slice](table-sql-ownership.md) adds four
operations in three destination-table mixins and migrates their live Catalog
callers. Its broader run passed 196 tests with one expected skip, followed by
41 passing worked-example/ownership/retirement checks. Source validation,
missing-row errors, ordering and link metadata are retained; destination reads
are now bounded batches. The linked guide contains the physical-table inventory.

## Initial scope and outcome — historical

The initial audit covered **73 public operations in 19 mixins**, adding a
`WEMI status` note in both implementation and `MetadataSQLAPI`. The inventory
below retains that baseline.

The same-day [Expression conversion follow-up](../calibre_compat/expression-conversion-policy.md)
migrated title, author-sort, timestamp, comment, primary-language clearing and
rating persistence hooks to `catalog.calibre`. It retired the now-uncalled
`update_title_creator_sort`, `set_title_rating` and
`break_lang_title_primary_link` entry points from implementation and API.
That left 70 methods, of which 57 retained legacy flags. The original
classification counts describe the audit baseline; subsequent retirements are
marked individually below.
At the initial audit, legacy notes were migration flags rather than runtime
guards: helpers remained callable and could still fail. The physical-table boundary
above supersedes that state. Production code uses explicit Catalog/storage owners.

- **auxiliary**: 4 operations.
- **current compatibility**: 4 operations.
- **legacy**: 60 operations.
- **storage compatibility**: 5 operations.

At this stage the flattened package imported its mixins directly; no import from the
removed `metadata_sql.mixins` package remains. The eleven changed implementations
are feed CRUD/replacement, file row updates/deletion, canonical tag deletion and
canonical Series lookup. Tag lookup/insertion already used canonical identities.
File size now writes `file_size_bytes`; bulk deletion accepts integers and old
one-cell bindings. Writes compose with portable macro transactions, including
feed replacement and rollback after a partial insertion failure. This fixes the
feed commit escape recorded in the earlier ownership review.

No schema migration is included. In particular, compatibility `files` rows have
not been converted into assets or replicas, and no Agent/Series tag link or Series
sequence-position field has been invented.

## Missing-table retirement — 2026-10-30

Compared executable SQL against a fresh generated schema and removed **48 more
methods** whose queries reference absent tables, together with their API
contracts, exports and **11 empty mixin modules**. **22 methods in 8 mixins
remained at this intermediate stage: 9 legacy, 4 auxiliary, 4 current
compatibility and 5 storage compatibility.** The physical-table boundary retained
13 original operations and removed the nine remaining unsupported helpers. The
subsequent batch extension adds nine operations over the same physical schema.
Historical inventory entries below retain the migration advice.

Caller changes:

- Removed 15 unused frozen `catalog/catalog_macros.py` helpers, including the
  title-tag aliases. Historical implementations remain in version control.
- Legacy size, cover, author and format cache operations that called the removed
  SQL now raise `NotImplementedError` before accessing the database or changing
  their maps. Creator/publisher cleanup also refuses the operation: deleting an
  Agent requires a policy covering all roles and references. These boundaries
  do not make the rest of the legacy cache WEMI-compatible.
- The Work–Series persistence hook now replaces current memberships directly
  through the Catalog writer, in one transaction. Deletions override updates;
  outer rollback is preserved. It accepts explicit Work IDs, not Expression IDs
  or compatibility rays. It no longer reads a legacy primary index. The separate
  Series-index hook explicitly refuses writes; priority is not a sequence number.

At this intermediate stage, unresolved legacy work included five writes that still
address read-only `books`/`titles` views (`update_book_last_modified`,
`set_override_book_path`, `set_has_cover`, `set_author_sort`, `update_title`).
Folder/file path replacements still use obsolete columns on existing tables.
Two conversion-option helpers retain unresolved
ownership/transaction behavior; the setter also references missing serialization
imports. Use the explicit Catalog/storage owners instead of enabling these paths.

The intermediate regression guard compared literal SQL relation names to fresh schema
objects on SQLite and APSW. It does not prove column validity, view writability,
dynamic SQL safety or WEMI semantics. Retirement tests also check the API surface,
production references, cache preservation on refusal and real Work–Series
membership writes with enclosing rollback. Identically named fixture builders
and the separate SQL macro `replace_in_folder_store_path` are not calls into
`metadata_sql` and remain outside this cleanup.

Validation on **2026-10-30**: **134 distinct targeted tests passed**, including
SQLite and APSW coverage. The broader regression run passed all 107 tests;
27 additional metadata SQL compatibility tests passed in the earlier run.
The new test module passes Ruff lint and formatting checks; comparison with
the pre-change sources found no new lint diagnostics in modified production
files. The full suite and live PostgreSQL were not run for this retirement.

## Public Work memberships — 2026-10-30

The first [replacement checklist](../catalog/next_tasks.md#cat06-replace-the-retired-metadata-sql-capabilities)
slice adds `WorkMembershipRepositoryAPI` to both `catalog.tags` and
`catalog.series`: `link_to_work`, `unlink_from_work`, `is_linked_to_work`,
`list_for_work`, `replace_for_work` and atomic `replace_for_works`.
[Worked examples and semantics](../catalog/catalog-api-usage.md#work-tag-and-series-memberships)
show shared Tags, clearing, explicit IDs and preservation of retained link data.
The Series cache persistence hook now delegates to the public batch operation.

These are membership-set APIs, not positional writes. Retained Series link types,
priorities and provenance survive replacement; removing a pair clears all types.
New priorities are allocated within each Tag/Series's Works, matching physical
uniqueness constraints. They neither choose a primary Series nor represent a
volume number. Catalog's shared pair-unlink operation now uses portable pair
deletion without rewriting other links. Entity rows and Series hierarchy remain.

Direct Tag links currently exist only for Works. Agent/Series tagging and other
WEMI Tag levels are not enabled by these APIs. All remaining gaps stay on the
checklist; no legacy SQL method or missing schema table was restored.

Membership follow-up validation: **134 passed, 22 skipped** on 2026-10-30.
Coverage includes SQLite/APSW memberships, retained metadata, late-failure and
outer rollback, executable usage examples and existing Catalog/Core boundaries.
Skipped cases require a configured live PostgreSQL test connection. The new
modules/tests pass Ruff lint and formatting; the full suite was not run.

## Rules for completing the migration

- `books.book_id` is a **ray ID**, not a Work ID. A ray describes a WEMI path.
  Resolve the intended entity before calling a repository; numeric equality is
  not evidence of identity. `titles.title_id` projects `work_id`, but cache
  callers passing book IDs do not thereby become safe Work callers.
- Catalog chooses WEMI level, credit role, normalization and selection policy.
  Databases implements portable SQL and transaction mechanics. Do not introduce
  database imports of Catalog to preserve an ambiguous legacy signature.
- Current credits use Agents, including publisher organisations, with explicit
  roles. Removing one role must preserve other roles and levels. Missing data
  does not require fake Agent/Series row zero.
- Link priority is ordering/selection metadata, not a Series volume number.
  Decide the missing domain field before moving old index values.
- Storage owns assets, replicas and physical placement. A recorded filename
  update is not a filesystem rename; replica bytes and unique asset bytes are
  different aggregates.

## Caller follow-up, in order

1. Finish migrating cache identity/read models after the Expression policy slice:
   `caches/write/author_sort_writer.py`, `covers_writer.py`, and the generic
   one-to-one, many-to-one and many-to-many writers. The selected persistence
   hooks now use the policy, but cover flags, cleanup, remaining helpers and
   surrounding cache invalidation do not acquire WEMI support. The unused
   `catalog/catalog_macros.py` facade is now removed. Legacy Calibre table classes
   remain unsupported except for the individually migrated operations.
   Unsupported callers explicitly refuse effects; do not revive that facade.
2. Fix the compatibility read model deliberately: `titles_v` currently returns
   NULL for `title_creator_sort` even though `works.work_creator_sort` exists.
   Define title/canonical-title and cover selection before exposing updates
   through those projections. Keep physical writes out of compatibility views.
3. Decide Series sequence position, Agent/Series tagging and conversion-option
   ownership. These need domain/schema decisions rather than table renames.
4. Complete remaining capabilities through explicit owners. Retain concrete
   table SQL and accelerators in databases; move compatibility interpretation
   and semantic commands to Catalog/storage. The unsupported helpers stay retired.

## Historical method inventory

Entries distinguish retained physical-table operations from retired facades.
Original classifications and findings remain as migration context; historical
module headings do not imply that the old classes or imports remain available.

### `books_mixin.py`

- `update_book_last_modified` — **retired 2026-10-30: compatibility/unsupported operation**. Original classification: legacy. books is a read-only projection whose book_id is a ray ID. Choose the actual Work, Expression, Manifestation or Item before updating its timestamp; never substitute that ray ID for an entity ID.
- `set_override_book_path` — **retired 2026-10-30: compatibility/unsupported operation**. Original classification: legacy. book_paths is a read-only compatibility placeholder, not storage placement. Resolve assets and replicas through storage policy; a ray ID is not a file or replica ID.
- `read_book_id_with_cover_id_and_cover_nmame` — **retired 2026-10-30: missing tables**. Original finding: covers and book_cover_links are absent. Resolve cover assets/composites for an explicit WEMI owner and select a replica; the old cover ID/name tuple has no direct replacement.
- `read_book_id_with_file_id_file_ext_file_name_and_file_size` — **retired 2026-10-30: missing tables**. Original finding: book_file_links is absent and file_size has become file_size_bytes in the compatibility files table. New inventory reads must distinguish WEMI attachment, digital asset and replica; their IDs are not interchangeable.
- `read_file_backups_for_book` — **retired 2026-10-30: missing tables**. Original finding: file_file_intralinks is absent. Backups belong to replica/storage policy, not links between legacy file rows; resolve the asset and its replicas explicitly.
- `read_file_properties_for_book` — **retired 2026-10-30: missing tables**. Original finding: book_file_links is absent and file_size has become file_size_bytes in the compatibility files table. New inventory reads must distinguish WEMI attachment, digital asset and replica; their IDs are not interchangeable.
- `read_book_sizes_sum_mode` — **retired 2026-10-30: missing tables**. Original finding: book_folder_links and file_folder_links are absent. Define whether size counts unique asset bytes, replica bytes or a selected payload before introducing a WEMI aggregate; folder sums do not define that policy.
- `read_book_sizes_max_mode` — **retired 2026-10-30: missing tables**. Original finding: book_folder_links and file_folder_links are absent. Define whether size counts unique asset bytes, replica bytes or a selected payload before introducing a WEMI aggregate; folder sums do not define that policy.
- `read_book_sizes_min_mode` — **retired 2026-10-30: missing tables**. Original finding: book_folder_links and file_folder_links are absent. Define whether size counts unique asset bytes, replica bytes or a selected payload before introducing a WEMI aggregate; folder sums do not define that policy.
- `set_has_cover` — **retired 2026-10-30: compatibility/unsupported operation**. Original classification: legacy. books is a read-only view and cover presence is derived from associated assets. Attach/detach a cover for an explicit WEMI owner; setting a boolean cannot create a cover.
- `set_conversion_options` — **retired 2026-10-30: compatibility/unsupported operation**. Original classification: legacy. conversion_options survives as legacy storage, but conversion_option_book has no agreed WEMI owner. This implementation also references undefined sqlite/cPickle. Decide conversion job/configuration scope before replacing serialization or enabling writes.
- `delete_conversion_options` — **retired 2026-10-30: compatibility/unsupported operation**. Original classification: legacy. conversion_options survives as legacy storage, but its book ID is not an explicit WEMI owner. Decide conversion job/configuration scope and use a composable transaction; the commit flag cannot provide that contract.

### `clear_unused_mixin.py`

- `publisher_clear_unused` — **retired 2026-10-30: missing tables**. Original finding: publishers/creators and their title links are absent. Credits now reference Agents with roles. Any Agent cleanup must consider all roles, WEMI levels, subtype rows and other references, not just one old title-link table.
- `creator_clear_unused` — **retired 2026-10-30: missing tables**. Original finding: publishers/creators and their title links are absent. Credits now reference Agents with roles. Any Agent cleanup must consider all roles, WEMI levels, subtype rows and other references, not just one old title-link table.
- `break_lang_title_primary_link` — **retired after the Expression policy migration**. Removed from implementation/API; the following records the original finding. language_title_links is absent. For an explicitly resolved Work use Catalog.works.set_original_language(work_id, None); Expression language is a different relationship. Do not reinterpret a cache book/ray ID as a Work ID.

### `creator_tag_links_mixin.py`

- `break_creator_tag_link` — **retired 2026-10-30: missing tables**. Original finding: creator_tag_links is absent; the current schema also has no agent_tag_links. Decide whether Agent tagging belongs in the model and add a supported link capability before replacing this operation.
- `clear_tag_title_links_for_title` — **retired 2026-10-30: missing tables**. Current replacement: Use catalog.tags.replace_for_work(work_id=..., entity_ids=()) after resolving the Work ID.
- `check_for_tag_title_link` — **retired 2026-10-30: missing tables**. Current replacement: Use catalog.tags.is_linked_to_work(entity_id=tag_id, work_id=...).
- `add_tag_title_link` — **retired 2026-10-30: missing tables**. Current replacement: Use catalog.tags.link_to_work(entity_id=tag_id, work_id=...).

### `creator_title_links_mixin.py`

- `break_creator_title_links` — **retired 2026-10-30: missing tables**. Original finding: creator_title_links is absent, and the tuple default is interpolated as one SQL literal. Resolve Agent IDs and an explicit WEMI owner, then remove only the intended credit roles through Catalog; preserve other roles.
- `make_creator_title_links` — **retired 2026-10-30: missing tables**. Original finding: creator_title_links is absent. This code ignores creator_type, hardcodes authors and derives priority globally. Use Catalog.agents.link_to_wemi with an explicit owner, credit role and owner-scoped ordering.
- `clear_title_creator_links_for_given_type_and_title` — **retired 2026-10-30: missing tables**. Original finding: creator_title_links is absent. Despite its name, this method hardcodes authors. Use role-scoped Catalog Agent credit replacement for an explicit WEMI owner, retaining translator/editor and other credits.
- `check_for_title_author_link` — **retired 2026-10-30: missing tables**. Original finding: creator_title_links is absent. Use Catalog Agent credits for an explicit WEMI owner and role; author, translator and publisher credits are distinct, and their priority is scoped to the owner.
- `update_title_author_link_priority` — **retired 2026-10-30: missing tables**. Original finding: creator_title_links is absent. Use Catalog Agent credits for an explicit WEMI owner and role; author, translator and publisher credits are distinct, and their priority is scoped to the owner.

### `creators_mixin.py`

- `get_creator_sort` — **retired 2026-10-30: missing tables**. Original finding: creators is absent. Agent sorting uses agent_sort_name through Catalog.agents. Resolve the migrated Agent identity before reusing an old creator ID.
- `get_creator_link` — **retired 2026-10-30: missing tables**. Original finding: creators is absent and generic creator_link has no direct Agent equivalent. Decide whether the URL is a human/organisation web field or a curated identifier, then use that Catalog owner.
- `update_creator_links` — **retired 2026-10-30: missing tables**. Original finding: creators is absent and generic creator_link has no direct Agent equivalent. Decide whether the URL is a human/organisation web field or a curated identifier, then use that Catalog owner.
- `update_creator_sorts` — **retired 2026-10-30: missing tables**. Original finding: creators is absent. Agent sorting uses agent_sort_name through Catalog.agents. Resolve the migrated Agent identity before reusing an old creator ID.
- `read_creator_with_sort_and_link` — **retired 2026-10-30: missing tables**. Original finding: creators is absent. Replace the legacy name/sort/generic-URL tuple with an Agent read model using agent_canonical_name, agent_sort_name and an explicitly defined web/identifier field.

### `feeds_mixin.py`

- `add_feed` — **retained: physical-table SQL (2026-10-30)**. Original classification: auxiliary. feeds remains a non-WEMI auxiliary table. These operations now use portable macros and compose with outer macro transactions; no Work, Expression, Manifestation or Item ID is implied.
- `delete_feed` — **retained: physical-table SQL (2026-10-30)**. Original classification: auxiliary. feeds remains a non-WEMI auxiliary table. These operations now use portable macros and compose with outer macro transactions; no Work, Expression, Manifestation or Item ID is implied.
- `update_feed` — **retained: physical-table SQL (2026-10-30)**. Original classification: auxiliary. feeds remains a non-WEMI auxiliary table. These operations now use portable macros and compose with outer macro transactions; no Work, Expression, Manifestation or Item ID is implied.
- `set_feeds` — **retained: physical-table SQL (2026-10-30)**. Original classification: auxiliary. feeds remains a non-WEMI auxiliary table. These operations now use portable macros and compose with outer macro transactions; no Work, Expression, Manifestation or Item ID is implied.

### `files_mixin.py`

- `set_file_name` — **retained: physical-table SQL (2026-10-30)**. Original classification: storage compatibility. files remains a compatibility table with file_size_bytes. These methods now use portable macros, but require files.file_id, not a digital_asset_id or asset_replica_id. New storage operations must use the asset/replica owner; physical files are untouched.
- `set_file_size` — **retained: physical-table SQL (2026-10-30)**. Original classification: storage compatibility. files remains a compatibility table with file_size_bytes. These methods now use portable macros, but require files.file_id, not a digital_asset_id or asset_replica_id. New storage operations must use the asset/replica owner; physical files are untouched.
- `set_file_size_and_name` — **retained: physical-table SQL (2026-10-30)**. Original classification: storage compatibility. files remains a compatibility table with file_size_bytes. These methods now use portable macros, but require files.file_id, not a digital_asset_id or asset_replica_id. New storage operations must use the asset/replica owner; physical files are untouched.
- `delete_file_by_id` — **retained: physical-table SQL (2026-10-30)**. Original classification: storage compatibility. files remains a compatibility table with file_size_bytes. These methods now use portable macros, but require files.file_id, not a digital_asset_id or asset_replica_id. New storage operations must use the asset/replica owner; physical files are untouched.
- `delete_files_by_id` — **retained: physical-table SQL (2026-10-30)**. Original classification: storage compatibility. files remains a compatibility table with file_size_bytes. These methods now use portable macros, but require files.file_id, not a digital_asset_id or asset_replica_id. New storage operations must use the asset/replica owner; physical files are untouched.

### `folders_mixin.py`

- `replace_in_folder_store_path` — **retired 2026-10-30: missing tables**. Original finding: folder_stores is absent. Store roots use stores.store_root_uri; URI-aware relocation belongs to storage policy and cannot safely use global SQL substring replacement.
- `replace_in_folder_store_marker_path` — **retired 2026-10-30: missing tables**. Original finding: folder_stores and its marker path are absent. There is no universal WEMI/storage replacement; define the backend marker/configuration contract before migrating this operation.
- `replace_in_folder_path` — **retired 2026-10-30: compatibility/unsupported operation**. Original classification: legacy. folders now stores folder_relpath scoped to a Store, not folder_path. Relocation needs store-scoped key validation and storage policy, not global substring replacement.
- `replace_in_cover_path` — **retired 2026-10-30: missing tables**. Original finding: covers is absent. Cover bytes live in asset replicas; choose the attachment and replica and use storage relocation policy instead of rewriting a global cover path.
- `replace_in_file_path` — **retired 2026-10-30: compatibility/unsupported operation**. Original classification: legacy. files.file_path is absent. Compatibility files use file_storage_key; current placement uses asset replicas. The old doctest exercises an artificial legacy schema, not the current model. Relocation requires storage-owned key/URI semantics.

### `publisher_title_links_mixin.py`

- `clear_publisher_title_links_by_title_id` — **retired 2026-10-30: missing tables**. Original finding: publishers and publisher_title_links are absent. Publishing is an Agent credit role, usually on a Manifestation. Select the intended edition/level and resolve the Agent before reading or changing that credit.
- `check_for_title_id_publisher_id_link` — **retired 2026-10-30: missing tables**. Original finding: publishers and publisher_title_links are absent. Publishing is an Agent credit role, usually on a Manifestation. Select the intended edition/level and resolve the Agent before reading or changing that credit.
- `clear_null_publisher_links_from_title` — **retired 2026-10-30: missing tables**. Original finding: publisher_title_links is absent and WEMI has no publisher-ID-zero sentinel. An absent publishing credit represents unknown publisher; use an explicit owner and Agent role rather than manufacturing a null Agent.

### `publishers_mixin.py`

- `link_publisher_to_null_publisher_row` — **retired 2026-10-30: missing tables**. Original finding: publisher_title_links is absent and WEMI has no publisher-ID-zero sentinel. An absent publishing credit represents unknown publisher; use an explicit owner and Agent role rather than manufacturing a null Agent.

### `series_mixin.py`

- `library_unset_series` — **retired 2026-10-30: missing tables**. Current replacement: Use catalog.series.unlink_from_work(entity_id=series_id, work_id=...).
- `remove_unused_series` — **retired 2026-10-30: missing tables**. Original finding: series_title_links is absent. Series retention must consider current WEMI memberships and the series parent hierarchy; no links in one legacy table is not proof that a Series is unused.

### `series_tag_links_mixin.py`

- `unapply_series_tags` — **retired 2026-10-30: missing tables**. Original finding: Neither series_tag_links nor tag_series_links exists in the current schema. Decide between direct Series annotations and derived Work tags, then add the chosen supported capability; generic Catalog relationships cannot invent it.
- `clear_series_tag_links_for_series` — **retired 2026-10-30: missing tables**. Original finding: Neither series_tag_links nor tag_series_links exists in the current schema. Decide between direct Series annotations and derived Work tags, then add the chosen supported capability; generic Catalog relationships cannot invent it.
- `check_for_series_tag_link` — **retired 2026-10-30: missing tables**. Original finding: Neither series_tag_links nor tag_series_links exists in the current schema. Decide between direct Series annotations and derived Work tags, then add the chosen supported capability; generic Catalog relationships cannot invent it.
- `add_series_tag_link` — **retired 2026-10-30: missing tables**. Original finding: Neither series_tag_links nor tag_series_links exists in the current schema. Decide between direct Series annotations and derived Work tags, then add the chosen supported capability; generic Catalog relationships cannot invent it.

### `series_title_links_mixin.py`

- `get_series_id_from_value` — **retained: physical-table SQL (2026-10-30)**. Original classification: current compatibility. series is current. Lookup now uses the shared canonical-identity service instead of raw equality, matching the normalized write path. New semantic callers should use Catalog.series; this does not restore legacy title links.
- `check_for_series_title_link` — **retired 2026-10-30: missing tables**. Current replacement: Use catalog.series.is_linked_to_work(entity_id=series_id, work_id=...) for membership only; no sequence index is returned.
- `get_primary_series_index` — **retired 2026-10-30: missing tables**. Original finding: series_title_links is absent. Current series_work_links has priority but no dedicated sequence index. Membership priority is not a book number; add a defined sequence-position field/API before migrating this behavior.
- `break_series_title_link` — **retired 2026-10-30: missing tables**. Current replacement: Use catalog.series.unlink_from_work(entity_id=series_id, work_id=...).
- `link_null_series_to_title` — **retired 2026-10-30: missing tables**. Original finding: series_title_links is absent. WEMI has no Series-ID-zero sentinel or field for an index without Series membership. Define that requirement explicitly; do not repurpose link priority as sequence position.
- `read_primary_title_series_id_from_meta` — **retired 2026-10-30: missing tables**. Original finding: meta is absent. Read catalog.series.list_for_work(work_id) and define primary selection when several Series apply; do not derive ownership from a book/ray ID.
- `update_index_for_series_title_link` — **retired 2026-10-30: missing tables**. Original finding: series_title_links is absent. Current series_work_links has priority but no dedicated sequence index. Membership priority is not a book number; add a defined sequence-position field/API before migrating this behavior.
- `get_title_series_ids_set` — **retired 2026-10-30: missing tables**. Current replacement: Use catalog.series.list_for_work(work_id) and collect series_id values; typed links can repeat an entity.

### `tag_links_mixin.py`

- `clear_creator_tag_links_for_creator` — **retired 2026-10-30: missing tables**. Original finding: creator_tag_links is absent; the current schema also has no agent_tag_links. Decide whether Agent tagging belongs in the model and add a supported link capability before replacing this operation.
- `check_for_creator_tag_link` — **retired 2026-10-30: missing tables**. Original finding: creator_tag_links is absent; the current schema also has no agent_tag_links. Decide whether Agent tagging belongs in the model and add a supported link capability before replacing this operation.
- `add_creator_tag_link` — **retired 2026-10-30: missing tables**. Original finding: creator_tag_links is absent; the current schema also has no agent_tag_links. Decide whether Agent tagging belongs in the model and add a supported link capability before replacing this operation.

### `tag_title_links_mixin.py`

- `break_tag_title_link` — **retired 2026-10-30: missing tables**. Current replacement: Use catalog.tags.unlink_from_work(entity_id=tag_id, work_id=...).

### `tags_mixin.py`

- `delete_tag_by_value` — **retained: physical-table SQL (2026-10-30)**. Original classification: current compatibility. tags is a current shared vocabulary. These helpers use the canonical-identity policy; writes now share portable macro transactions. New semantic callers should use Catalog.tags and explicit owner/tag relationships.
- `get_tag_id_from_value` — **retained: physical-table SQL (2026-10-30)**. Original classification: current compatibility. tags is a current shared vocabulary. These helpers use the canonical-identity policy; writes now share portable macro transactions. New semantic callers should use Catalog.tags and explicit owner/tag relationships.
- `add_tag` — **retained: physical-table SQL (2026-10-30)**. Original classification: current compatibility. tags is a current shared vocabulary. These helpers use the canonical-identity policy; writes now share portable macro transactions. New semantic callers should use Catalog.tags and explicit owner/tag relationships.

### `title_comments_mixin.py`

- `clear_title_comments_from_title_id` — **retired 2026-10-30: missing tables**. Original finding: comment_title_links is absent. Use Catalog.comments.replace_for_wemi with data=None and an explicit owner/level; comment_work_links is the Work capability, not a generic book-comment link.

### `title_values_mixin.py`

- `set_title_rating` — **retired after the Expression policy migration**. Removed from implementation/API; the following records the original finding. rating_title_links is absent. A rating value plus one is not a WEMI rating ID. Use Catalog.ratings and supported owner/rating links with explicit value, scale and provenance; zero is a value decision, not an implicit delete sentinel.
- `set_author_sort` — **retired 2026-10-30: compatibility/unsupported operation**. Original classification: legacy. titles is a read-only Work projection. Use Catalog.works.update with work_creator_sort for an explicit Work ID; cache book IDs are ray IDs. The compatibility titles view still projects NULL for title_creator_sort and needs a separate read-model fix.

### `titles_mixin.py`

- `update_title` — **retired 2026-10-30: compatibility/unsupported operation**. Original classification: legacy. titles is a read-only Work projection. Use Catalog.works.update for an explicit Work ID, distinguishing work_title from work_canonical_title; the compatibility title projection prefers the canonical title. Never substitute a ray ID.
- `update_title_creator_sort` — **retired after the Expression policy migration**. Removed from implementation/API; the following records the original finding. titles is a read-only Work projection. Use Catalog.works.update with work_creator_sort for an explicit Work ID; cache book IDs are ray IDs. The compatibility titles view still projects NULL for title_creator_sort and needs a separate read-model fix.

## Evidence and validation

Schema sources are the packaged `database_generator_frbr` table/link definitions
and `aggregate_sql/wemi_views.sql`, checked against a generated current-schema
fixture. Repository behavior is defined under `catalog/repositories`; portable
writes use `database_driver_plugins/SQL/macros`.

The initial regression coverage lived in the now-retired
`tests/databases/test_metadata_sql_wemi.py`: current
schema columns, atomic feed replacement, enclosing-transaction rollback, legacy
batch bindings, shared canonical identities and complete migration-note coverage.
Verification on **2026-10-29**: **59 distinct tests passed** across SQLite and
APSW, including the new compatibility regressions, Catalog ownership, portable
link deletion and the Catalog executable-SQL boundary. The initial run passed
55 tests; four malformed-batch tests expected the wrong exception class. After
correcting those assertions to the macro contract's `ValueError`, all four
passed in the focused follow-up. No production fix was needed for those failures.

All mixin modules parse, targeted undefined-name checks pass for changed
implementations, and `git diff --check` passes. This was not a full-suite or live
PostgreSQL run and did not establish support for the then-60 flagged legacy methods.
