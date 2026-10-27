# LiuXin database field guide

Reviewed **2026-10-20** against a fresh SQLite database produced by the packaged
FRBR generator. The inventory covers all **67 explicit tables** in `table_sql`,
with column names and declared SQLite types checked against `PRAGMA table_info`.
Generated relations are described separately below. This is not a PostgreSQL
physical-type inventory.

Use the [schema overview](<../03 - Database Schema.md>) for relationships and
[column metadata](column-metadata.md) for normalization declarations. Each
explicit table below links to its DDL for nullability, defaults, foreign keys and
checks. A stored policy describes intent; it does not establish that every
runtime mutation enforces it. See [next tasks](next_tasks.md).

## Scope and reading notes

- Row/source timestamp columns ending in `_ep_k` use epoch milliseconds.
- A `*_scratch` name is not permission to delete its contents. Some owners store
  recovery state there, notably `storage_ingest_operation_scratch`.
- `files` and `file_derivations` remain compatibility records; new storage design
  distinguishes atomic assets, logical composites and observed replicas.
- Normalized keys are derived through the declared column identity policy,
  rather than by choosing a local lowercase rule.

## Constant and database-metadata tables

### `languages`
Locked constant language catalogue used for canonical language lookup and display.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/constants_tables/languages.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `language_id` | `INTEGER` | Primary key for the table. |
| `language` | `TEXT` | English display name used for legacy/UI compatibility. |
| `language_code` | `TEXT` | Canonical language code field kept for legacy compatibility; intended to hold the stable ISO-639-2/B-style code. |
| `language_iso639_1` | `TEXT` | Two-letter ISO 639-1 code when one exists. |
| `language_iso639_2_b` | `TEXT` | Bibliographic three-letter ISO 639-2/B code. |
| `language_iso639_2_t` | `TEXT` | Terminology three-letter ISO 639-2/T code when it differs. |
| `language_bcp47_primary` | `TEXT` | Primary BCP 47 language tag for canonicalisation. |
| `language_bcp47_variants` | `TEXT` | Convenience list/blob of common BCP 47 variants for the language. |
| `language_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `language_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `language_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `language_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `language_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `library_id`
Per-library stable identifier table.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/library_id.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `library_id` | `INTEGER` | Identifier/reference field for library. |
| `library_id_uuid` | `TEXT` | Stable UUID for the library instance. |
| `library_id_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `library_id_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `library_id_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `library_id_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `library_id_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `database_version`
Tracks the schema/application version written into the database.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/database_version.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `database_version_id` | `TEXT` | Primary key for the table. |
| `database_version_version` | `TEXT` | Stored schema/application version string. |
| `database_version_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `database_version_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `database_version_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `database_version_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |

### `database_metadata`
Small bag of database-instance metadata.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/database_metadata.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `database_metadata_id` | `INTEGER` | Primary key for the table. |
| `database_metadata_unique_id` | `TEXT` | Stable unique identifier for this database instance. |
| `database_metadata_parent_LiuXin_instance` | `TEXT` | Identifier for the parent/syncing LiuXin instance when relevant. |
| `database_metadata_db_name` | `TEXT` | Human-facing name of the database/library. |
| `database_metadata_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `database_metadata_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `database_metadata_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `database_metadata_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `database_metadata_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `preferences`
Key/value preferences stored inside the database.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/preferences.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `preference_id` | `INTEGER` | Primary key for the table. |
| `preference_key` | `TEXT` | Preference name/key. |
| `preference_value` | `TEXT` | Serialised preference value. |
| `preference_value_type` | `TEXT` | Type hint for decoding the stored preference value. |
| `preference_parent_liuxin_instance` | `TEXT` | Owning/parent LiuXin instance for the preference when syncing. |
| `preference_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `preference_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `preference_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `preference_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `preference_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `custom_columns`
Custom-column registry describing user-defined metadata fields.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/custom_columns.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `custom_column_id` | `INTEGER` | Primary key for the table. |
| `custom_column_mark_for_delete` | `INTEGER` | Soft-delete marker for a custom column definition. |
| `custom_column_in_table` | `TEXT` | Main table this custom column attaches to. |
| `custom_column_label` | `TEXT` | Short stable label/key used in UI/API references. |
| `custom_column_label_norm` | `TEXT` | Derived identity key governed by the column normalization declaration. |
| `custom_column_name` | `TEXT` | Human-readable custom-column name. |
| `custom_column_name_norm` | `TEXT` | Derived identity key governed by the column normalization declaration. |
| `custom_column_datatype` | `TEXT` | Logical LiuXin datatype for the custom column. |
| `custom_column_db_datatype` | `TEXT` | Concrete database datatype used for storage. |
| `custom_column_is_multiple` | `INTEGER` | Boolean flag: the custom column can hold multiple values. |
| `custom_column_normalized` | `INTEGER` | Boolean flag: values live in a normalised side/link table instead of inline. |
| `custom_column_editable` | `INTEGER` | Boolean flag: the column is user-editable. |
| `custom_column_display` | `TEXT` | Display/rendering configuration blob. |
| `custom_column_display_sort` | `INTEGER` | Boolean flag: use display transform when sorting. |
| `custom_column_ordered` | `INTEGER` | Boolean flag: multi-values are order-sensitive. |
| `custom_column_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `custom_column_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `custom_column_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `custom_column_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `custom_column_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `hashes`
Generic hash-value table used where a separate hash object is still desired.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/hashes.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `hash_id` | `TEXT` | Identifier/reference field for hash. |
| `hash` | `TEXT` | Hash value text. |
| `hash_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `hash_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `hash_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `hash_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `hash_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `feeds`
Saved feed/update sources.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/feeds.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `feed_id` | `INTEGER` | Primary key for the table. |
| `feed_title` | `TEXT` | Display title for the feed definition. |
| `feed_script` | `TEXT` | Fetcher/update script or stored logic for the feed. |
| `feed_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `feed_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `feed_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `feed_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `feed_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `last_read_positions`
Per-user/per-device reading position checkpoints.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/last_read_position.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `last_read_position_id` | `INTEGER` | Primary key for the table. |
| `last_read_position_book` | `INTEGER` | Legacy target record identifier for the thing being read. |
| `last_read_position_format` | `TEXT` | Format code associated with the reading position. |
| `last_read_position_user` | `TEXT` | User identity that owns the checkpoint. |
| `last_read_position_device` | `TEXT` | Device identity string that reported the checkpoint. |
| `last_read_position_cfi` | `TEXT` | Location string/CFI or equivalent reader position token. |
| `last_read_position_epoch` | `REAL` | Timestamp reported by the reader/source system for the checkpoint. |
| `last_read_position_pos_frac` | `REAL` | Approximate progress fraction through the resource. |
| `last_read_position_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `last_read_position_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `last_read_position_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `last_read_position_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `last_read_positions_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `compressed_files`
Legacy queue/cache table for compressed files discovered during ingest.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/compressed_files.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `compressed_file_id` | `INTEGER` | Primary key for the table. |
| `compressed_file_name` | `TEXT` | Legacy discovered filename. |
| `compressed_file_extension` | `INTEGER` | Legacy extension/format marker for the discovered compressed file. |
| `compressed_file_path` | `INTEGER` | Legacy path reference for the discovered compressed file. |
| `compressed_file_hash_1` | `TEXT` | Primary legacy hash value for the compressed file. |
| `compressed_file_hash_2` | `INTEGER` | Secondary legacy hash/grouping value. |
| `compressed_file_size` | `INTEGER` | Observed compressed file size. |
| `compressed_file_group_id` | `INTEGER` | Legacy grouping/batch identifier. |
| `compressed_file_folder` | `INTEGER` | Legacy folder reference for the compressed file. |
| `compressed_file_cached` | `INTEGER` | Boolean flag: the compressed file has been cached locally. |
| `compressed_file_cache_attempted` | `INTEGER` | Boolean flag: caching was attempted even if it failed. |
| `compressed_file_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `compressed_file_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `compressed_file_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `compressed_file_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `compressed_file_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `new_books`
Legacy queue/cache table for newly discovered books pending ingest.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/new_books.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `new_book_id` | `INTEGER` | Primary key for the table. |
| `new_book_name` | `TEXT` | Legacy discovered book filename/title token. |
| `new_book_extension` | `INTEGER` | Legacy extension/format marker for the discovered book. |
| `new_book_path` | `INTEGER` | Legacy path reference for the discovered book. |
| `new_book_hash_1` | `TEXT` | Primary legacy hash value for the discovered book. |
| `new_book_hash_2` | `INTEGER` | Secondary legacy hash/grouping value. |
| `new_book_size` | `INTEGER` | Observed size of the discovered book file. |
| `new_book_group_id` | `INTEGER` | Legacy grouping/batch identifier. |
| `new_book_cached` | `INTEGER` | Boolean flag: the new book has been cached locally. |
| `new_book_cache_attempted` | `INTEGER` | Boolean flag: caching was attempted even if it failed. |
| `new_book_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `new_book_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `new_book_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `new_book_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `new_book_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `conversion_options`
Per-format or per-book conversion setting blobs.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/conversion_options.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `conversion_option_id` | `INTEGER` | Primary key for the table. |
| `conversion_option_format` | `TEXT` | Target/output format the conversion options apply to. |
| `conversion_option_book` | `INTEGER` | Legacy book identifier the options are scoped to, or NULL for format-wide defaults. |
| `conversion_option_data` | `BLOB` | Opaque serialised conversion-options blob. |
| `conversion_option_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `conversion_option_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `conversion_option_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `conversion_option_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |

### `metadata_dirtied_books`
Queue of records whose metadata needs recalculation or export.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/metadata_dirtied.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `metadata_dirtied_id` | `TEXT` | Identifier/reference field for metadata dirtied. |
| `metadata_dirtied_table_id` | `INTEGER` | Primary key of the row marked dirty. |
| `metadata_dirtied_table` | `TEXT` | Table name containing the row marked dirty. |
| `metadata_drtied_reason` | `TEXT` | Reason the row was marked dirty and needs follow-up work. |
| `metadata_dirtied_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `metadata_dirtied_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `metadata_dirtied_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `metadata_dirtied_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `metadata_dirtied_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

## FRBR core tables
### `works`
FRBR Work rows: the conceptual work level.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/frbr_metadata_core_tables/1-works.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `work_id` | `INTEGER` | Primary key for the table. |
| `work_type` | `TEXT` | High-level work kind such as novel, collection, film, or series. |
| `work_medium` | `TEXT` | Primary medium of the work such as text, audio, moving_image, or mixed. |
| `work_title` | `TEXT` | Display title for the work concept. |
| `work_canonical_title` | `TEXT` | Canonicalised title used for matching and dedupe. |
| `work_sort_title` | `TEXT` | Sort title that ignores leading articles or similar noise. |
| `work_creator_sort` | `TEXT` | Cached creator-sort string used in views and ordering. |
| `work_flags` | `TEXT` | Free-form/internal flag field for work-level import or handling state. |
| `work_original_language_id` | `INTEGER` | Language the work was originally created in. |
| `work_original_year` | `INTEGER` | Best-known original year of creation/publication. |
| `work_original_date` | `INTEGER` | Best-known original date in integer/date-normalised form. |
| `work_original_copyright_date` | `TEXT` | Original copyright date text as supplied by source metadata. |
| `work_wikipedia_link` | `TEXT` | Reference link for the work in Wikipedia or a similar authority source. |
| `work_is_fiction` | `INTEGER` | Tri-state fiction flag: 1 fiction, 0 non-fiction, NULL unknown. |
| `work_audience` | `TEXT` | Intended audience bucket such as adult, YA, children, or all_ages. |
| `work_completion_status` | `TEXT` | Completion state for the work as a concept/series. |
| `work_discovery_note` | `TEXT` | Free-form note about how/why the work was identified. |
| `work_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `work_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `work_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `work_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `work_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `expressions`
FRBR Expression rows: a language/version/realisation of a work.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/frbr_metadata_core_tables/2-expressions.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `expression_id` | `INTEGER` | Primary key for the table. |
| `expression_type` | `TEXT` | Expression kind such as text, translation, revision, or dub. |
| `expression_label` | `TEXT` | Human label distinguishing the expression from siblings. |
| `expression_year` | `INTEGER` | Year this expression was produced/released. |
| `expression_is_preferred` | `INTEGER` | Tri-state flag identifying the preferred expression in a set. |
| `expression_original_date` | `INTEGER` | Original date for this expression specifically. |
| `expression_original_copyright_date` | `TEXT` | Copyright date text for this expression. |
| `expression_flags` | `TEXT` | Free-form/internal flag field for expression-level handling state. |
| `expression_language_id` | `INTEGER` | Language of the expression. |
| `expression_mode` | `TEXT` | Mode of expression such as text, spoken_word, moving_image, music, or mixed. |
| `expression_title_override` | `TEXT` | Explicit title override when the expression title differs from the work title. |
| `expression_subtitle` | `TEXT` | Subtitle specific to this expression. |
| `expression_wordcount` | `INTEGER` | Approximate word count for text expressions. |
| `expression_fiction_length_category` | `TEXT` | Length bucket such as short_story, novella, or novel. |
| `expression_cut_type` | `TEXT` | Variant/cut label for AV expressions. |
| `expression_nominal_duration_seconds` | `INTEGER` | Approximate duration for timed expressions. |
| `expression_status` | `TEXT` | State such as complete, fragment, or draft. |
| `expression_origin_note` | `TEXT` | Free-form provenance/origin note for the expression. |
| `expression_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `expression_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `expression_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `expression_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `expression_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `manifestations`
FRBR Manifestation rows: edition/product-level embodiment.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/frbr_metadata_core_tables/3-manifestations.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `manifestation_id` | `INTEGER` | Primary key for the table. |
| `manifestation_subtitle` | `TEXT` | Subtitle as issued on this manifestation. |
| `manifestation_carrier_type` | `TEXT` | Carrier/product type such as print_book, ebook, audiobook, or disc. |
| `manifestation_format_detail` | `TEXT` | More specific format detail such as EPUB, PDF, paperback size, or UHD BD. |
| `manifestation_edition_statement` | `TEXT` | Edition statement as issued. |
| `manifestation_pub_year` | `INTEGER` | Publication year for the manifestation. |
| `manifestation_pub_date` | `TEXT` | Publication date for the manifestation. |
| `manifestation_flags` | `TEXT` | Free-form/internal manifestation flags. |
| `manifestation_page_count` | `INTEGER` | Stable page count for the issued manifestation when applicable. |
| `manifestation_runtime_minutes` | `INTEGER` | Stable runtime for the issued manifestation when applicable. |
| `manifestation_region_code` | `TEXT` | Region code/restriction for AV/media manifestations. |
| `manifestation_status` | `TEXT` | Publication/availability status such as in_print or out_of_print. |
| `manifestation_note` | `TEXT` | Free-form note about the manifestation. |
| `manifestation_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `manifestation_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `manifestation_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `manifestation_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `manifestation_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `items`
FRBR Item rows: one specific acquired/held exemplar or digital copy-context.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/frbr_metadata_core_tables/4-items.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `item_id` | `INTEGER` | Primary key for the table. |
| `item_manifestation_id` | `INTEGER` | Manifestation this item exemplifies. |
| `item_flags` | `TEXT` | Free-form/internal item flags. |
| `item_type` | `TEXT` | Item kind such as digital or physical. |
| `item_location` | `TEXT` | Physical shelf/box or logical location note for the item. |
| `item_inventory_code` | `TEXT` | Barcode or internal inventory/custody code. |
| `item_original_date` | `INTEGER` | Original date attached specifically to this copy/exemplar if needed. |
| `item_original_copyright_date` | `TEXT` | Original copyright text attached specifically to this item. |
| `item_source` | `TEXT` | Source channel such as calibre, scan, web_dl, or manual. |
| `item_source_detail` | `TEXT` | Detailed provenance string, seller, library, URL, etc. |
| `item_source_path` | `TEXT` | Original source path/URI for the item. |
| `item_source_name` | `TEXT` | Human-facing name of the source. |
| `item_acquired_date` | `TEXT` | Date the item was acquired. |
| `item_acquired_price_minor` | `INTEGER` | Acquisition price in minor currency units. |
| `item_lifecycle_status` | `TEXT` | Lifecycle/custody state such as active, lost, replaced, or withdrawn. |
| `item_condition` | `TEXT` | Condition assessment for the exemplar. |
| `item_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `item_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `item_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `item_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `item_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `agents`
Agent supertype table for people, organisations, groups, and pseudonyms.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/frbr_metadata_core_tables/5-agents.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `agent_id` | `INTEGER` | Primary key for the table. |
| `agent_type` | `TEXT` | Agent supertype discriminator: person, organisation, group, or pseudonym. |
| `agent_canonical_name` | `TEXT` | Canonical display name for the agent. |
| `agent_sort_name` | `TEXT` | Sort name used for ordering/search. |
| `agent_aliases` | `TEXT` | Alias/pseudonym list or serialised alias blob. |
| `agent_note` | `TEXT` | Free-form note about the agent. |
| `agent_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `agent_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `agent_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `agent_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `agent_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `human_agents`
1:1 subtype table for person-specific agent data.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/frbr_metadata_core_tables/5-agents.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `human_agent_id` | `INTEGER` | Primary key for the table. |
| `human_agent_agent_id` | `INTEGER` | Owning `agents` row for this human-agent subtype row. |
| `human_agent_given_name` | `TEXT` | Given/first name component. |
| `human_agent_middle_name` | `TEXT` | Middle name component. |
| `human_agent_family_name` | `TEXT` | Family/surname component. |
| `human_agent_prefix` | `TEXT` | Honorific/prefix component. |
| `human_agent_suffix` | `TEXT` | Suffix component. |
| `human_agent_preferred_name` | `TEXT` | Preferred public/stage/pen form still treated as the same person. |
| `human_agent_birth_date` | `TEXT` | Birth date text/normalised value. |
| `human_agent_death_date` | `TEXT` | Death date text/normalised value. |
| `human_agent_nationality` | `TEXT` | Nationality or associated nationality text. |
| `human_agent_biography` | `TEXT` | Biography/biographical note. |
| `human_agent_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `human_agent_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `human_agent_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `org_agents`
1:1 subtype table for organisation-specific agent data.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/frbr_metadata_core_tables/5-agents.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `org_agent_id` | `INTEGER` | Primary key for the table. |
| `org_agent_agent_id` | `INTEGER` | Owning `agents` row for this organisation subtype row. |
| `org_agent_legal_name` | `TEXT` | Legal name of the organisation. |
| `org_agent_trading_name` | `TEXT` | Trading/operating/public name of the organisation. |
| `org_agent_registration_id` | `TEXT` | Company/charity/etc. registration identifier. |
| `org_agent_jurisdiction` | `TEXT` | Jurisdiction under which the organisation exists. |
| `org_agent_founded_date` | `TEXT` | Founding/establishment date. |
| `org_agent_dissolved_date` | `TEXT` | Dissolution/end date. |
| `org_agent_website` | `TEXT` | Primary website URL. |
| `org_agent_contact_email` | `TEXT` | Contact email address. |
| `org_agent_description` | `TEXT` | Free-form description of the organisation. |
| `org_agent_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `org_agent_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `org_agent_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `org_agent_relations`
Parent/child or related-organisation graph edges.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/frbr_metadata_core_attributes/agents_graph_table.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `org_agent_relation_id` | `INTEGER` | Primary key for the table. |
| `org_agent_relation_child_agent_id` | `INTEGER` | Child/subordinate organisation agent. |
| `org_agent_relation_parent_agent_id` | `INTEGER` | Parent/umbrella organisation agent. |
| `org_agent_relation_type` | `TEXT` | Relation type such as imprint_of, subsidiary_of, or department_of. |
| `org_agent_relation_start_date` | `TEXT` | Date the organisational relation started. |
| `org_agent_relation_end_date` | `TEXT` | Date the organisational relation ended. |
| `org_agent_relation_note` | `TEXT` | Free-form note about the relation. |
| `org_agent_relation_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `org_agent_relation_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `org_agent_relation_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `org_agent_relation_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `org_agent_relation_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `entity_identifiers`
Identifier table for WEMI entities and related top-level entities.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/frbr_metadata_core_attributes/entity_identifiers.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `entity_identifier_id` | `INTEGER` | Primary key for the table. |
| `entity_identifier_entity_type` | `TEXT` | Type of entity the identifier belongs to. |
| `entity_identifier_entity_id` | `INTEGER` | Primary key of the identified entity. |
| `entity_identifier_scheme` | `TEXT` | Identifier scheme such as ISBN, ASIN, DOI, Wikidata, etc. |
| `entity_identifier_value` | `TEXT` | Identifier value as supplied/canonicalised. |
| `entity_identifier_is_primary` | `INTEGER` | Boolean flag: preferred identifier for the scheme/entity. |
| `entity_identifier_provenance` | `TEXT` | Source/provenance note for the identifier. |
| `entity_identifier_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `entity_identifier_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `entity_identifier_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `entity_identifier_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `entity_identifier_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `item_identifiers`
Identifier table specifically for items/copies.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/frbr_metadata_core_attributes/item_identifiers.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `item_identifier_id` | `INTEGER` | Primary key for the table. |
| `item_identifier_item_id` | `INTEGER` | Item the identifier belongs to. |
| `item_identifier_scheme` | `TEXT` | Item-identifier scheme such as barcode, accession, vendor order, etc. |
| `item_identifier_value` | `TEXT` | Identifier value. |
| `item_identifier_source` | `TEXT` | Where the item identifier came from. |
| `item_identifier_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `item_identifier_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `item_identifier_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `item_identifier_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `item_identifier_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

## Additional metadata tables
### `annotations`
User/device annotations anchored to items.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/metadata_additional/annotations.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `annotation_id` | `INTEGER` | Primary key for the table. |
| `annotation_user_id` | `INTEGER` | User who owns/authored the annotation. |
| `annotation_item_id` | `INTEGER` | Item the annotation is anchored to. |
| `annotation_kind` | `TEXT` | Annotation kind such as highlight, note, bookmark, or correction. |
| `annotation_anchor_type` | `TEXT` | Anchor method used to locate the annotation. |
| `annotation_anchor_start` | `TEXT` | Start anchor token/offset. |
| `annotation_anchor_end` | `TEXT` | End anchor token/offset. |
| `annotation_selected_text` | `TEXT` | Selected/highlighted text if captured. |
| `annotation_note_text` | `TEXT` | User note text attached to the annotation. |
| `annotation_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `annotation_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `annotation_source_deleted_datestamp_ep_k` | `INTEGER` | Source-side deletion timestamp if the source annotation was removed. |
| `annotation_source` | `TEXT` | Reader/app/source system that supplied the annotation. |
| `annotation_device_id` | `INTEGER` | Device that created or synced the annotation. |
| `annotation_extra_json` | `TEXT` | Extra annotation payload not yet normalised. |
| `annotation_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `annotation_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `annotation_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `comments`
Free-form comment blob table.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/metadata_additional/comments.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `comment_id` | `INTEGER` | Primary key for the table. |
| `comment` | `TEXT` | Free-form comment text. |
| `comment_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `comment_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `comment_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `comment_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `comment_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `notes`
Free-form note blob table.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/metadata_additional/notes.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `note_id` | `INTEGER` | Primary key for the table. |
| `note` | `TEXT` | Free-form note text. |
| `note_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `note_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `note_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `note_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `note_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `synopses`
Synopsis/summary text blobs.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/metadata_additional/synopses.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `synopsis_id` | `INTEGER` | Identifier/reference field for synopsis. |
| `synopsis` | `TEXT` | Free-form synopsis/summary text. |
| `synopsis_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `synopsis_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `synopsis_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `synopsis_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `synopsis_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `ratings`
Normalised rating values and provenance.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/metadata_additional/ratings.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `rating_id` | `INTEGER` | Primary key for the table. |
| `rating` | `FLOAT` | Rating value. |
| `rating_out_of` | `INT` | Scale maximum the rating is out of. |
| `rating_for_calibre_tag_viewer` | `INT` | Compatibility flag/value for Calibre tag-viewer behaviour. |
| `rating_source` | `TEXT` | Source of the rating. |
| `rating_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `rating_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `rating_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `rating_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `rating_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `labels`
Short reusable labels/tags used across the schema.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/metadata_additional/labels.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `label_id` | `INTEGER` | Primary key for the table. |
| `label_text` | `TEXT` | Raw label text. |
| `label_text_norm` | `TEXT` | Normalised label text for case-insensitive matching. |
| `label_description` | `TEXT` | Longer explanation of what the label means. |
| `label_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |
| `label_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `label_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `label_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `label_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |

### `genres`
Genre tree table.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/metadata_additional/genres.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `genre_id` | `INTEGER` | Primary key for the table. |
| `genre` | `TEXT` | Display name of the genre. |
| `genre_sort` | `TEXT` | Sort form of the genre name. |
| `genre_phash` | `TEXT` | Phonetic/hash helper for fuzzy genre matching. |
| `genre_parent_id` | `INTEGER` | Parent genre in the hierarchy. |
| `genre_position` | `INTEGER` | Sibling-order/position among children. |
| `genre_tree_id` | `INTEGER` | Tree identifier when multiple hierarchies are supported. |
| `genre_full` | `TEXT` | Cached full path/name for the genre. |
| `genre_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `genre_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `genre_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `genre_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `genre_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `subjects`
Subject tree table.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/metadata_additional/subjects.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `subject_id` | `INTEGER` | Primary key for the table. |
| `subject` | `TEXT` | Display name of the subject. |
| `subject_phash` | `TEXT` | Phonetic/hash helper for fuzzy subject matching. |
| `subject_sort` | `TEXT` | Sort form of the subject name. |
| `subject_parent_id` | `INTEGER` | Parent subject in the hierarchy. |
| `subject_parent_position` | `INTEGER` | Sibling-order/position among children. |
| `subject_tree_id` | `TEXT` | Tree identifier when multiple hierarchies are supported. |
| `subject_full` | `TEXT` | Cached full path/name for the subject. |
| `subject_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `subject_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `subject_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `subject_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `subject_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `series`
Series tree table, including nested/parent series.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/metadata_additional/series.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `series_id` | `INTEGER` | Primary key for the table. |
| `series` | `TEXT` | Series name. |
| `series_name_norm` | `TEXT` | Normalised series name for dedupe/matching. |
| `series_sort` | `TEXT` | Sort form of the series name. |
| `series_phash` | `TEXT` | Phonetic/hash helper for fuzzy series matching. |
| `series_over_author` | `INTEGER` | Author-over-series sort/display helper carried over from Calibre-style data. |
| `series_parent_id` | `INTEGER` | Parent series in a nested series hierarchy. |
| `series_parent_position` | `INTEGER` | Position within the parent series. |
| `series_tree_id` | `TEXT` | Tree identifier when multiple hierarchies are supported. |
| `series_full` | `TEXT` | Cached full path/name for the series. |
| `series_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `series_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `series_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `series_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `series_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

## Storage tables
### `stores`

- `store_failure_domain`: groups stores that fail together; replication should spread copies across different failure domains when required.
- `store_region`: broad geographic or logical region used for placement policy.
- `store_tags_json`: free-form tags used by policy resolution to prefer or exclude stores.
- `store_default_replication_policy_id`: default live-copy policy for assets placed here when no more specific override exists.
- `store_default_backup_policy_id`: default backup/archive policy for assets placed here when no more specific override exists.

Logical storage backends such as filesystems, NAS, tape, HTTP, or rclone targets.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/1-storages.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `store_id` | `INTEGER` | Primary key for the table. |
| `store_uuid` | `TEXT` | Stable runtime Store identity used across registration and recovery. |
| `store_host_uuid` | `TEXT` | Optional host identity associated with the Store. |
| `store_device_uuid` | `TEXT` | Optional device identity associated with the Store. |
| `store_name` | `TEXT` | Human-facing store name. |
| `store_kind` | `TEXT` | Backend/store implementation kind. |
| `store_access_protocol` | `TEXT` | Protocol used to access the store. |
| `store_root_uri` | `TEXT` | Base root URI/path for the store; storage keys are relative to this. |
| `store_auth_method` | `TEXT` | Authentication method used for the store. |
| `store_credentials` | `TEXT` | Credential reference/blob; should not be raw plaintext credentials. |
| `store_storage_mask` | `INTEGER` | Broad classification bitmask used in policy or placement filtering. |
| `store_policy_json` | `TEXT` | Fine-grained placement/behaviour policy blob for the store. |
| `store_failure_domain` | `TEXT` | Fault-isolation bucket used when spreading replicas across independent failure domains. |
| `store_region` | `TEXT` | Geographic or administrative placement bucket used by policy resolution. |
| `store_tags_json` | `TEXT` | Lightweight store tag set used for policy matching. |
| `store_default_replication_policy_id` | `INTEGER` | Optional default replication policy reference for placement. |
| `store_default_backup_policy_id` | `INTEGER` | Optional default backup policy reference for placement. |
| `store_supports_active_replica_mode` | `INTEGER` | Boolean declaration that the Store supports active replicas. |
| `store_supports_backup_replica_mode` | `INTEGER` | Boolean declaration that the Store supports backup replicas. |
| `store_supports_archive_replica_mode` | `INTEGER` | Boolean declaration that the Store supports archive replicas. |
| `store_operational_role` | `TEXT` | Declared operational role; see the DDL for accepted values. |
| `store_online_status` | `TEXT` | Observed administrative/online state of the store. |
| `store_location_note` | `TEXT` | Human note about the store’s physical/logical location. |
| `store_last_seen_online_timestamp_ep_k` | `INTEGER` | Last time the store was observed online. |
| `store_last_healthcheck_ok_timestamp_ep_k` | `INTEGER` | Last successful store health-check time. |
| `store_supports_folders` | `INTEGER` | Boolean capability flag: the store supports folders/directories. |
| `store_supports_hierarchical_list` | `INTEGER` | Boolean capability flag: the store can list hierarchies natively. |
| `store_supports_random_read` | `INTEGER` | Boolean capability flag: efficient random reads are supported. |
| `store_supports_random_write` | `INTEGER` | Boolean capability flag: efficient in-place/random writes are supported. |
| `store_supports_append` | `INTEGER` | Boolean capability flag: append operations are supported. |
| `store_supports_atomic_rename` | `INTEGER` | Boolean capability flag: rename/move can be atomic. |
| `store_supports_atomic_overwrite` | `INTEGER` | Boolean capability flag: overwrite can be atomic. |
| `store_supports_delete` | `INTEGER` | Boolean capability flag: delete is supported. |
| `store_is_read_only` | `INTEGER` | Boolean capability/state flag: writes should be disallowed. |
| `store_is_eventually_consistent` | `INTEGER` | Boolean flag: listing/reads may lag writes. |
| `store_supports_checksums` | `INTEGER` | Boolean capability flag: store can provide or verify checksums. |
| `store_supports_immutable_objects` | `INTEGER` | Boolean capability flag: store can keep objects immutable/WORM-like. |
| `store_supports_snapshots` | `INTEGER` | Boolean capability flag: snapshotting is supported. |
| `store_supports_server_side_encryption` | `INTEGER` | Boolean capability flag: server-side encryption is available. |
| `store_supports_parallel_read` | `INTEGER` | Boolean capability flag: parallel reads are worthwhile/supported. |
| `store_supports_parallel_write` | `INTEGER` | Boolean capability flag: parallel writes are worthwhile/supported. |
| `store_requires_mount` | `INTEGER` | Boolean flag: store must be mounted/attached before use. |
| `store_latency_class` | `TEXT` | Rough latency/temperature class such as hot, warm, cold, or glacial. |
| `store_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `store_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `store_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `store_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `store_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `folders`
Folder hierarchy inside a store, when the store supports folders.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/2-folders.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `folder_id` | `INTEGER` | Primary key for the table. |
| `folder_store_id` | `INTEGER` | Store this folder belongs to. |
| `folder_parent_id` | `INTEGER` | Parent folder in the same store. |
| `folder_name` | `TEXT` | Single path segment for the folder. |
| `folder_relpath` | `TEXT` | Cached full relative path from the store root. |
| `folder_policy_json` | `TEXT` | Folder-level policy overrides or hints. |
| `folder_default_replication_policy_id` | `INTEGER` | Optional default replication policy reference for placement. |
| `folder_default_backup_policy_id` | `INTEGER` | Optional default backup policy reference for placement. |
| `folder_last_seen_timestamp_ep_k` | `INTEGER` | Last time this folder was observed during reconciliation. |
| `folder_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |
| `folder_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `folder_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `folder_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `folder_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |

### `replication_policies`
Named declarative policies for live replica placement and healing.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/1a-storage-policies.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `replication_policy_id` | `INTEGER` | Identifier/reference field for replication policy. |
| `replication_policy_name` | `TEXT` | Stable human-facing name for the replication policy. |
| `replication_policy_name_norm` | `TEXT` | Derived identity key governed by the column normalization declaration. |
| `replication_policy_min_copies` | `INTEGER` | Hard minimum replica count below which the asset is under-replicated. |
| `replication_policy_target_copies` | `INTEGER` | Ideal steady-state replica count; NULL means use the minimum. |
| `replication_policy_distinct_by_json` | `TEXT` | Serialised list of distinctness dimensions such as store, failure_domain, or region. |
| `replication_policy_max_copies_per_bucket` | `INTEGER` | Maximum copies allowed within one distinctness bucket. |
| `replication_policy_required_store_tags_json` | `TEXT` | Store tags that must be present for a store to qualify. |
| `replication_policy_preferred_store_tags_json` | `TEXT` | Store tags that are preferred but not mandatory. |
| `replication_policy_forbidden_store_tags_json` | `TEXT` | Store tags that disqualify a store. |
| `replication_policy_required_capabilities_json` | `TEXT` | Capabilities a store must have to satisfy the policy. |
| `replication_policy_forbidden_capabilities_json` | `TEXT` | Capabilities/state markers that disqualify a store. |
| `replication_policy_synchronous_write_copies` | `INTEGER` | How many copies must be written before the write is considered complete. |
| `replication_policy_auto_heal` | `INTEGER` | Boolean flag: storage may auto-heal when under target/minimum. |
| `replication_policy_mode` | `TEXT` | Replication mode such as active, backup, or archive. |
| `replication_policy_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `replication_policy_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `replication_policy_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `backup_policies`
Named declarative policies for backup/archive copy placement and behaviour.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/1a-storage-policies.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `backup_policy_id` | `INTEGER` | Identifier/reference field for backup policy. |
| `backup_policy_name` | `TEXT` | Stable human-facing name for the backup policy. |
| `backup_policy_name_norm` | `TEXT` | Derived identity key governed by the column normalization declaration. |
| `backup_policy_min_backup_copies` | `INTEGER` | Hard minimum backup copy count. |
| `backup_policy_target_backup_copies` | `INTEGER` | Ideal steady-state backup copy count. |
| `backup_policy_distinct_by_json` | `TEXT` | Serialised distinctness dimensions for backup placement. |
| `backup_policy_max_copies_per_bucket` | `INTEGER` | Maximum copies allowed within one backup placement bucket. |
| `backup_policy_required_store_tags_json` | `TEXT` | Store tags required for backup placement. |
| `backup_policy_preferred_store_tags_json` | `TEXT` | Store tags preferred for backup placement. |
| `backup_policy_forbidden_store_tags_json` | `TEXT` | Store tags that disqualify a backup target. |
| `backup_policy_periodic_verification` | `INTEGER` | Boolean flag: backup copies should be re-verified periodically. |
| `backup_policy_retention_locked` | `INTEGER` | Boolean flag: backup copies are intended to be retention-protected/WORM-like. |
| `backup_policy_mode` | `TEXT` | Backup mode such as backup or archive. |
| `backup_policy_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `backup_policy_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `backup_policy_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `digital_assets`
Atomic byte identities. Logical multipart membership belongs to `composite_digital_assets` and its generated member links.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/3-digital-assets.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `digital_asset_id` | `INTEGER` | Primary key for the table. |
| `digital_asset_name` | `TEXT` | Human-facing name for the digital asset. |
| `digital_asset_base_name` | `TEXT` | Filename stem without extension. |
| `digital_asset_extension` | `TEXT` | Filename extension/format suffix. |
| `digital_asset_tag` | `TEXT` | Short extra tag used in naming/export patterns. |
| `digital_asset_auto_name` | `TEXT` | Auto-generated name candidate. |
| `digital_asset_use_auto_name` | `INTEGER` | Boolean flag: prefer the auto-generated name. |
| `digital_asset_mime_type` | `TEXT` | MIME/content type for the asset. |
| `digital_asset_media_category` | `TEXT` | Coarse media category for filtering/routing. |
| `digital_asset_class_mask` | `INTEGER` | Bitmask for class/policy grouping. |
| `digital_asset_visibility_mask` | `INTEGER` | Bitmask controlling visibility/export/display decisions. |
| `digital_asset_critical` | `INTEGER` | Boolean/priority-like flag: asset is operationally important to preserve. |
| `digital_asset_size_bytes` | `INTEGER` | Declared/canonical byte size for the asset, for this atomic asset. |
| `digital_asset_hash_sha256` | `TEXT` | Canonical SHA-256 hash of the asset bytes. |
| `digital_asset_hash_blake3` | `TEXT` | Canonical BLAKE3 hash of the asset bytes. |
| `digital_asset_phash` | `TEXT` | Perceptual hash where one is meaningful. |
| `digital_asset_corrupt` | `INTEGER` | Boolean flag: the asset is known or suspected corrupt. |
| `digital_asset_integrity_status` | `TEXT` | Overall integrity assessment for the managed asset. |
| `digital_asset_last_seen_timestamp_ep_k` | `INTEGER` | Last time any replica/scan confirmed the asset exists. |
| `digital_asset_last_integrity_check_timestamp_ep_k` | `INTEGER` | Last integrity verification time for the asset. |
| `digital_asset_acquired_timestamp_ep_k` | `INTEGER` | When the asset was acquired/ingested into management. |
| `digital_asset_source` | `TEXT` | Source channel/system for the asset. |
| `digital_asset_original_name` | `TEXT` | Original source filename/name. |
| `digital_asset_original_path` | `TEXT` | Original source path/URI. |
| `digital_asset_replication_policy_id` | `INTEGER` | Assigned replication policy overriding broader defaults. |
| `digital_asset_backup_policy_id` | `INTEGER` | Assigned backup policy overriding broader defaults. |
| `digital_asset_conversion_settings` | `TEXT` | Serialised conversion/processing settings associated with the asset. |
| `digital_asset_processed` | `INTEGER` | Boolean flag: downstream processing has completed or been attempted. |
| `digital_asset_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `digital_asset_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `digital_asset_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `digital_asset_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `digital_asset_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `asset_replicas`

- `asset_replica_mode`: what kind of copy this is. Expected values include `active`, `backup`, `archive`, `cache`, `transient`, and `unmanaged`.
Observed physical copies of digital assets on particular stores.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/3-digital-assets.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `asset_replica_id` | `INTEGER` | Primary key for the table. |
| `asset_replica_digital_asset_id` | `INTEGER` | Digital asset this replica realises physically. |
| `asset_replica_store_id` | `INTEGER` | Store containing the replica. |
| `asset_replica_folder_id` | `INTEGER` | Folder containing the replica when folder tracking is used. |
| `asset_replica_storage_key` | `TEXT` | Store-relative canonical locator/key for the replica. |
| `asset_replica_mode` | `TEXT` | Copy mode: active, backup, archive, cache, transient or unmanaged. |
| `asset_replica_name` | `TEXT` | Replica filename/display name. |
| `asset_replica_base_name` | `TEXT` | Replica filename stem. |
| `asset_replica_extension` | `TEXT` | Replica filename extension. |
| `asset_replica_presence_status` | `TEXT` | Observed presence state such as present, missing, tombstoned, etc. |
| `asset_replica_integrity_status` | `TEXT` | Observed integrity state for this specific physical copy. |
| `asset_replica_last_seen_timestamp_ep_k` | `INTEGER` | Last time this replica was observed/listed/read. |
| `asset_replica_last_integrity_check_timestamp_ep_k` | `INTEGER` | Last time this replica was integrity-checked. |
| `asset_replica_observed_size_bytes` | `INTEGER` | Observed on-store size for the replica. |
| `asset_replica_observed_hash_sha256` | `TEXT` | Observed SHA-256 for the specific replica copy. |
| `asset_replica_observed_hash_blake3` | `TEXT` | Observed BLAKE3 for the specific replica copy. |
| `asset_replica_failure_reason` | `TEXT` | Last known failure or mismatch reason for the replica. |
| `asset_replica_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `asset_replica_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `asset_replica_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `asset_replica_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `asset_replica_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `images`
Image storage table retained separately from digital_assets for now.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/5-images.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `image_id` | `INTEGER` | Primary key for the table. |
| `image_item_id` | `INTEGER` | Item the image is primarily attached to. |
| `image_store_id` | `INTEGER` | Store containing the image. |
| `image_folder_id` | `INTEGER` | Folder containing the image when folder tracking is used. |
| `image_storage_key` | `TEXT` | Store-relative canonical locator/key for the image. |
| `image_name` | `TEXT` | Human-facing image name. |
| `image_base_name` | `TEXT` | Image filename stem. |
| `image_extension` | `TEXT` | Image filename extension. |
| `image_tag` | `TEXT` | Short extra tag used in image naming/export patterns. |
| `image_auto_name` | `TEXT` | Auto-generated image name candidate. |
| `image_use_auto_name` | `INTEGER` | Boolean flag: prefer the auto-generated name. |
| `image_mime_type` | `TEXT` | Image MIME/content type. |
| `image_role` | `TEXT` | Semantic role of the image such as cover or author image. |
| `image_media_category` | `TEXT` | Coarse image/media category. |
| `image_class_mask` | `INTEGER` | Bitmask for class/policy grouping. |
| `image_visibility_mask` | `INTEGER` | Bitmask controlling visibility/export/display decisions. |
| `image_critical` | `INTEGER` | Boolean/priority-like flag: image is operationally important to preserve. |
| `image_size_bytes` | `INTEGER` | Canonical/declared size of the image bytes. |
| `image_hash_sha256` | `TEXT` | Canonical SHA-256 hash of the image bytes. |
| `image_hash_blake3` | `TEXT` | Canonical BLAKE3 hash of the image bytes. |
| `image_phash` | `TEXT` | Perceptual hash for near-duplicate image matching. |
| `image_corrupt` | `INTEGER` | Boolean flag: image is known or suspected corrupt. |
| `image_integrity_status` | `TEXT` | Overall integrity assessment for the image. |
| `image_last_seen_timestamp_ep_k` | `INTEGER` | Last time the image was observed on storage. |
| `image_last_integrity_check_timestamp_ep_k` | `INTEGER` | Last time the image was integrity-checked. |
| `image_acquired_timestamp_ep_k` | `INTEGER` | When the image was acquired/ingested. |
| `image_source` | `TEXT` | Source channel/system for the image. |
| `image_original_name` | `TEXT` | Original source filename/name. |
| `image_original_path` | `TEXT` | Original source path/URI. |
| `image_anthology` | `INTEGER` | Legacy grouping/assembly marker for images. |
| `image_parent` | `TEXT` | Legacy parent/reference field for image lineage/grouping. |
| `image_conversion_settings` | `TEXT` | Serialised conversion/processing settings associated with the image. |
| `image_processed` | `INTEGER` | Boolean flag: image processing has completed or been attempted. |
| `image_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `image_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `image_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `image_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `image_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `devices`
Device registry for reading/import/export contexts.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/4-devices.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `device_id` | `INTEGER` | Primary key for the table. |
| `device_type` | `TEXT` | Device kind/model/category. |
| `device_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `device_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `device_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `device_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `device_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

## Workflow and transform tables
### `workflow_states`
Reusable workflow-state vocabulary.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/workflow_tables/workflow_state.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `workflow_state_id` | `INTEGER` | Primary key for the table. |
| `workflow_state_code` | `TEXT` | Stable code used by software to refer to the workflow state. |
| `workflow_state_label` | `TEXT` | Human-facing label for the workflow state. |
| `workflow_state_description` | `TEXT` | Longer explanation of the workflow state. |
| `workflow_state_is_terminal` | `INTEGER` | Boolean flag: no further progress is expected from this state. |
| `workflow_state_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `workflow_state_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `workflow_state_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `workflow_state_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |

### `workflow_steps`
Reusable workflow-step definitions.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/workflow_tables/workflow_steps.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `workflow_step_id` | `INTEGER` | Primary key for the table. |
| `workflow_step_code` | `TEXT` | Stable code used by software to refer to the workflow step. |
| `workflow_step_label` | `TEXT` | Human-facing label for the workflow step. |
| `workflow_step_group` | `TEXT` | Optional grouping bucket such as ingest, quality, derivation, or export. |
| `workflow_step_is_required` | `INTEGER` | Boolean flag: step is required for completeness. |
| `workflow_step_is_skippable` | `INTEGER` | Boolean flag: step may be explicitly skipped. |
| `workflow_step_ord` | `INTEGER` | UI/processing ordering hint. |
| `workflow_step_scope` | `TEXT` | Whether the step applies to items, digital assets, or both. |
| `workflow_step_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `workflow_step_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `workflow_step_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `workflow_step_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |

### `item_workflow`
Current per-item workflow/materialised state rows.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/workflow_tables/item_workflow.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `item_workflow_id` | `INTEGER` | Primary key for the table. |
| `item_workflow_item_id` | `INTEGER` | Item whose current workflow state is being tracked. |
| `item_workflow_step_id` | `INTEGER` | Workflow step this current-state row refers to. |
| `item_workflow_status` | `TEXT` | Current status for the item/step pair. |
| `item_workflow_priority` | `INTEGER` | Priority used when scheduling or surfacing work. |
| `item_workflow_assigned_to` | `TEXT` | Who/what currently owns the work. |
| `item_workflow_reason` | `TEXT` | Reason for blocked/failed/skipped state. |
| `item_workflow_progress` | `REAL` | Approximate 0..1 progress value. |
| `item_workflow_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `item_workflow_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `item_workflow_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `item_workflow_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `item_workflow_started_timestamp_ep_k` | `INTEGER` | Start timestamp in epoch milliseconds. |
| `item_workflow_finished_timestamp_ep_k` | `INTEGER` | Finish/completion timestamp in epoch milliseconds. |
| `item_workflow_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `item_workflow_events`
Audit/event log for item workflow changes.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/workflow_tables/item_workflow_events.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `item_workflow_event_id` | `INTEGER` | Primary key for the table. |
| `item_workflow_event_item_id` | `INTEGER` | Item whose workflow event is being logged. |
| `item_workflow_event_step_id` | `INTEGER` | Workflow step the event relates to. |
| `item_workflow_event_from_status` | `TEXT` | Previous status before the event. |
| `item_workflow_event_to_status` | `TEXT` | New status after the event. |
| `item_workflow_event_actor` | `TEXT` | User/tool/worker that caused the transition. |
| `item_workflow_event_note` | `TEXT` | Free-form event note. |
| `item_workflow_event_tool` | `TEXT` | Tool/process name associated with the event. |
| `item_workflow_event_run_id` | `TEXT` | Batch/job correlation identifier for the event. |
| `item_workflow_event_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `item_workflow_event_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `item_workflow_event_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `item_workflow_event_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `item_workflow_event_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `digital_asset_workflow`
Current per-digital-asset workflow/materialised state rows.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/workflow_tables/file_workflow.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `digital_asset_workflow_id` | `INTEGER` | Primary key for the table. |
| `digital_asset_workflow_digital_asset_id` | `INTEGER` | Digital asset whose current workflow state is being tracked. |
| `digital_asset_workflow_step_id` | `INTEGER` | Workflow step this current-state row refers to. |
| `digital_asset_workflow_status` | `TEXT` | Current status for the digital-asset/step pair. |
| `digital_asset_workflow_priority` | `INTEGER` | Priority used when scheduling or surfacing work. |
| `digital_asset_workflow_assigned_to` | `TEXT` | Who/what currently owns the work. |
| `digital_asset_workflow_reason` | `TEXT` | Reason for blocked/failed/skipped state. |
| `digital_asset_workflow_progress` | `REAL` | Approximate 0..1 progress value. |
| `digital_asset_workflow_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `digital_asset_workflow_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `digital_asset_workflow_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `digital_asset_workflow_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `digital_asset_workflow_started_timestamp_ep_k` | `INTEGER` | Start timestamp in epoch milliseconds. |
| `digital_asset_workflow_finished_timestamp_ep_k` | `INTEGER` | Finish/completion timestamp in epoch milliseconds. |
| `digital_asset_workflow_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `digital_asset_workflow_events`
Audit/event log for digital-asset workflow changes.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/workflow_tables/file_workflow_events.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `digital_asset_workflow_event_id` | `INTEGER` | Primary key for the table. |
| `digital_asset_workflow_event_digital_asset_id` | `INTEGER` | Digital asset whose workflow event is being logged. |
| `digital_asset_workflow_event_step_id` | `INTEGER` | Workflow step the event relates to. |
| `digital_asset_workflow_event_from_status` | `TEXT` | Previous status before the event. |
| `digital_asset_workflow_event_to_status` | `TEXT` | New status after the event. |
| `digital_asset_workflow_event_actor` | `TEXT` | User/tool/worker that caused the transition. |
| `digital_asset_workflow_event_note` | `TEXT` | Free-form event note. |
| `digital_asset_workflow_event_tool` | `TEXT` | Tool/process name associated with the event. |
| `digital_asset_workflow_event_run_id` | `TEXT` | Batch/job correlation identifier for the event. |
| `digital_asset_workflow_event_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `digital_asset_workflow_event_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `digital_asset_workflow_event_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `digital_asset_workflow_event_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `digital_asset_workflow_event_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `transform_runs`
Execution records for transforms such as OCR, convert, hash, dedupe, or repair.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/workflow_tables/transform_run.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `transform_run_id` | `INTEGER` | Primary key for the table. |
| `transform_run_type` | `TEXT` | Transform kind such as OCR, convert, thumbnail, hash, dedupe, repair, or extract. |
| `transform_run_tool` | `TEXT` | Tool/executable and usually version used for the run. |
| `transform_run_profile` | `TEXT` | Named preset/profile used for the run. |
| `transform_run_params` | `TEXT` | Deterministic serialised parameter blob for the run. |
| `transform_run_params_hash` | `TEXT` | Hash of the parameters for caching/replay detection. |
| `transform_run_actor` | `TEXT` | User, service, or automation that initiated the run. |
| `transform_run_status` | `TEXT` | Outcome/status of the run. |
| `transform_run_error` | `TEXT` | Error text if the run failed or aborted. |
| `transform_run_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `transform_run_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `transform_run_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `transform_run_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `transform_run_started_timestamp_ep_k` | `INTEGER` | Start timestamp in epoch milliseconds. |
| `transform_run_finished_timestamp_ep_k` | `INTEGER` | Finish/completion timestamp in epoch milliseconds. |
| `transform_run_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `transform_run_inputs`
Links transform runs to their input digital assets.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/workflow_tables/transform_run_inputs.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `transform_run_input_id` | `INTEGER` | Primary key for the table. |
| `transform_run_input_run_id` | `INTEGER` | Transform run this input belongs to. |
| `transform_run_input_digital_asset_id` | `INTEGER` | Digital asset used as an input. |
| `transform_run_input_role` | `TEXT` | Role of the input within the run. |
| `transform_run_input_note` | `TEXT` | Free-form note about the input. |
| `transform_run_input_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `transform_run_input_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `transform_run_input_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `transform_run_input_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `transform_run_input_started_timestamp_ep_k` | `INTEGER` | Start timestamp in epoch milliseconds. |
| `transform_run_input_finished_timestamp_ep_k` | `INTEGER` | Finish/completion timestamp in epoch milliseconds. |

### `transform_run_outputs`
Links transform runs to their output digital assets.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/workflow_tables/transform_run_outputs.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `transform_run_output_id` | `INTEGER` | Primary key for the table. |
| `transform_run_output_run_id` | `INTEGER` | Transform run this output belongs to. |
| `transform_run_output_digital_asset_id` | `INTEGER` | Digital asset produced as an output. |
| `transform_run_output_role` | `TEXT` | Role of the output within the run. |
| `transform_run_output_note` | `TEXT` | Free-form note about the output. |
| `transform_run_output_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `transform_run_output_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `transform_run_output_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `transform_run_output_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `transform_run_output_started_timestamp_ep_k` | `INTEGER` | Start timestamp in epoch milliseconds. |
| `transform_run_output_finished_timestamp_ep_k` | `INTEGER` | Finish/completion timestamp in epoch milliseconds. |

### `digital_asset_derivations`
Explicit parent/child derivation edges between digital assets.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/workflow_tables/file_lineage.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `digital_asset_derivation_id` | `INTEGER` | Primary key for the table. |
| `digital_asset_derivation_parent_digital_asset_id` | `INTEGER` | Source/parent digital asset in the derivation edge. |
| `digital_asset_derivation_child_digital_asset_id` | `INTEGER` | Derived/child digital asset in the derivation edge. |
| `digital_asset_derivation_run_id` | `INTEGER` | Transform run that produced the derivation, when known. |
| `digital_asset_derivation_kind` | `TEXT` | Derivation kind such as converted, OCR text, thumbnail, repacked, repaired, etc. |
| `digital_asset_derivation_note` | `TEXT` | Free-form note about the derivation edge. |
| `digital_asset_derivation_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `digital_asset_derivation_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `digital_asset_derivation_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `digital_asset_derivation_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `digital_asset_derivation_started_timestamp_ep_k` | `INTEGER` | Start timestamp in epoch milliseconds. |
| `digital_asset_derivation_finished_timestamp_ep_k` | `INTEGER` | Finish/completion timestamp in epoch milliseconds. |
| `digital_asset_derivation_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

## Plugin data tables
### `works_plugin_data`
Arbitrary plugin key/value storage attached to works.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/frbr_plugin_data.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `works_plugin_data_id` | `INTEGER` | Primary key for the table. |
| `works_plugin_data_work` | `INTEGER` | Work this plugin-data row attaches to. |
| `works_plugin_data_name` | `TEXT` | Plugin-defined key/name. |
| `works_plugin_data_val` | `TEXT` | Plugin-defined value. |
| `works_plugin_data_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `works_plugin_data_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `works_plugin_data_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `works_plugin_data_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `works_plugin_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `expressions_plugin_data`
Arbitrary plugin key/value storage attached to expressions.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/frbr_plugin_data.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `expressions_plugin_data_id` | `INTEGER` | Primary key for the table. |
| `expressions_plugin_data_expressions` | `INTEGER` | Expression this plugin-data row attaches to. |
| `expressions_plugin_data_name` | `TEXT` | Plugin-defined key/name. |
| `expressions_plugin_data_val` | `TEXT` | Plugin-defined value. |
| `expressions_plugin_data_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `expressions_plugin_data_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `expressions_plugin_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `manifestations_plugin_data`
Arbitrary plugin key/value storage attached to manifestations.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/frbr_plugin_data.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `manifestations_plugin_data_id` | `INTEGER` | Primary key for the table. |
| `manifestations_plugin_data_manifestations` | `INTEGER` | Manifestation this plugin-data row attaches to. |
| `manifestations_plugin_data_name` | `TEXT` | Plugin-defined key/name. |
| `manifestations_plugin_data_val` | `TEXT` | Plugin-defined value. |
| `manifestations_plugin_data_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `manifestations_plugin_data_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `manifestations_plugin_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `items_plugin_data`
Arbitrary plugin key/value storage attached to items.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/frbr_plugin_data.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `items_plugin_data_id` | `INTEGER` | Primary key for the table. |
| `items_plugin_data_items` | `INTEGER` | Item this plugin-data row attaches to. |
| `items_plugin_data_name` | `TEXT` | Plugin-defined key/name. |
| `items_plugin_data_val` | `TEXT` | Plugin-defined value. |
| `items_plugin_data_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `items_plugin_data_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `items_plugin_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

## Additional current explicit tables

### `backup_presence_links`
Recorded relationship between a packed backup and its source. Protection fields record intent; they do not guard every byte deletion.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/1c-backup-presence-links.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `backup_presence_link_id` | `INTEGER` | Primary key. |
| `backup_presence_link_backup_store_id` | `INTEGER` | Backup store id. |
| `backup_presence_link_workflow_id` | `INTEGER` | Workflow id. |
| `backup_presence_link_source_identifier` | `TEXT` | Source identifier. |
| `backup_presence_link_source_kind` | `TEXT` | Source kind. |
| `backup_presence_link_source_file_id` | `INTEGER` | Source file id. |
| `backup_presence_link_source_asset_replica_id` | `INTEGER` | Source asset replica id. |
| `backup_presence_link_source_store_id` | `INTEGER` | Source store id. |
| `backup_presence_link_archive_path` | `TEXT` | Archive path. |
| `backup_presence_link_type` | `TEXT` | Presence relationship label; defaults to packed_presence. |
| `backup_presence_link_source` | `TEXT` | Source. |
| `backup_presence_link_output_url` | `TEXT` | Output url. |
| `backup_presence_link_is_protected` | `INTEGER` | Recorded protection intent; not proof of universal deletion enforcement. |
| `backup_presence_link_is_immutable` | `INTEGER` | Recorded immutability intent; not proof of backend enforcement. |
| `backup_presence_link_created_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup presence link created. |
| `backup_presence_link_modified_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup presence link modified. |
| `backup_presence_link_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `backup_workflow_outputs`
References to backup outputs and their recorded verification results.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/1b-backup-workflows.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `backup_workflow_output_id` | `INTEGER` | Primary key. |
| `backup_workflow_output_workflow_id` | `INTEGER` | Workflow id. |
| `backup_workflow_output_url` | `TEXT` | Url. |
| `backup_workflow_output_digital_asset_id` | `INTEGER` | Digital asset id. |
| `backup_workflow_output_asset_replica_id` | `INTEGER` | Asset replica id. |
| `backup_workflow_output_store_id` | `INTEGER` | Store id. |
| `backup_workflow_output_verified_ok` | `INTEGER` | Optional boolean recording output verification outcome. |
| `backup_workflow_output_created_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow output created. |
| `backup_workflow_output_modified_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow output modified. |
| `backup_workflow_output_source_created_datestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow output source created. |
| `backup_workflow_output_source_modified_datestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow output source modified. |
| `backup_workflow_output_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `backup_workflow_sources`
Ordered backup input descriptors, expected integrity observations and archive-relative destinations.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/1b-backup-workflows.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `backup_workflow_source_id` | `INTEGER` | Primary key. |
| `backup_workflow_source_workflow_id` | `INTEGER` | Workflow id. |
| `backup_workflow_source_ordinal` | `INTEGER` | Ordinal. |
| `backup_workflow_source_kind` | `TEXT` | Kind. |
| `backup_workflow_source_identifier` | `TEXT` | Identifier. |
| `backup_workflow_source_archive_path` | `TEXT` | Archive path. |
| `backup_workflow_source_expected_size` | `INTEGER` | Expected size. |
| `backup_workflow_source_expected_hash` | `TEXT` | Expected hash. |
| `backup_workflow_source_file_id` | `INTEGER` | File id. |
| `backup_workflow_source_asset_replica_id` | `INTEGER` | Asset replica id. |
| `backup_workflow_source_store_id` | `INTEGER` | Store id. |
| `backup_workflow_source_created_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow source created. |
| `backup_workflow_source_modified_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow source modified. |
| `backup_workflow_source_source_created_datestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow source source created. |
| `backup_workflow_source_source_modified_datestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow source source modified. |
| `backup_workflow_source_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `backup_workflow_state`
Per-workflow checkpoint used to resume staging/build work. This is not the universal job ledger.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/1b-backup-workflows.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `backup_workflow_state_id` | `INTEGER` | Primary key. |
| `backup_workflow_state_workflow_id` | `INTEGER` | Workflow id. |
| `backup_workflow_state_status` | `TEXT` | Status. Default: `'draft'`. |
| `backup_workflow_state_next_source_index` | `INTEGER` | Next source index. Default: `0`. |
| `backup_workflow_state_staged_source_count` | `INTEGER` | Staged source count. Default: `0`. |
| `backup_workflow_state_completed_steps_json` | `TEXT` | Serialized completed workflow step identifiers. |
| `backup_workflow_state_source_results_json` | `TEXT` | Serialized per-source results used during resume. |
| `backup_workflow_state_output_artifact_url` | `TEXT` | Output artifact url. |
| `backup_workflow_state_last_error` | `TEXT` | Last error. |
| `backup_workflow_state_created_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow state created. |
| `backup_workflow_state_modified_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow state modified. |
| `backup_workflow_state_source_created_datestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow state source created. |
| `backup_workflow_state_source_modified_datestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow state source modified. |
| `backup_workflow_state_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `backup_workflows`
Persisted backup definition and top-level status. Execution and recovery are owned by storage backup workflows.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/1b-backup-workflows.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `backup_workflow_id` | `INTEGER` | Primary key. |
| `backup_workflow_name` | `TEXT` | Name. |
| `backup_workflow_kind` | `TEXT` | Kind. |
| `backup_workflow_destination_store_id` | `INTEGER` | Destination store id. |
| `backup_workflow_staging_store_id` | `INTEGER` | Staging store id. |
| `backup_workflow_output_url` | `TEXT` | Url. |
| `backup_workflow_verify_after_build` | `INTEGER` | Verify after build. Default: `1`. |
| `backup_workflow_cleanup_staging_after_success` | `INTEGER` | Cleanup staging after success. Default: `0`. |
| `backup_workflow_staging_root` | `TEXT` | Staging root. |
| `backup_workflow_options_json` | `TEXT` | Options json. |
| `backup_workflow_status` | `TEXT` | Status. Default: `'draft'`. |
| `backup_workflow_last_error` | `TEXT` | Last error. |
| `backup_workflow_created_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow created. |
| `backup_workflow_modified_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow modified. |
| `backup_workflow_source_created_datestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow source created. |
| `backup_workflow_source_modified_datestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for backup workflow source modified. |
| `backup_workflow_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `column_metadata`
Per-column policy declarations. See [column metadata](column-metadata.md); declaring a validation profile does not validate existing data.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/column_metadata.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `column_metadata_id` | `INTEGER` | Primary key. |
| `column_metadata_table_name` | `TEXT` | Table name. |
| `column_metadata_column_name` | `TEXT` | Column name. |
| `column_metadata_case_sensitive` | `INTEGER` | Case sensitive. Default: `1`. |
| `column_metadata_semantic_role` | `TEXT` | Semantic role. Default: `'machine_value'`. |
| `column_metadata_normalization_profile` | `TEXT` | Normalization profile. Default: `'none'`. |
| `column_metadata_comparison_column` | `TEXT` | Comparison column. |
| `column_metadata_empty_value_policy` | `TEXT` | Empty value policy. Default: `'null_is_missing'`. |
| `column_metadata_merge_policy` | `TEXT` | Merge policy. Default: `'replace'`. |
| `column_metadata_validation_profile` | `TEXT` | Validation profile. Default: `'none'`. |
| `column_metadata_formatting_options_json` | `TEXT` | Formatting options json. Default: `'{}'`. |
| `column_metadata_display_options_json` | `TEXT` | Display options json. Default: `'{}'`. |
| `column_metadata_created_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for column metadata created. |
| `column_metadata_modified_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for column metadata modified. |
| `column_metadata_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `composite_digital_assets`
Logical multipart payloads. Generated member links carry sequence, role and required-member data; replicas belong to the atomic members.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/3-digital-assets.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `composite_digital_asset_id` | `INTEGER` | Primary key. |
| `composite_digital_asset_name` | `TEXT` | Name. |
| `composite_digital_asset_media_type` | `TEXT` | Media type. |
| `composite_digital_asset_media_category` | `TEXT` | Media category. |
| `composite_digital_asset_source` | `TEXT` | Source. |
| `composite_digital_asset_replication_policy_id` | `INTEGER` | Replication policy id. |
| `composite_digital_asset_backup_policy_id` | `INTEGER` | Backup policy id. |
| `composite_digital_asset_created_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for composite digital asset created. |
| `composite_digital_asset_modified_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for composite digital asset modified. |
| `composite_digital_asset_source_created_datestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for composite digital asset source created. |
| `composite_digital_asset_source_modified_datestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for composite digital asset source modified. |
| `composite_digital_asset_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `file_derivations`
Compatibility parent/child relationships between legacy file rows; not a complete reproducible transformation receipt.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/workflow_tables/5a-legacy-file-derivations.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `file_derivation_id` | `INTEGER` | Primary key for the table. |
| `file_derivation_parent_file_id` | `INTEGER` | Parent file id. |
| `file_derivation_child_file_id` | `INTEGER` | Child file id. |
| `file_derivation_kind` | `TEXT` | Derivation kind such as converted, OCR text, thumbnail, repacked, repaired, etc. |
| `file_derivation_note` | `TEXT` | Free-form note about the derivation edge. |
| `file_derivation_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `file_derivation_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `file_derivation_started_timestamp_ep_k` | `INTEGER` | Start timestamp in epoch milliseconds. |
| `file_derivation_finished_timestamp_ep_k` | `INTEGER` | Finish/completion timestamp in epoch milliseconds. |
| `file_derivation_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `files`
Compatibility file records still used by live adapters. They combine locators and payload hints; atomic assets and replicas are the current storage model.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/storage_tables/3a-legacy-files.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `file_id` | `INTEGER` | Primary key for the table. |
| `file_item_id` | `INTEGER` | Item id. |
| `file_store_id` | `INTEGER` | Store id. |
| `file_folder_id` | `INTEGER` | Folder id. |
| `file_storage_key` | `TEXT` | Storage key. |
| `file_name` | `TEXT` | Human-facing name for the digital legacy file. |
| `file_base_name` | `TEXT` | Filename stem without extension. |
| `file_extension` | `TEXT` | Filename extension/format suffix. |
| `file_tag` | `TEXT` | Short extra tag used in naming/export patterns. |
| `file_auto_name` | `TEXT` | Auto-generated name candidate. |
| `file_use_auto_name` | `INTEGER` | Boolean flag: prefer the auto-generated name. |
| `file_mime_type` | `TEXT` | MIME/content type for the legacy file. |
| `file_role` | `TEXT` | Role. |
| `file_media_category` | `TEXT` | Coarse media category for filtering/routing. |
| `file_class_mask` | `INTEGER` | Bitmask for class/policy grouping. |
| `file_visibility_mask` | `INTEGER` | Bitmask controlling visibility/export/display decisions. |
| `file_critical` | `INTEGER` | Boolean/priority-like flag: legacy file is operationally important to preserve. |
| `file_size_bytes` | `INTEGER` | Declared/canonical byte size for the legacy file, for this atomic legacy file. |
| `file_hash_sha256` | `TEXT` | Canonical SHA-256 hash of the legacy file bytes. |
| `file_hash_blake3` | `TEXT` | Canonical BLAKE3 hash of the legacy file bytes. |
| `file_phash` | `TEXT` | Perceptual hash where one is meaningful. |
| `file_corrupt` | `INTEGER` | Boolean flag: the legacy file is known or suspected corrupt. |
| `file_integrity_status` | `TEXT` | Overall integrity assessment for the managed legacy file. |
| `file_last_seen_timestamp_ep_k` | `INTEGER` | Last time any replica/scan confirmed the legacy file exists. |
| `file_last_integrity_check_timestamp_ep_k` | `INTEGER` | Last integrity verification time for the legacy file. |
| `file_acquired_timestamp_ep_k` | `INTEGER` | When the legacy file was acquired/ingested into management. |
| `file_source` | `TEXT` | Source channel/system for the legacy file. |
| `file_original_name` | `TEXT` | Original source filename/name. |
| `file_original_path` | `TEXT` | Original source path/URI. |
| `file_anthology` | `INTEGER` | Anthology. |
| `file_parent` | `TEXT` | Parent. |
| `file_conversion_settings` | `TEXT` | Serialised conversion/processing settings associated with the legacy file. |
| `file_processed` | `INTEGER` | Boolean flag: downstream processing has completed or been attempted. |
| `file_created_timestamp_ep_k` | `INTEGER` | Row creation timestamp in epoch milliseconds. |
| `file_modified_timestamp_ep_k` | `INTEGER` | Row last-modified timestamp in epoch milliseconds. |
| `file_source_created_datestamp_ep_k` | `INTEGER` | Best-known creation timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `file_source_modified_datestamp_ep_k` | `INTEGER` | Best-known modification timestamp from the upstream source system/artifact, in epoch milliseconds. |
| `file_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |

### `normalized_identities`
Declarations connecting display values to normalized keys and uniqueness scopes.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/normalized_identities.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `normalized_identity_table_name` | `TEXT` | Table containing the value and key columns. |
| `normalized_identity_value_column` | `TEXT` | Physical column retaining the canonical display value. |
| `normalized_identity_key_column` | `TEXT` | Physical column storing the derived comparison key. |
| `normalized_identity_normalization_profile` | `TEXT` | Canonical normalizer used to derive the identity key. |
| `normalized_identity_scope_columns_json` | `TEXT` | JSON list of columns that scope the identity's uniqueness. |
| `normalized_identity_unique` | `INTEGER` | Whether this declaration requires uniqueness. |

### `storage_ingest_operations`
Durable ingest journal for publication and database-finalization recovery. Its required scratch column contains recovery data.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/workflow_tables/storage_ingest_operations.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `storage_ingest_operation_id` | `INTEGER` | Primary key. |
| `storage_ingest_operation_uuid` | `TEXT` | Uuid. |
| `storage_ingest_operation_state` | `TEXT` | State. Default: `'started'`. |
| `storage_ingest_operation_store_uuid` | `TEXT` | Store uuid. |
| `storage_ingest_operation_storage_key` | `TEXT` | Storage key. |
| `storage_ingest_operation_digital_asset_id` | `INTEGER` | Digital asset id. |
| `storage_ingest_operation_asset_replica_id` | `INTEGER` | Asset replica id. |
| `storage_ingest_operation_last_error` | `TEXT` | Last error. |
| `storage_ingest_operation_created_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for storage ingest operation created. |
| `storage_ingest_operation_modified_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for storage ingest operation modified. |
| `storage_ingest_operation_scratch` | `TEXT` | Required serialized journal payload used for ingest recovery; not disposable scratch. |

### `storage_schema_migrations`
Applied storage migration IDs, versions and optional details.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/workflow_tables/storage_schema_migrations.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `storage_schema_migration_id` | `TEXT` | Primary key. |
| `storage_schema_migration_version` | `INTEGER` | Version. |
| `storage_schema_migration_applied_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for storage schema migration applied. |
| `storage_schema_migration_details_json` | `TEXT` | Details json. |

### `tags`
Canonical tag values and comparison keys, related to bibliographic entities through generated links.

[DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/metadata_additional/tags.sql).

| Field | Type | Purpose |
| --- | --- | --- |
| `tag_id` | `INTEGER` | Primary key. |
| `tag` | `TEXT` | Canonical display spelling of the tag. |
| `tag_phash` | `TEXT` | Normalized tag comparison key; historical name, not a perceptual image hash. |
| `tag_description` | `TEXT` | Optional descriptive text. |
| `tag_scratch` | `TEXT` | Owner-defined auxiliary state; check the consuming workflow before clearing it. |
| `tag_created_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for tag created. |
| `tag_modified_timestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for tag modified. |
| `tag_source_created_datestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for tag source created. |
| `tag_source_modified_datestamp_ep_k` | `INTEGER` | Epoch-millisecond timestamp for tag source modified. |

## Auto-generated interlink and intralink tables
The FRBR generator also creates many relationship tables from `interlink_table_requests.toml` and `intralink_table_requests.toml`. Rather than list every generated instance here, this section documents the **standard columns and what they mean**.
Physical relation columns carry the link prefix: for example, `expression_work_link_work_id`, `expression_work_link_expression_id` and `expression_work_link_primary`. The short suffixes below explain roles, not literal unprefixed SQL column names. Optional columns differ by the TOML request; query [link capabilities](link-capabilities.md) or inspect the generated table.

### Standard interlink table fields
| Field pattern | Purpose |
|---|---|
| `<left>_<right>_link_id` | Primary key for the generated link table. |
| `<left>_id` / `<right>_id` | Foreign keys to the linked rows. Depending on TOML, these may be nullable during placeholder-row workflows. |
| `priority` | Ordering or preference value for multi-link relationships. |
| `primary` | 0/1 flag marking the primary/preferred linked row. |
| `type` | Semantic role of the link (for example `primary_payload`, `cover`, `translation`, etc.). |
| `origin` | Where the link came from: user, import, inference, plugin, etc. |
| `policy` | Policy blob/text attached to the specific relationship. |
| `data` | Free-form relationship payload for cases that need extra detail. |
| `index` | Extra indexing/ordering token when requested by the spec. |

### Standard intralink/self-link table fields
| Field pattern | Purpose |
|---|---|
| `<table>_intralink_id` | Primary key for the self-link row. |
| `<table>_primary_id` | Source/primary row in the self-link. |
| `<table>_secondary_id` | Target/secondary row in the self-link. |
| `type` | Kind of relationship (for example `converted_from`, `compressed_version`, `backup`, etc.). |
| `origin` | Where the relation came from. |
| `data` | Free-form payload attached to the relation. |
| `priority` / `primary` | Optional generated metadata when requested by the spec. |

### Particularly important generated storage-facing links
- `digital_assets` ↔ `items`: currently requested with `priority`, `type`, `origin`, and `primary`. This is where item-level bibliographic identity meets storage-managed payloads.
- `digital_assets` ↔ `images`: currently requested with `priority` and `type`; useful for cover/contained/author-image assignment.
- `digital_assets` intralinks: currently requested with `type`, `data`, and `origin`; these are for **derivation-style** relationships, not multipart composition.
- Multipart membership lives in `composite_digital_asset_digital_asset_links`, with `sequence_number` and `is_required`; it is distinct from derivation.
