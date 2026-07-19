# Column metadata policy

LiuXin stores one complete policy row for every physical table column in
`column_metadata`. The policy controls equality, validation, empty-value
handling, and merge behavior in database writers; it never changes,
lowercases, title-cases, or otherwise rewrites stored display text.

## Decision rule

Human-facing display columns fall into two groups:

- **Case-insensitive identity** — names, titles, labels, taxonomy terms, sort
  values, and human assignee/actor names. Case is presentation rather than
  identity, so `Space Opera` and `space opera` denote the same tag.
- **Case-sensitive verbatim text** — prose, annotations, descriptions, reasons,
  errors, and resource names. A case-only edit can carry meaning in prose, and
  case-distinct resource names can coexist on case-sensitive filesystems.

The display registry deliberately excludes machine-facing values: identifiers,
codes, hashes and perceptual hashes, normalized/search keys, paths and URLs,
JSON, dates, credentials, flags, scratch fields, and controlled
type/kind/status/source tokens. Those columns receive explicit, exact-comparison
policies inferred from their schema declaration and naming convention.

## Complete column policy

Each `column_metadata` row stores:

| Field | Meaning |
| --- | --- |
| `semantic_role` | The column's declared purpose. Display roles are joined by machine roles such as `identifier`, `relationship_key`, `code`, `boolean`, `number`, `ordering`, `date_time`, `locator`, `structured_data`, `hash`, `normalized_key`, `provenance`, and `scratch`. |
| `normalization_profile` | Comparison normalization: `none`, `unicode_nfc`, `unicode_nfc_trim_casefold`, `tag_search_term`, or `title_search_term`. |
| `comparison_column` | Optional derived key in the same table used for equality lookup. |
| `empty_value_policy` | `null_is_missing`, `null_or_blank_is_missing`, or `preserve`. |
| `merge_policy` | `replace`, `set_union`, `append`, or `preserve_existing`. |
| `validation_profile` | Explicit declarative validator. `none` means no specialized validator; typed profiles include `identifier`, `code`, `boolean`, `number`, `date_time`, `locator`, `json`, `hash`, `normalized_key`, and the display-text profiles. |

All registered display columns treat `NULL` and blank text as missing. Titles,
names, labels, and sort keys use Unicode NFC, trimming, and case folding.
Verbatim prose uses NFC without case folding. Resource names use no
normalization because case and filesystem spelling can be significant.

Taxonomy terms use `set_union`; `comments.comment`, `notes.note`, and
`synopses.synopsis` use `append`; other display columns use `replace`.
Validation profile names are declarative contracts for consumers and do not
themselves rewrite stored values.

## Machine-column defaults

Machine-facing columns are always case-sensitive, use no comparison
normalization, and have no derived comparison column. The generator applies
the following defaults in precedence order:

| Schema signal | Semantic role | Empty value | Merge | Validation |
| --- | --- | --- | --- | --- |
| Primary key or unique identifier/UUID name | `identifier` | `null_is_missing` | `preserve_existing` | `identifier` |
| Foreign key or other `_id` relationship | `relationship_key` | `null_is_missing` | `replace` | `identifier` |
| Timestamp, datestamp, date, or year | `date_time` | `null_is_missing` | Created/source-created values are preserved; other values replace | `date_time` |
| Boolean naming signal | `boolean` | `null_is_missing` | `replace` | `boolean` |
| Position, priority, rank, sequence, or ordinal | `ordering` | `null_is_missing` | `replace` | `number` |
| Hash or perceptual hash | `hash` | `null_is_missing` | `replace` | `hash` |
| Normalized/search key | `normalized_key` | `null_is_missing` | `replace` | `normalized_key` |
| Path, URI, URL, CFI, or storage key | `locator` | `null_is_missing` | `replace` | `locator` |
| JSON/options/settings/capabilities/credentials payload | `structured_data` | `null_is_missing` | `replace` | `json` |
| Type, kind, status, role, format, scheme, or other controlled code | `code` | `null_is_missing` | `replace` | `code` |
| Other numeric declaration | `number` | `null_is_missing` | `replace` | `number` |
| Source, origin, or provenance value | `provenance` | `null_is_missing` | `replace` | `none` |
| Scratch column | `scratch` | `preserve` | `replace` | `none` |
| No more specific signal | `machine_value` | `null_is_missing` | `replace` | `none` |

All columns in append-only event tables use `preserve_existing`. The
schema-wide seed is exhaustive: after generation, the set of
`(table_name, column_name)` catalog keys must exactly equal the set of physical
columns in managed tables. The `column_metadata` table describes its own
columns as well.

The current derived comparison keys are:

| Display column | Comparison column | Normalization |
| --- | --- | --- |
| `tags.tag` | `tags.tag_phash` | `tag_search_term` |
| `labels.label_text` | `labels.label_text_norm` | `tag_search_term` |
| `genres.genre` | `genres.genre_phash` | `title_search_term` |
| `subjects.subject` | `subjects.subject_sort` | `title_search_term` |
| `series.series` | `series.series_name_norm` | `title_search_term` |

## Case-insensitive display columns

| Table | Columns |
| --- | --- |
| `agents` | `agent_aliases`, `agent_canonical_name`, `agent_sort_name` |
| `backup_policies` | `backup_policy_name` |
| `backup_workflows` | `backup_workflow_name` |
| `custom_columns` | `custom_column_label`, `custom_column_name` |
| `database_metadata` | `database_metadata_db_name` |
| `digital_asset_workflow` | `digital_asset_workflow_assigned_to` |
| `digital_asset_workflow_events` | `digital_asset_workflow_event_actor` |
| `digital_assets` | `digital_asset_tag` |
| `expressions` | `expression_label`, `expression_subtitle`, `expression_title_override` |
| `feeds` | `feed_title` |
| `files` | `file_tag` |
| `genres` | `genre`, `genre_full`, `genre_sort` |
| `human_agents` | `human_agent_family_name`, `human_agent_given_name`, `human_agent_middle_name`, `human_agent_nationality`, `human_agent_preferred_name`, `human_agent_prefix`, `human_agent_suffix` |
| `images` | `image_tag` |
| `item_workflow` | `item_workflow_assigned_to` |
| `item_workflow_events` | `item_workflow_event_actor` |
| `items` | `item_location` |
| `labels` | `label_text` |
| `languages` | `language` |
| `manifestations` | `manifestation_edition_statement`, `manifestation_format_detail`, `manifestation_region_code`, `manifestation_subtitle` |
| `org_agents` | `org_agent_jurisdiction`, `org_agent_legal_name`, `org_agent_trading_name` |
| `replication_policies` | `replication_policy_name` |
| `series` | `series`, `series_full`, `series_sort` |
| `stores` | `store_name` |
| `subjects` | `subject`, `subject_full`, `subject_sort` |
| `tags` | `tag` |
| `workflow_states` | `workflow_state_label` |
| `workflow_steps` | `workflow_step_group`, `workflow_step_label` |
| `works` | `work_canonical_title`, `work_creator_sort`, `work_sort_title`, `work_title` |

## Case-sensitive display columns

| Table | Columns |
| --- | --- |
| `agents` | `agent_note` |
| `annotations` | `annotation_note_text`, `annotation_selected_text` |
| `asset_replicas` | `asset_replica_base_name`, `asset_replica_failure_reason`, `asset_replica_name` |
| `backup_workflow_state` | `backup_workflow_state_last_error` |
| `backup_workflows` | `backup_workflow_last_error` |
| `comments` | `comment` |
| `composite_digital_assets` | `composite_digital_asset_name` |
| `compressed_files` | `compressed_file_name` |
| `digital_asset_derivations` | `digital_asset_derivation_note` |
| `digital_asset_workflow` | `digital_asset_workflow_reason` |
| `digital_asset_workflow_events` | `digital_asset_workflow_event_note` |
| `digital_assets` | `digital_asset_auto_name`, `digital_asset_base_name`, `digital_asset_name`, `digital_asset_original_name` |
| `expressions` | `expression_origin_note` |
| `file_derivations` | `file_derivation_note` |
| `files` | `file_auto_name`, `file_base_name`, `file_name`, `file_original_name` |
| `folders` | `folder_name` |
| `human_agents` | `human_agent_biography` |
| `images` | `image_auto_name`, `image_base_name`, `image_name`, `image_original_name` |
| `item_workflow` | `item_workflow_reason` |
| `item_workflow_events` | `item_workflow_event_note` |
| `items` | `item_source_name` |
| `labels` | `label_description` |
| `manifestations` | `manifestation_note` |
| `metadata_dirtied_books` | `metadata_drtied_reason` |
| `new_books` | `new_book_name` |
| `notes` | `note` |
| `org_agent_relations` | `org_agent_relation_note` |
| `org_agents` | `org_agent_description` |
| `stores` | `store_location_note` |
| `synopses` | `synopsis` |
| `tags` | `tag_description` |
| `transform_run_inputs` | `transform_run_input_note` |
| `transform_run_outputs` | `transform_run_output_note` |
| `workflow_states` | `workflow_state_description` |
| `works` | `work_discovery_note` |

## Storage and API

Fresh SQLite and PostgreSQL schemas seed these decisions for every physical
column into `column_metadata`. The canonical display registry and machine
classifier are in
`LiuXin_alpha.databases.column_metadata`.

The same interface is available at each database layer:

```python
database.get_case_sensitivity("tags", "tag")  # False
database.set_case_sensitivity("tags", "tag", True)

metadata = database.get_column_metadata("tags", "tag")
database.set_column_metadata(metadata)

database.get_semantic_role("tags", "tag")
database.set_semantic_role("tags", "tag", ColumnSemanticRole.TAXONOMY_TERM)

database.get_normalization_profile("tags", "tag")
database.set_normalization_profile(
    "tags",
    "tag",
    ColumnNormalizationProfile.TAG_SEARCH_TERM,
)

database.get_comparison_column("tags", "tag")
database.set_comparison_column("tags", "tag", "tag_phash")

database.get_empty_value_policy("tags", "tag")
database.set_empty_value_policy(
    "tags",
    "tag",
    ColumnEmptyValuePolicy.NULL_OR_BLANK_IS_MISSING,
)

database.get_merge_policy("tags", "tag")
database.set_merge_policy("tags", "tag", ColumnMergePolicy.SET_UNION)

database.get_validation_profile("tags", "tag")
database.set_validation_profile(
    "tags",
    "tag",
    ColumnValidationProfile.TAXONOMY_TERM,
)
```

The driver uses `direct_get_case_sensitivity` and
`direct_set_case_sensitivity`; the driver wrapper and database use the names
shown above. The earlier `is_column_case_sensitive` /
`set_column_case_sensitive` spellings remain compatibility aliases. A stored
row overrides the canonical default for that database.

Every other policy field follows the same naming rule: drivers prefix the
method with `direct_`, while the wrapper and database expose the unprefixed
name. Each field setter reads the complete record, replaces only the requested
field, validates the result, and persists it through the complete-policy
setter.

`get_column_metadata` returns a frozen typed `ColumnMetadata` value. To change
one field while retaining the rest, use `dataclasses.replace` and persist the
result with `set_column_metadata`.

When adding any physical column, the schema generator supplies a machine
default automatically. When it is human-facing display text, add it to exactly one of
`CASE_INSENSITIVE_DISPLAY_COLUMNS` or `CASE_SENSITIVE_DISPLAY_COLUMNS`, update
this document, and add writer behavior coverage if the column participates in
deduplication.
