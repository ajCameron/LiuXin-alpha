# LiuXin current FRBR database (ASCII sketch)

Generated from the FRBR generator source tree, not by hand.
Source root: `/tmp/liuxin_dbchart/src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr`

## quick counts

- tables from `table_sql/`: 60
- aggregate views: 22
- generated interlink tables requested in TOML: 85
- generated intralink tables requested in TOML: 8
- live generator check: live generation failed: near `entity_identifier_scratch`: syntax error while creating `entity_identifiers`

## sketch

```text
                               [languages]
                                   ^   ^
                                   |   |
                work_original_language_id   expression_language_id
                                   |   |
[agents] <==> [works] <==> [expressions] <==> [manifestations] ---> [items]
   ||            ||                 ||                 ||               ||
   ||            ||                 ||                 ||               ||
   ||            ||                 ||                 ||               ||
   ||            ||                 ||                 ||               ++--> [images]
   ||            ||                 ||                 ||
   ||            ||                 ||                 ++<==> [agents]
   ||            ||                 ||
   ||            ||                 ++<==> [notes] / [labels] / [languages] / [images]
   ||            ||
   ||            ++<==> [agents] / [comments]
   ||
   ++<==> [works / expressions / manifestations / items] via generated agent_*_links

[entity_identifiers]  --> polymorphic attachment for work/expression/manifestation/item
[item_identifiers]    --> direct attachment to items
[comments]            --> attached to many entity tables via generated *_comment_links
[notes]               --> attached to selected entity tables via generated *_note_links
[genres/labels/ratings/series/subjects/synopses] --> additional metadata hanging off the graph

storage / file side:

[stores] ---> [folders] ---> [asset_replicas] ---> [digital_assets] <--> [digital_asset_derivations]
   ||            ||               ||
   ||            ||               ++--> [backup_presence_links] / [backup_workflow_outputs]
   ||            ||
   ||            ++--> [images]
   ||
   ++<==> [devices] / [comments]

[items] <==> [digital_assets]                 via digital_asset_item_links
[items] <==> [composite_digital_assets]       via composite_digital_asset_item_links
[composite_digital_assets] <==> [digital_assets] via composite_digital_asset_digital_asset_links

workflow side:

[workflow_states] -> [workflow_steps] -> [transform_runs] -> [transform_run_inputs/outputs]
[digital_asset_workflow] -> [digital_asset_workflow_events]
[item_workflow]          -> [item_workflow_events]
```

## tables by group

### core_wemi (7)

- `agents`
- `expressions`
- `human_agents`
- `items`
- `manifestations`
- `org_agents`
- `works`

### core_attributes (3)

- `entity_identifiers`
- `item_identifiers`
- `org_agent_relations`

### additional_metadata (9)

- `annotations`
- `comments`
- `genres`
- `labels`
- `notes`
- `ratings`
- `series`
- `subjects`
- `synopses`

### storage (14)

- `asset_replicas`
- `backup_policies`
- `backup_presence_links`
- `backup_workflow_outputs`
- `backup_workflow_sources`
- `backup_workflow_state`
- `backup_workflows`
- `composite_digital_assets`
- `devices`
- `digital_assets`
- `folders`
- `images`
- `replication_policies`
- `stores`

### workflow (10)

- `digital_asset_derivations`
- `digital_asset_workflow`
- `digital_asset_workflow_events`
- `item_workflow`
- `item_workflow_events`
- `transform_run_inputs`
- `transform_run_outputs`
- `transform_runs`
- `workflow_states`
- `workflow_steps`

### db_metadata (16)

- `compressed_files`
- `conversion_options`
- `custom_columns`
- `database_metadata`
- `database_version`
- `expressions_plugin_data`
- `feeds`
- `hashes`
- `items_plugin_data`
- `last_read_positions`
- `library_id`
- `manifestations_plugin_data`
- `metadata_dirtied_books`
- `new_books`
- `preferences`
- `works_plugin_data`

### constants (1)

- `languages`

## direct foreign keys declared in main SQL

- `asset_replicas` --asset_replica_digital_asset_id--> `digital_assets`
- `asset_replicas` --asset_replica_folder_id--> `folders`
- `asset_replicas` --asset_replica_store_id--> `stores`
- `backup_presence_links` --backup_presence_link_source_asset_replica_id--> `asset_replicas`
- `backup_presence_links` --backup_presence_link_workflow_id--> `backup_workflows`
- `backup_presence_links` --backup_presence_link_backup_store_id--> `stores`
- `backup_presence_links` --backup_presence_link_source_store_id--> `stores`
- `backup_workflow_outputs` --backup_workflow_output_asset_replica_id--> `asset_replicas`
- `backup_workflow_outputs` --backup_workflow_output_workflow_id--> `backup_workflows`
- `backup_workflow_outputs` --backup_workflow_output_digital_asset_id--> `digital_assets`
- `backup_workflow_outputs` --backup_workflow_output_store_id--> `stores`
- `backup_workflow_sources` --backup_workflow_source_workflow_id--> `backup_workflows`
- `backup_workflow_state` --backup_workflow_state_workflow_id--> `backup_workflows`
- `backup_workflows` --backup_workflow_destination_store_id--> `stores`
- `backup_workflows` --backup_workflow_staging_store_id--> `stores`
- `composite_digital_assets` --composite_digital_asset_backup_policy_id--> `backup_policies`
- `composite_digital_assets` --composite_digital_asset_replication_policy_id--> `replication_policies`
- `digital_assets` --digital_asset_backup_policy_id--> `backup_policies`
- `digital_assets` --digital_asset_replication_policy_id--> `replication_policies`
- `expressions` --expression_language_id--> `languages`
- `folders` --folder_default_backup_policy_id--> `backup_policies`
- `folders` --folder_parent_id--> `folders`
- `folders` --folder_default_replication_policy_id--> `replication_policies`
- `folders` --folder_store_id--> `stores`
- `genres` --genre_parent_id--> `genres`
- `images` --image_folder_id--> `folders`
- `images` --image_item_id--> `items`
- `images` --image_store_id--> `stores`
- `item_identifiers` --item_identifier_item_id--> `items`
- `items` --item_manifestation_id--> `manifestations`
- `series` --series_parent_id--> `series`
- `stores` --store_default_backup_policy_id--> `backup_policies`
- `stores` --store_default_replication_policy_id--> `replication_policies`
- `subjects` --subject_parent_id--> `subjects`
- `works` --work_original_language_id--> `languages`

## generated interlink tables (from TOML)

- `agent_comment_links` : `agents` <=> `comments`  [one_to_many; extra cols: priority, origin]
- `agent_expression_links` : `agents` <=> `expressions`  [many_to_many; extra cols: priority, type, origin]
- `agent_feed_links` : `agents` <=> `feeds`  [many_to_many; extra cols: priority, type]
- `agent_image_links` : `agents` <=> `images`  [many_to_many; extra cols: priority, type, origin]
- `agent_item_links` : `agents` <=> `items`  [many_to_many; extra cols: priority, type, origin]
- `agent_label_links` : `agents` <=> `labels`  [many_to_many; extra cols: origin]
- `agent_language_links` : `agents` <=> `languages`  [many_to_many; extra cols: priority, type]
- `agent_manifestation_links` : `agents` <=> `manifestations`  [many_to_many; extra cols: priority, type]
- `agent_series_links` : `agents` <=> `series`  [many_to_many; extra cols: priority, type]
- `agent_subject_links` : `agents` <=> `subjects`  [many_to_many; extra cols: priority, type]
- `agent_synopse_links` : `agents` <=> `synopses`  [many_to_many; extra cols: priority]
- `agent_work_links` : `agents` <=> `works`  [many_to_many; extra cols: priority, type]
- `comment_compressed_file_links` : `comments` <=> `compressed_files`  [many_to_one; extra cols: priority]
- `comment_conversion_option_links` : `comments` <=> `conversion_options`  [many_to_one; extra cols: priority]
- `comment_custom_column_links` : `comments` <=> `custom_columns`  [many_to_one; extra cols: priority]
- `comment_device_links` : `comments` <=> `devices`  [many_to_one; extra cols: priority]
- `comment_expression_links` : `comments` <=> `expressions`  [many_to_one; extra cols: priority]
- `comment_feed_links` : `comments` <=> `feeds`  [many_to_one; extra cols: priority]
- `comment_item_links` : `comments` <=> `items`  [many_to_one; extra cols: priority, type]
- `comment_manifestation_links` : `comments` <=> `manifestations`  [many_to_one; extra cols: priority]
- `comment_series_links` : `comments` <=> `series`  [many_to_one; extra cols: priority]
- `comment_store_links` : `comments` <=> `stores`  [many_to_many; extra cols: priority]
- `comment_work_links` : `comments` <=> `works`  [many_to_one; extra cols: priority]
- `composite_digital_asset_digital_asset_links` : `composite_digital_assets` <=> `digital_assets`  [many_to_many; extra cols: type, origin, sequence_number, is_required]
- `composite_digital_asset_item_links` : `composite_digital_assets` <=> `items`  [many_to_many; extra cols: priority, type, origin, primary]
- `device_expression_links` : `devices` <=> `expressions`  [many_to_many; extra cols: priority, type, data, policy]
- `device_feed_links` : `devices` <=> `feeds`  [many_to_many; extra cols: priority, type, data, policy]
- `device_item_links` : `devices` <=> `items`  [many_to_many; extra cols: priority, type, data, policy]
- `device_label_links` : `devices` <=> `labels`  [many_to_many; extra cols: (none)]
- `device_note_links` : `devices` <=> `notes`  [one_to_many; extra cols: priority]
- `device_series_links` : `devices` <=> `series`  [many_to_many; extra cols: priority, type, policy]
- `device_store_links` : `devices` <=> `stores`  [many_to_many; extra cols: priority, type, policy]
- `digital_asset_hashe_links` : `digital_assets` <=> `hashes`  [one_to_many; extra cols: type]
- `digital_asset_image_links` : `digital_assets` <=> `images`  [many_to_many; extra cols: priority, type]
- `digital_asset_item_links` : `digital_assets` <=> `items`  [many_to_many; extra cols: priority, type, origin, primary]
- `digital_asset_label_links` : `digital_assets` <=> `labels`  [many_to_many; extra cols: (none)]
- `digital_asset_language_links` : `digital_assets` <=> `languages`  [many_to_many; extra cols: priority, type]
- `digital_asset_last_read_position_links` : `digital_assets` <=> `last_read_positions`  [one_to_many; extra cols: (none)]
- `expression_image_links` : `expressions` <=> `images`  [many_to_many; extra cols: priority]
- `expression_label_links` : `expressions` <=> `labels`  [many_to_many; extra cols: (none)]
- `expression_language_links` : `expressions` <=> `languages`  [many_to_many; extra cols: priority, type]
- `expression_manifestation_links` : `expressions` <=> `manifestations`  [many_to_many; extra cols: priority, primary, origin]
- `expression_note_links` : `expressions` <=> `notes`  [one_to_many; extra cols: priority]
- `feed_genre_links` : `feeds` <=> `genres`  [many_to_many; extra cols: priority]
- `feed_image_links` : `feeds` <=> `images`  [many_to_many; extra cols: priority]
- `feed_item_links` : `feeds` <=> `items`  [many_to_many; extra cols: type]
- `feed_language_links` : `feeds` <=> `languages`  [many_to_many; extra cols: type]
- `feed_note_links` : `feeds` <=> `notes`  [one_to_many; extra cols: nullable]
- `feed_subject_links` : `feeds` <=> `subjects`  [many_to_many; extra cols: (none)]
- `feed_synopse_links` : `feeds` <=> `synopses`  [many_to_one; extra cols: priority]
- `feed_folder_links` : `folders` <=> `feeds`  [many_to_many; extra cols: (none)]
- `folder_item_links` : `folders` <=> `items`  [many_to_many; extra cols: priority, type]
- `folder_series_links` : `folders` <=> `series`  [many_to_many; extra cols: (none)]
- `folder_store_links` : `folders` <=> `stores`  [many_to_many; extra cols: priority, type, policy]
- `folder_work_links` : `folders` <=> `works`  [many_to_many; extra cols: priority, type]
- `genre_work_links` : `genres` <=> `works`  [many_to_many; extra cols: priority, type]
- `hashe_new_book_links` : `hashes` <=> `new_books`  [many_to_one; extra cols: type]
- `image_item_links` : `images` <=> `items`  [many_to_many; extra cols: priority, type]
- `image_label_links` : `images` <=> `labels`  [many_to_many; extra cols: (none)]
- `image_manifestation_links` : `images` <=> `manifestations`  [many_to_many; extra cols: priority, type]
- `image_series_links` : `images` <=> `series`  [many_to_many; extra cols: priority, type]
- `image_work_links` : `images` <=> `works`  [many_to_many; extra cols: priority, type]
- `item_label_links` : `items` <=> `labels`  [many_to_many; extra cols: (none)]
- `item_language_links` : `items` <=> `languages`  [many_to_many; extra cols: priority, type]
- `item_last_read_position_links` : `items` <=> `last_read_positions`  [many_to_many; extra cols: priority]
- `label_new_book_links` : `labels` <=> `new_books`  [many_to_many; extra cols: (none)]
- `label_series_links` : `labels` <=> `series`  [many_to_many; extra cols: (none)]
- `label_store_links` : `labels` <=> `stores`  [many_to_many; extra cols: (none)]
- `label_work_links` : `labels` <=> `works`  [many_to_many; extra cols: (none)]
- `language_manifestation_links` : `languages` <=> `manifestations`  [many_to_many; extra cols: type]
- `language_work_links` : `languages` <=> `works`  [many_to_many; extra cols: priority, type]
- `agent_note_links` : `notes` <=> `agents`  [many_to_one; extra cols: (none)]
- `note_store_links` : `notes` <=> `stores`  [many_to_many; extra cols: (none)]
- `note_work_links` : `notes` <=> `works`  [many_to_many; extra cols: (none)]
- `rating_series_links` : `ratings` <=> `series`  [many_to_many; extra cols: priority, type, nullable, origin]
- `rating_work_links` : `ratings` <=> `works`  [many_to_many; extra cols: priority, type, nullable, origin]
- `series_subject_links` : `series` <=> `subjects`  [many_to_many; extra cols: priority]
- `series_synopse_links` : `series` <=> `synopses`  [many_to_many; extra cols: priority, nullable, origin]
- `series_work_links` : `series` <=> `works`  [many_to_many; extra cols: priority, type]
- `store_subject_links` : `stores` <=> `subjects`  [many_to_many; extra cols: priority, type, nullable, policy]
- `store_synopse_links` : `stores` <=> `synopses`  [many_to_many; extra cols: priority, nullable, origin]
- `subject_synopse_links` : `subjects` <=> `synopses`  [many_to_many; extra cols: priority, origin]
- `subject_work_links` : `subjects` <=> `works`  [many_to_many; extra cols: priority]
- `synopse_work_links` : `synopses` <=> `works`  [many_to_many; extra cols: priority, type]
- `expression_work_links` : `works` <=> `expressions`  [many_to_many; extra cols: priority, primary, origin]

## generated intralink tables (from TOML)

- `compressed_file_compressed_file_intralinks` : `compressed_files` <=> `compressed_files`  [extra cols: type]
- `image_image_intralinks` : `images` <=> `images`  [extra cols: type, data, origin]
- `digital_asset_digital_asset_intralinks` : `digital_assets` <=> `digital_assets`  [extra cols: type, data, origin]
- `store_store_intralinks` : `stores` <=> `stores`  [extra cols: type, data, origin]
- `work_work_intralinks` : `works` <=> `works`  [extra cols: type, origin]
- `expression_expression_intralinks` : `expressions` <=> `expressions`  [extra cols: type, origin]
- `manifestation_manifestation_intralinks` : `manifestations` <=> `manifestations`  [extra cols: type, origin]
- `item_item_intralinks` : `items` <=> `items`  [extra cols: type, origin]

## aggregate / compatibility views

- `wemi_rays_v`
- `wemi_primary_rays_v`
- `wemi_ray_items_v`
- `wemi_work_stats_v`
- `titles_v`
- `titles`
- `books_v`
- `books`
- `digital_asset_inventory_v`
- `file_inventory_v`
- `agent_credits_v`
- `book_publishers_v`
- `publishers_v`
- `subjects_tags_v`
- `identifiers_v`
- `identifiers`
- `ingest_audit_v`
- `ingest_audit`
- `duplicate_candidates_v`
- `duplicate_candidates`
- `search_seed_v`
- `search_seed`

## notes

- This chart is source-derived. It does not require the generator to run successfully.
- Some metadata families exist as tables now even where their higher-level container APIs are still evolving.
- The current schema is graph-shaped, not a strict WEMI pyramid: Work<=>Expression and Expression<=>Manifestation are both many-to-many via generated link tables.
