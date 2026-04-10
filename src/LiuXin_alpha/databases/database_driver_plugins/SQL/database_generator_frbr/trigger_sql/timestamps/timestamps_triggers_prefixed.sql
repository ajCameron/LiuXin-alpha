PRAGMA `foreign_keys` = ON;

-- Auto-update modified timestamps for feeds
CREATE TRIGGER IF NOT EXISTS `trg_feeds_feed_touch_modified`
AFTER UPDATE ON `feeds`
WHEN NEW.`feed_modified_timestamp_ep_k` = OLD.`feed_modified_timestamp_ep_k`
BEGIN
  UPDATE `feeds`
  SET `feed_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `feed_id` = NEW.`feed_id`;
END;

-- Auto-update modified timestamps for last_read_positions
CREATE TRIGGER IF NOT EXISTS `trg_last_read_positions_last_read_position_touch_modified`
AFTER UPDATE ON `last_read_positions`
WHEN NEW.`last_read_position_modified_timestamp_ep_k` = OLD.`last_read_position_modified_timestamp_ep_k`
BEGIN
  UPDATE `last_read_positions`
  SET `last_read_position_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `last_read_position_id` = NEW.`last_read_position_id`;
END;

-- Auto-update modified timestamps for works
CREATE TRIGGER IF NOT EXISTS `trg_works_work_touch_modified`
AFTER UPDATE ON `works`
WHEN NEW.`work_modified_timestamp_ep_k` = OLD.`work_modified_timestamp_ep_k`
BEGIN
  UPDATE `works`
  SET `work_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `work_id` = NEW.`work_id`;
END;

-- Auto-update modified timestamps for expressions
CREATE TRIGGER IF NOT EXISTS `trg_expressions_expression_touch_modified`
AFTER UPDATE ON `expressions`
WHEN NEW.`expression_modified_timestamp_ep_k` = OLD.`expression_modified_timestamp_ep_k`
BEGIN
  UPDATE `expressions`
  SET `expression_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `expression_id` = NEW.`expression_id`;
END;

-- Auto-update modified timestamps for manifestations
CREATE TRIGGER IF NOT EXISTS `trg_manifestations_manifestation_touch_modified`
AFTER UPDATE ON `manifestations`
WHEN NEW.`manifestation_modified_timestamp_ep_k` = OLD.`manifestation_modified_timestamp_ep_k`
BEGIN
  UPDATE `manifestations`
  SET `manifestation_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `manifestation_id` = NEW.`manifestation_id`;
END;

-- Auto-update modified timestamps for items
CREATE TRIGGER IF NOT EXISTS `trg_items_item_touch_modified`
AFTER UPDATE ON `items`
WHEN NEW.`item_modified_timestamp_ep_k` = OLD.`item_modified_timestamp_ep_k`
BEGIN
  UPDATE `items`
  SET `item_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `item_id` = NEW.`item_id`;
END;

-- Auto-update modified timestamps for agents
CREATE TRIGGER IF NOT EXISTS `trg_agents_agent_touch_modified`
AFTER UPDATE ON `agents`
WHEN NEW.`agent_modified_timestamp_ep_k` = OLD.`agent_modified_timestamp_ep_k`
BEGIN
  UPDATE `agents`
  SET `agent_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `agent_id` = NEW.`agent_id`;
END;

-- Auto-update modified timestamps for org_agent_relations
CREATE TRIGGER IF NOT EXISTS `trg_org_agent_relations_org_agent_relation_touch_modified`
AFTER UPDATE ON `org_agent_relations`
WHEN NEW.`org_agent_relation_modified_timestamp_ep_k` = OLD.`org_agent_relation_modified_timestamp_ep_k`
BEGIN
  UPDATE `org_agent_relations`
  SET `org_agent_relation_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `org_agent_relation_id` = NEW.`org_agent_relation_id`;
END;

-- Auto-update modified timestamps for entity_identifiers
CREATE TRIGGER IF NOT EXISTS `trg_entity_identifiers_entity_identifier_touch_modified`
AFTER UPDATE ON `entity_identifiers`
WHEN NEW.`entity_identifier_modified_timestamp_ep_k` = OLD.`entity_identifier_modified_timestamp_ep_k`
BEGIN
  UPDATE `entity_identifiers`
  SET `entity_identifier_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `entity_identifier_id` = NEW.`entity_identifier_id`;
END;

-- Auto-update modified timestamps for item_identifiers
CREATE TRIGGER IF NOT EXISTS `trg_item_identifiers_item_identifier_touch_modified`
AFTER UPDATE ON `item_identifiers`
WHEN NEW.`item_identifier_modified_timestamp_ep_k` = OLD.`item_identifier_modified_timestamp_ep_k`
BEGIN
  UPDATE `item_identifiers`
  SET `item_identifier_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `item_identifier_id` = NEW.`item_identifier_id`;
END;

-- Auto-update modified timestamps for annotations
CREATE TRIGGER IF NOT EXISTS `trg_annotations_annotation_touch_modified`
AFTER UPDATE ON `annotations`
WHEN NEW.`annotation_modified_timestamp_ep_k` = OLD.`annotation_modified_timestamp_ep_k`
BEGIN
  UPDATE `annotations`
  SET `annotation_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `annotation_id` = NEW.`annotation_id`;
END;

-- Auto-update modified timestamps for comments
CREATE TRIGGER IF NOT EXISTS `trg_comments_comment_touch_modified`
AFTER UPDATE ON `comments`
WHEN NEW.`comment_modified_timestamp_ep_k` = OLD.`comment_modified_timestamp_ep_k`
BEGIN
  UPDATE `comments`
  SET `comment_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `comment_id` = NEW.`comment_id`;
END;

-- Auto-update modified timestamps for genres
CREATE TRIGGER IF NOT EXISTS `trg_genres_genre_touch_modified`
AFTER UPDATE ON `genres`
WHEN NEW.`genre_modified_timestamp_ep_k` = OLD.`genre_modified_timestamp_ep_k`
BEGIN
  UPDATE `genres`
  SET `genre_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `genre_id` = NEW.`genre_id`;
END;

-- Auto-update modified timestamps for labels
CREATE TRIGGER IF NOT EXISTS `trg_labels_label_touch_modified`
AFTER UPDATE ON `labels`
WHEN NEW.`label_modified_timestamp_ep_k` = OLD.`label_modified_timestamp_ep_k`
BEGIN
  UPDATE `labels`
  SET `label_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `label_id` = NEW.`label_id`;
END;

-- Auto-update modified timestamps for notes
CREATE TRIGGER IF NOT EXISTS `trg_notes_note_touch_modified`
AFTER UPDATE ON `notes`
WHEN NEW.`note_modified_timestamp_ep_k` = OLD.`note_modified_timestamp_ep_k`
BEGIN
  UPDATE `notes`
  SET `note_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `note_id` = NEW.`note_id`;
END;

-- Auto-update modified timestamps for ratings
CREATE TRIGGER IF NOT EXISTS `trg_ratings_rating_touch_modified`
AFTER UPDATE ON `ratings`
WHEN NEW.`rating_modified_timestamp_ep_k` = OLD.`rating_modified_timestamp_ep_k`
BEGIN
  UPDATE `ratings`
  SET `rating_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `rating_id` = NEW.`rating_id`;
END;

-- Auto-update modified timestamps for series
CREATE TRIGGER IF NOT EXISTS `trg_series_series_touch_modified`
AFTER UPDATE ON `series`
WHEN NEW.`series_modified_timestamp_ep_k` = OLD.`series_modified_timestamp_ep_k`
BEGIN
  UPDATE `series`
  SET `series_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `series_id` = NEW.`series_id`;
END;

-- Auto-update modified timestamps for subjects
CREATE TRIGGER IF NOT EXISTS `trg_subjects_subject_touch_modified`
AFTER UPDATE ON `subjects`
WHEN NEW.`subject_modified_timestamp_ep_k` = OLD.`subject_modified_timestamp_ep_k`
BEGIN
  UPDATE `subjects`
  SET `subject_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `subject_id` = NEW.`subject_id`;
END;

-- Auto-update modified timestamps for synopses
CREATE TRIGGER IF NOT EXISTS `trg_synopses_synopsis_touch_modified`
AFTER UPDATE ON `synopses`
WHEN NEW.`synopsis_modified_timestamp_ep_k` = OLD.`synopsis_modified_timestamp_ep_k`
BEGIN
  UPDATE `synopses`
  SET `synopsis_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `synopsis_id` = NEW.`synopsis_id`;
END;

-- Auto-update modified timestamps for stores
CREATE TRIGGER IF NOT EXISTS `trg_stores_store_touch_modified`
AFTER UPDATE ON `stores`
WHEN NEW.`store_modified_timestamp_ep_k` = OLD.`store_modified_timestamp_ep_k`
BEGIN
  UPDATE `stores`
  SET `store_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `store_id` = NEW.`store_id`;
END;

-- Auto-update modified timestamps for folders
CREATE TRIGGER IF NOT EXISTS `trg_folders_folder_touch_modified`
AFTER UPDATE ON `folders`
WHEN NEW.`folder_modified_timestamp_ep_k` = OLD.`folder_modified_timestamp_ep_k`
BEGIN
  UPDATE `folders`
  SET `folder_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `folder_id` = NEW.`folder_id`;
END;

-- Auto-update modified timestamps for digital_assets
CREATE TRIGGER IF NOT EXISTS `trg_digital_assets_touch_modified`
AFTER UPDATE ON `digital_assets`
WHEN NEW.`digital_asset_modified_timestamp_ep_k` = OLD.`digital_asset_modified_timestamp_ep_k`
BEGIN
  UPDATE `digital_assets`
  SET `digital_asset_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `digital_asset_id` = NEW.`digital_asset_id`;
END;

-- Auto-update modified timestamps for asset_replicas
CREATE TRIGGER IF NOT EXISTS `trg_asset_replicas_touch_modified`
AFTER UPDATE ON `asset_replicas`
WHEN NEW.`asset_replica_modified_timestamp_ep_k` = OLD.`asset_replica_modified_timestamp_ep_k`
BEGIN
  UPDATE `asset_replicas`
  SET `asset_replica_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `asset_replica_id` = NEW.`asset_replica_id`;
END;

-- Auto-update modified timestamps for devices
CREATE TRIGGER IF NOT EXISTS `trg_devices_device_touch_modified`
AFTER UPDATE ON `devices`
WHEN NEW.`device_modified_timestamp_ep_k` = OLD.`device_modified_timestamp_ep_k`
BEGIN
  UPDATE `devices`
  SET `device_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `device_id` = NEW.`device_id`;
END;

-- Auto-update modified timestamps for workflow_states
CREATE TRIGGER IF NOT EXISTS `trg_workflow_states_workflow_state_touch_modified`
AFTER UPDATE ON `workflow_states`
WHEN NEW.`workflow_state_modified_timestamp_ep_k` = OLD.`workflow_state_modified_timestamp_ep_k`
BEGIN
  UPDATE `workflow_states`
  SET `workflow_state_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `workflow_state_id` = NEW.`workflow_state_id`;
END;
