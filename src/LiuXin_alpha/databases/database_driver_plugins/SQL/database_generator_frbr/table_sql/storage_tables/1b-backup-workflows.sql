-- BREAK

-- -----------------------------------------------------
-- Table `backup_workflows`
-- Durable backup/export workflow intent and high-level status.
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `backup_workflows` (
  `backup_workflow_id` INTEGER PRIMARY KEY,

  `backup_workflow_name` TEXT NULL,
  `backup_workflow_kind` TEXT NULL,

  `backup_workflow_destination_store_id` INTEGER NULL,
  `backup_workflow_staging_store_id` INTEGER NULL,

  `backup_workflow_output_url` TEXT NULL,
  `backup_workflow_verify_after_build` INTEGER NOT NULL DEFAULT 1,
  `backup_workflow_cleanup_staging_after_success` INTEGER NOT NULL DEFAULT 0,
  `backup_workflow_staging_root` TEXT NULL,
  `backup_workflow_options_json` TEXT NULL,

  `backup_workflow_status` TEXT NOT NULL DEFAULT 'draft',
  `backup_workflow_last_error` TEXT NULL,

  `backup_workflow_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `backup_workflow_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `backup_workflow_source_created_datestamp_ep_k` INTEGER NULL,
  `backup_workflow_source_modified_datestamp_ep_k` INTEGER NULL,
  `backup_workflow_scratch` TEXT NULL,

  CONSTRAINT `backup_workflow_destination_store_fk`
    FOREIGN KEY (`backup_workflow_destination_store_id`)
    REFERENCES `stores` (`store_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `backup_workflow_staging_store_fk`
    FOREIGN KEY (`backup_workflow_staging_store_id`)
    REFERENCES `stores` (`store_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `backup_workflow_verify_after_build_bool`
    CHECK (`backup_workflow_verify_after_build` IN (0,1)),

  CONSTRAINT `backup_workflow_cleanup_after_success_bool`
    CHECK (`backup_workflow_cleanup_staging_after_success` IN (0,1)),

  CONSTRAINT `backup_workflow_status_check`
    CHECK (`backup_workflow_status` IN ('draft','running','failed','complete','cancelled'))
);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_backup_workflows_kind`
ON `backup_workflows` (`backup_workflow_kind`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_backup_workflows_destination_store`
ON `backup_workflows` (`backup_workflow_destination_store_id`);

-- BREAK
-- BREAK

-- -----------------------------------------------------
-- Table `backup_workflow_sources`
-- One designated source row per workflow.
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `backup_workflow_sources` (
  `backup_workflow_source_id` INTEGER PRIMARY KEY,

  `backup_workflow_source_workflow_id` INTEGER NULL,
  `backup_workflow_source_ordinal` INTEGER NULL,
  `backup_workflow_source_kind` TEXT NULL,
  `backup_workflow_source_identifier` TEXT NULL,
  `backup_workflow_source_archive_path` TEXT NULL,
  `backup_workflow_source_expected_size` INTEGER NULL,
  `backup_workflow_source_expected_hash` TEXT NULL,
  `backup_workflow_source_file_id` INTEGER NULL,
  `backup_workflow_source_asset_replica_id` INTEGER NULL,
  `backup_workflow_source_store_id` INTEGER NULL,

  `backup_workflow_source_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `backup_workflow_source_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `backup_workflow_source_source_created_datestamp_ep_k` INTEGER NULL,
  `backup_workflow_source_source_modified_datestamp_ep_k` INTEGER NULL,
  `backup_workflow_source_scratch` TEXT NULL,

  CONSTRAINT `backup_workflow_source_workflow_fk`
    FOREIGN KEY (`backup_workflow_source_workflow_id`)
    REFERENCES `backup_workflows` (`backup_workflow_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `backup_workflow_source_kind_check`
    CHECK (`backup_workflow_source_kind` IN ('local_path','store_location')),

  CONSTRAINT `backup_workflow_source_ordinal_non_negative`
    CHECK (`backup_workflow_source_ordinal` >= 0),

  CONSTRAINT `backup_workflow_source_expected_size_non_negative`
    CHECK (`backup_workflow_source_expected_size` IS NULL OR `backup_workflow_source_expected_size` >= 0),

  CONSTRAINT `backup_workflow_source_workflow_ordinal_unique`
    UNIQUE (`backup_workflow_source_workflow_id`, `backup_workflow_source_ordinal`),

  CONSTRAINT `backup_workflow_source_workflow_archive_path_unique`
    UNIQUE (`backup_workflow_source_workflow_id`, `backup_workflow_source_archive_path`)
);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_backup_workflow_sources_workflow_id`
ON `backup_workflow_sources` (`backup_workflow_source_workflow_id`);

-- BREAK
-- BREAK

-- -----------------------------------------------------
-- Table `backup_workflow_state`
-- One resumable checkpoint row per workflow.
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `backup_workflow_state` (
  `backup_workflow_state_id` INTEGER PRIMARY KEY,

  `backup_workflow_state_workflow_id` INTEGER NULL,
  `backup_workflow_state_status` TEXT NOT NULL DEFAULT 'draft',
  `backup_workflow_state_next_source_index` INTEGER NOT NULL DEFAULT 0,
  `backup_workflow_state_staged_source_count` INTEGER NOT NULL DEFAULT 0,
  `backup_workflow_state_completed_steps_json` TEXT NULL,
  `backup_workflow_state_source_results_json` TEXT NULL,
  `backup_workflow_state_output_artifact_url` TEXT NULL,
  `backup_workflow_state_last_error` TEXT NULL,

  `backup_workflow_state_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `backup_workflow_state_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `backup_workflow_state_source_created_datestamp_ep_k` INTEGER NULL,
  `backup_workflow_state_source_modified_datestamp_ep_k` INTEGER NULL,
  `backup_workflow_state_scratch` TEXT NULL,

  CONSTRAINT `backup_workflow_state_workflow_fk`
    FOREIGN KEY (`backup_workflow_state_workflow_id`)
    REFERENCES `backup_workflows` (`backup_workflow_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `backup_workflow_state_workflow_unique`
    UNIQUE (`backup_workflow_state_workflow_id`),

  CONSTRAINT `backup_workflow_state_status_check`
    CHECK (`backup_workflow_state_status` IN ('draft','running','failed','complete','cancelled')),

  CONSTRAINT `backup_workflow_state_next_source_index_non_negative`
    CHECK (`backup_workflow_state_next_source_index` >= 0),

  CONSTRAINT `backup_workflow_state_staged_source_count_non_negative`
    CHECK (`backup_workflow_state_staged_source_count` >= 0)
);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_backup_workflow_state_workflow_id`
ON `backup_workflow_state` (`backup_workflow_state_workflow_id`);

-- BREAK
-- BREAK

-- -----------------------------------------------------
-- Table `backup_workflow_outputs`
-- Historical output artifacts produced by workflows.
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `backup_workflow_outputs` (
  `backup_workflow_output_id` INTEGER PRIMARY KEY,

  `backup_workflow_output_workflow_id` INTEGER NULL,
  `backup_workflow_output_url` TEXT NULL,
  `backup_workflow_output_digital_asset_id` INTEGER NULL,
  `backup_workflow_output_asset_replica_id` INTEGER NULL,
  `backup_workflow_output_store_id` INTEGER NULL,
  `backup_workflow_output_verified_ok` INTEGER NULL,

  `backup_workflow_output_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `backup_workflow_output_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `backup_workflow_output_source_created_datestamp_ep_k` INTEGER NULL,
  `backup_workflow_output_source_modified_datestamp_ep_k` INTEGER NULL,
  `backup_workflow_output_scratch` TEXT NULL,

  CONSTRAINT `backup_workflow_output_workflow_fk`
    FOREIGN KEY (`backup_workflow_output_workflow_id`)
    REFERENCES `backup_workflows` (`backup_workflow_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `backup_workflow_output_digital_asset_fk`
    FOREIGN KEY (`backup_workflow_output_digital_asset_id`)
    REFERENCES `digital_assets` (`digital_asset_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `backup_workflow_output_asset_replica_fk`
    FOREIGN KEY (`backup_workflow_output_asset_replica_id`)
    REFERENCES `asset_replicas` (`asset_replica_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `backup_workflow_output_store_fk`
    FOREIGN KEY (`backup_workflow_output_store_id`)
    REFERENCES `stores` (`store_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `backup_workflow_output_verified_ok_bool`
    CHECK (`backup_workflow_output_verified_ok` IS NULL OR `backup_workflow_output_verified_ok` IN (0,1))
);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_backup_workflow_outputs_workflow_id`
ON `backup_workflow_outputs` (`backup_workflow_output_workflow_id`);

-- BREAK
