-- BREAK

-- -----------------------------------------------------
-- Table `backup_presence_links`
-- Durable protected links from source files/replicas to completed backup stores.
-- This is deliberately store-facing rather than workflow-facing: workflows may
-- create these rows, but the rows describe persisted backup presence, not just
-- job execution history.
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `backup_presence_links` (
  `backup_presence_link_id` INTEGER PRIMARY KEY,

  `backup_presence_link_backup_store_id` INTEGER NOT NULL,
  `backup_presence_link_workflow_id` INTEGER NULL,

  `backup_presence_link_source_identifier` TEXT NOT NULL,
  `backup_presence_link_source_kind` TEXT NULL,
  `backup_presence_link_source_file_id` INTEGER NULL,
  `backup_presence_link_source_asset_replica_id` INTEGER NULL,
  `backup_presence_link_source_store_id` INTEGER NULL,

  `backup_presence_link_archive_path` TEXT NOT NULL,
  `backup_presence_link_type` TEXT NOT NULL DEFAULT 'packed_presence',
  `backup_presence_link_output_url` TEXT NULL,

  `backup_presence_link_is_protected` INTEGER NOT NULL DEFAULT 1,
  `backup_presence_link_is_immutable` INTEGER NOT NULL DEFAULT 1,

  `backup_presence_link_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `backup_presence_link_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `backup_presence_link_scratch` TEXT NULL,

  CONSTRAINT `backup_presence_link_backup_store_fk`
    FOREIGN KEY (`backup_presence_link_backup_store_id`)
    REFERENCES `stores` (`store_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `backup_presence_link_workflow_fk`
    FOREIGN KEY (`backup_presence_link_workflow_id`)
    REFERENCES `backup_workflows` (`backup_workflow_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `backup_presence_link_source_asset_replica_fk`
    FOREIGN KEY (`backup_presence_link_source_asset_replica_id`)
    REFERENCES `asset_replicas` (`asset_replica_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `backup_presence_link_source_store_fk`
    FOREIGN KEY (`backup_presence_link_source_store_id`)
    REFERENCES `stores` (`store_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `backup_presence_link_has_source_check`
    CHECK (`backup_presence_link_source_identifier` <> '' OR `backup_presence_link_source_file_id` IS NOT NULL OR `backup_presence_link_source_asset_replica_id` IS NOT NULL),

  CONSTRAINT `backup_presence_link_protected_bool_check`
    CHECK (`backup_presence_link_is_protected` IN (0,1)),

  CONSTRAINT `backup_presence_link_immutable_bool_check`
    CHECK (`backup_presence_link_is_immutable` IN (0,1))
);

-- BREAK
-- BREAK

CREATE UNIQUE INDEX IF NOT EXISTS `idx_backup_presence_links_unique_store_archive_path`
ON `backup_presence_links` (`backup_presence_link_backup_store_id`, `backup_presence_link_archive_path`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_backup_presence_links_source_file_id`
ON `backup_presence_links` (`backup_presence_link_source_file_id`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_backup_presence_links_source_asset_replica_id`
ON `backup_presence_links` (`backup_presence_link_source_asset_replica_id`);

-- BREAK
-- BREAK
