-- BREAK

-- -----------------------------------------------------
-- Table `files`
-- Legacy compatibility surface for pre-digital-assets storage code.
--
-- New storage work should prefer `digital_assets` plus `asset_replicas`.
-- This table remains while terminal sync, import, backup, and older contract
-- tests are migrated off the historical `files` row shape.
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `files` (
  `file_id` INTEGER PRIMARY KEY,

  -- Relations
  `file_item_id`   INTEGER NULL,
  `file_store_id`  INTEGER NULL,
  `file_folder_id` INTEGER NULL,

  -- Locator, relative to stores.store_root_uri
  `file_storage_key` TEXT NULL,

  -- Naming / descriptive hints
  `file_name` TEXT NULL,
  `file_base_name` TEXT NULL,
  `file_extension` TEXT NULL,
  `file_tag` TEXT NULL,
  `file_auto_name` TEXT NULL,
  `file_use_auto_name` INTEGER DEFAULT 1,

  -- Type / classification
  `file_mime_type` TEXT NULL,
  `file_role` TEXT NULL,
  `file_media_category` TEXT NULL,
  `file_class_mask` INTEGER NULL,
  `file_visibility_mask` INTEGER NULL,
  `file_critical` INTEGER NULL DEFAULT 1,

  -- Size / integrity
  `file_size_bytes` INTEGER NULL,
  `file_hash_sha256` TEXT NULL,
  `file_hash_blake3` TEXT NULL,
  `file_phash` TEXT NULL,
  `file_corrupt` INTEGER NULL,
  `file_integrity_status` TEXT NULL,
  `file_last_seen_timestamp_ep_k` INTEGER NULL,
  `file_last_integrity_check_timestamp_ep_k` INTEGER NULL,

  -- Provenance / ingestion
  `file_acquired_timestamp_ep_k` INTEGER NULL,
  `file_source` TEXT NULL,
  `file_original_name` TEXT NULL,
  `file_original_path` TEXT NULL,

  -- Processing / lineage placeholders
  `file_anthology` INTEGER NULL,
  `file_parent` TEXT NULL,
  `file_conversion_settings` TEXT NULL,
  `file_processed` INTEGER NULL DEFAULT 0,

  -- timestamps (epoch_ms)
  `file_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `file_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `file_source_created_datestamp_ep_k` INTEGER NULL,
  `file_source_modified_datestamp_ep_k` INTEGER NULL,

  `file_scratch` TEXT NULL,

  CONSTRAINT `file_item_fk`
    FOREIGN KEY (`file_item_id`)
    REFERENCES `items` (`item_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `file_store_fk`
    FOREIGN KEY (`file_store_id`)
    REFERENCES `stores` (`store_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `file_folder_fk`
    FOREIGN KEY (`file_folder_id`)
    REFERENCES `folders` (`folder_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE
);

-- BREAK
-- BREAK

CREATE UNIQUE INDEX IF NOT EXISTS `idx_files_unique_store_key`
ON `files` (`file_store_id`, `file_storage_key`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_files_item_id`
ON `files` (`file_item_id`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_files_folder_id`
ON `files` (`file_folder_id`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_files_hash_sha256`
ON `files` (`file_hash_sha256`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_files_class_mask`
ON `files` (`file_class_mask`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_files_visibility_mask`
ON `files` (`file_visibility_mask`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_files_role`
ON `files` (`file_role`);

-- BREAK
