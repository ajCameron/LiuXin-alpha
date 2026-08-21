-- BREAK

-- -----------------------------------------------------
-- Table `digital_assets`
-- One row per atomic, byte-bearing managed digital asset.
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `digital_assets` (
  `digital_asset_id` INTEGER PRIMARY KEY,

  -- Naming / descriptive hints
  `digital_asset_name` TEXT NULL,
  `digital_asset_base_name` TEXT NULL,
  `digital_asset_extension` TEXT NULL,
  `digital_asset_tag` TEXT NULL,
  `digital_asset_auto_name` TEXT NULL,
  `digital_asset_use_auto_name` INTEGER DEFAULT 1,

  -- Type / classification
  `digital_asset_mime_type` TEXT NULL,
  `digital_asset_media_category` TEXT NULL,
  `digital_asset_class_mask` INTEGER NULL,
  `digital_asset_visibility_mask` INTEGER NULL,
  `digital_asset_critical` INTEGER NULL DEFAULT 1,

  -- Size / integrity
  `digital_asset_size_bytes` INTEGER NULL,
  `digital_asset_hash_sha256` TEXT NULL,
  `digital_asset_hash_blake3` TEXT NULL,
  `digital_asset_phash` TEXT NULL,
  `digital_asset_corrupt` INTEGER NULL,
  `digital_asset_integrity_status` TEXT NULL,
  `digital_asset_last_seen_timestamp_ep_k` INTEGER NULL,
  `digital_asset_last_integrity_check_timestamp_ep_k` INTEGER NULL,

  -- Provenance / ingestion
  `digital_asset_acquired_timestamp_ep_k` INTEGER NULL,
  `digital_asset_source` TEXT NULL,
  `digital_asset_original_name` TEXT NULL,
  `digital_asset_original_path` TEXT NULL,

  -- Policy assignment
  `digital_asset_replication_policy_id` INTEGER NULL,
  `digital_asset_backup_policy_id` INTEGER NULL,

  -- Processing / lineage placeholders
  `digital_asset_conversion_settings` TEXT NULL,
  `digital_asset_processed` INTEGER NULL DEFAULT 0,

  -- timestamps (epoch_ms)
  `digital_asset_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `digital_asset_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `digital_asset_source_created_datestamp_ep_k` INTEGER NULL,
  `digital_asset_source_modified_datestamp_ep_k` INTEGER NULL,

  `digital_asset_scratch` TEXT NULL,

  CONSTRAINT `digital_asset_replication_policy_fk`
    FOREIGN KEY (`digital_asset_replication_policy_id`)
    REFERENCES `replication_policies` (`replication_policy_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `digital_asset_backup_policy_fk`
    FOREIGN KEY (`digital_asset_backup_policy_id`)
    REFERENCES `backup_policies` (`backup_policy_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE
);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_digital_assets_hash_sha256`
ON `digital_assets` (`digital_asset_hash_sha256`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_digital_assets_class_mask`
ON `digital_assets` (`digital_asset_class_mask`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_digital_assets_visibility_mask`
ON `digital_assets` (`digital_asset_visibility_mask`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_digital_assets_replication_policy_id`
ON `digital_assets` (`digital_asset_replication_policy_id`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_digital_assets_backup_policy_id`
ON `digital_assets` (`digital_asset_backup_policy_id`);

-- BREAK
-- BREAK

-- -----------------------------------------------------
-- Table `composite_digital_assets`
-- One row per logical multipart assembly of atomic digital assets.
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `composite_digital_assets` (
  `composite_digital_asset_id` INTEGER PRIMARY KEY,

  `composite_digital_asset_name` TEXT NULL,
  `composite_digital_asset_media_type` TEXT NULL,
  `composite_digital_asset_media_category` TEXT NULL,
  `composite_digital_asset_source` TEXT NULL,

  `composite_digital_asset_replication_policy_id` INTEGER NULL,
  `composite_digital_asset_backup_policy_id` INTEGER NULL,

  `composite_digital_asset_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `composite_digital_asset_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `composite_digital_asset_source_created_datestamp_ep_k` INTEGER NULL,
  `composite_digital_asset_source_modified_datestamp_ep_k` INTEGER NULL,

  `composite_digital_asset_scratch` TEXT NULL,

  CONSTRAINT `composite_digital_asset_replication_policy_fk`
    FOREIGN KEY (`composite_digital_asset_replication_policy_id`)
    REFERENCES `replication_policies` (`replication_policy_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `composite_digital_asset_backup_policy_fk`
    FOREIGN KEY (`composite_digital_asset_backup_policy_id`)
    REFERENCES `backup_policies` (`backup_policy_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE
);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_composite_digital_assets_replication_policy_id`
ON `composite_digital_assets` (`composite_digital_asset_replication_policy_id`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_composite_digital_assets_backup_policy_id`
ON `composite_digital_assets` (`composite_digital_asset_backup_policy_id`);

-- BREAK
-- BREAK

-- -----------------------------------------------------
-- Table `asset_replicas`
-- One row per physical copy of one managed digital asset on one store.
-- Canonical locator: (asset_replica_store_id, asset_replica_storage_key)
-- asset_replica_storage_key is RELATIVE to stores.store_root_uri.
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `asset_replicas` (
  `asset_replica_id` INTEGER PRIMARY KEY,

  `asset_replica_digital_asset_id` INTEGER NULL,
  `asset_replica_store_id` INTEGER NULL,
  `asset_replica_folder_id` INTEGER NULL,

  `asset_replica_storage_key` TEXT NULL,
  `asset_replica_mode` TEXT NOT NULL DEFAULT 'active',

  `asset_replica_name` TEXT NULL,
  `asset_replica_base_name` TEXT NULL,
  `asset_replica_extension` TEXT NULL,

  `asset_replica_presence_status` TEXT NULL,
  `asset_replica_integrity_status` TEXT NULL,
  `asset_replica_last_seen_timestamp_ep_k` INTEGER NULL,
  `asset_replica_last_integrity_check_timestamp_ep_k` INTEGER NULL,

  `asset_replica_observed_size_bytes` INTEGER NULL,
  `asset_replica_observed_hash_sha256` TEXT NULL,
  `asset_replica_observed_hash_blake3` TEXT NULL,
  `asset_replica_failure_reason` TEXT NULL,

  `asset_replica_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `asset_replica_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `asset_replica_source_created_datestamp_ep_k` INTEGER NULL,
  `asset_replica_source_modified_datestamp_ep_k` INTEGER NULL,

  `asset_replica_scratch` TEXT NULL,

  CONSTRAINT `asset_replica_digital_asset_fk`
    FOREIGN KEY (`asset_replica_digital_asset_id`)
    REFERENCES `digital_assets` (`digital_asset_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `asset_replica_store_fk`
    FOREIGN KEY (`asset_replica_store_id`)
    REFERENCES `stores` (`store_id`)
    ON DELETE RESTRICT
    ON UPDATE CASCADE,

  CONSTRAINT `asset_replica_folder_fk`
    FOREIGN KEY (`asset_replica_folder_id`)
    REFERENCES `folders` (`folder_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `asset_replica_mode_check`
    CHECK (`asset_replica_mode` IN ('active', 'backup', 'archive', 'cache', 'transient', 'unmanaged'))
);

-- BREAK
-- BREAK

CREATE UNIQUE INDEX IF NOT EXISTS `idx_asset_replicas_unique_store_key`
ON `asset_replicas` (`asset_replica_store_id`, `asset_replica_storage_key`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_asset_replicas_digital_asset_id`
ON `asset_replicas` (`asset_replica_digital_asset_id`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_asset_replicas_store_id`
ON `asset_replicas` (`asset_replica_store_id`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_asset_replicas_folder_id`
ON `asset_replicas` (`asset_replica_folder_id`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_asset_replicas_mode`
ON `asset_replicas` (`asset_replica_mode`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_asset_replicas_observed_hash_sha256`
ON `asset_replicas` (`asset_replica_observed_hash_sha256`);

-- BREAK
