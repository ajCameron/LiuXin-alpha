-- BREAK

-- -----------------------------------------------------
-- Table `digital_assets`
-- One row per managed digital asset.
-- Atomic assets represent one byte-bearing managed payload.
-- Composite assets represent an ordered logical assembly of other assets.
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `digital_assets` (
  `digital_asset_id` INTEGER PRIMARY KEY,

  `digital_asset_kind` TEXT NULL DEFAULT 'atomic', -- 'atomic' | 'composite'

  -- Naming / descriptive hints
  `digital_asset_name` TEXT NULL,
  `digital_asset_base_name` TEXT NULL,
  `digital_asset_extension` TEXT NULL,
  `digital_asset_tag` TEXT NULL,
  `digital_asset_auto_name` TEXT NULL,
  `digital_asset_use_auto_name` INTEGER DEFAULT 1,

  -- Type / role / classification
  `digital_asset_mime_type` TEXT NULL,
  `digital_asset_role` TEXT NULL,
  `digital_asset_media_category` TEXT NULL,
  `digital_asset_class_mask` INTEGER NULL,
  `digital_asset_visibility_mask` INTEGER NULL,
  `digital_asset_critical` INTEGER NULL DEFAULT 1,

  -- Size / integrity (normally for atomic assets; composites may leave these NULL)
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

  -- Processing / lineage placeholders
  `digital_asset_conversion_settings` TEXT NULL,
  `digital_asset_processed` INTEGER NULL DEFAULT 0,

  -- timestamps (epoch_ms)
  `digital_asset_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `digital_asset_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `digital_asset_source_created_datestamp_ep_k` INTEGER NULL,
  `digital_asset_source_modified_datestamp_ep_k` INTEGER NULL,

  `digital_asset_scratch` TEXT NULL,

  CONSTRAINT `digital_asset_kind_check`
    CHECK (`digital_asset_kind` IS NULL OR `digital_asset_kind` IN ('atomic', 'composite'))
);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_digital_assets_hash_sha256`
ON `digital_assets` (`digital_asset_hash_sha256`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_digital_assets_kind`
ON `digital_assets` (`digital_asset_kind`);

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

CREATE INDEX IF NOT EXISTS `idx_digital_assets_role`
ON `digital_assets` (`digital_asset_role`);

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
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `asset_replica_folder_fk`
    FOREIGN KEY (`asset_replica_folder_id`)
    REFERENCES `folders` (`folder_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE
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

CREATE INDEX IF NOT EXISTS `idx_asset_replicas_folder_id`
ON `asset_replicas` (`asset_replica_folder_id`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_asset_replicas_observed_hash_sha256`
ON `asset_replicas` (`asset_replica_observed_hash_sha256`);

-- BREAK
-- BREAK

-- -----------------------------------------------------
-- Table `digital_asset_compositions`
-- Ordered membership links for composite digital assets.
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `digital_asset_compositions` (
  `digital_asset_composition_id` INTEGER PRIMARY KEY,

  `digital_asset_composition_parent_asset_id` INTEGER NULL,
  `digital_asset_composition_member_asset_id` INTEGER NULL,
  `digital_asset_composition_sequence_number` INTEGER NULL,
  `digital_asset_composition_role` TEXT NULL,
  `digital_asset_composition_label` TEXT NULL,
  `digital_asset_composition_is_required` INTEGER NOT NULL DEFAULT 1,

  `digital_asset_composition_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `digital_asset_composition_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `digital_asset_composition_source_created_datestamp_ep_k` INTEGER NULL,
  `digital_asset_composition_source_modified_datestamp_ep_k` INTEGER NULL,

  `digital_asset_composition_scratch` TEXT NULL,

  CONSTRAINT `digital_asset_composition_parent_fk`
    FOREIGN KEY (`digital_asset_composition_parent_asset_id`)
    REFERENCES `digital_assets`(`digital_asset_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `digital_asset_composition_member_fk`
    FOREIGN KEY (`digital_asset_composition_member_asset_id`)
    REFERENCES `digital_assets`(`digital_asset_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `digital_asset_composition_no_self`
    CHECK (`digital_asset_composition_parent_asset_id` != `digital_asset_composition_member_asset_id`),

  CONSTRAINT `digital_asset_composition_required_bool`
    CHECK (`digital_asset_composition_is_required` IN (0,1))
);

-- BREAK
-- BREAK

CREATE UNIQUE INDEX IF NOT EXISTS `idx_digital_asset_compositions_parent_member`
ON `digital_asset_compositions` (`digital_asset_composition_parent_asset_id`, `digital_asset_composition_member_asset_id`);

-- BREAK
-- BREAK

CREATE UNIQUE INDEX IF NOT EXISTS `idx_digital_asset_compositions_parent_sequence`
ON `digital_asset_compositions` (`digital_asset_composition_parent_asset_id`, `digital_asset_composition_sequence_number`);

-- BREAK
