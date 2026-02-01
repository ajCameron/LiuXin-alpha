-- LiuXin-alpha (FRBR + Storage) : Main tables only
-- Notes:
--  - SQLite DDL
--  - "Main tables" only: no lookup/type tables, no subtables, no link tables
--  - Foreign keys require PRAGMA foreign_keys=ON per connection
--  - Use the -- BREAK markers as section boundaries







-- =====================================================
-- STORAGE (Stores / Folders / Files)
-- =====================================================

-- -----------------------------------------------------
-- Table `stores`
-- Logical storage backends (filesystem, NAS, tape, rclone, http_ro, etc.)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `stores` (
  `store_id` INTEGER PRIMARY KEY,

  -- Identity / addressing
  `store_name` TEXT NOT NULL,
  `store_kind` TEXT NOT NULL,           -- 'filesystem', 'nas', 'tape', 'rclone', 'http_ro', ...
  `store_access_protocol` TEXT NULL,    -- 'file', 'nfs', 'smb', 'tape', 'rclone', 'http', 'https', ...
  `store_root_uri` TEXT NULL,           -- base locator (may change); file keys are relative to this

  -- Access / auth
  `store_auth_method` TEXT NULL,        -- 'none', 'env', 'keyring', 'config_file', 'embedded', ...
  `store_credentials` TEXT NULL,        -- encrypted blob or reference (ENV:/KEYRING:/PATH:...), not plaintext

  -- Storage policy
  `store_storage_mask` INTEGER NULL,    -- broad classification bitmask
  `store_policy_json` TEXT NULL,        -- fine-grained 'block'/'prefer' rules

  -- State / notes
  `store_online_status` TEXT NULL,      -- 'online', 'offline', 'retired'
  `store_location_note` TEXT NULL,

  -- Telemetry
  `store_last_seen_online` DATETIME NULL,
  `store_last_healthcheck_ok` DATETIME NULL,

  -- Capabilities
  `store_supports_folders` INT NOT NULL DEFAULT 1,
  `store_supports_hierarchical_list` INT NOT NULL DEFAULT 1,

  `store_supports_random_read` INT NOT NULL DEFAULT 1,
  `store_supports_random_write` INT NOT NULL DEFAULT 1,
  `store_supports_append` INT NOT NULL DEFAULT 1,

  `store_supports_atomic_rename` INT NOT NULL DEFAULT 1,
  `store_supports_atomic_overwrite` INT NOT NULL DEFAULT 1,

  `store_supports_delete` INT NOT NULL DEFAULT 1,
  `store_is_read_only` INT NOT NULL DEFAULT 0,

  `store_is_eventually_consistent` INT NOT NULL DEFAULT 0,

  `store_supports_checksums` INT NOT NULL DEFAULT 0,
  `store_supports_immutable_objects` INT NOT NULL DEFAULT 0,
  `store_supports_snapshots` INT NOT NULL DEFAULT 0,
  `store_supports_server_side_encryption` INT NOT NULL DEFAULT 0,

  `store_supports_parallel_read` INT NOT NULL DEFAULT 1,
  `store_supports_parallel_write` INT NOT NULL DEFAULT 1,

  `store_requires_mount` INT NOT NULL DEFAULT 0,
  `store_latency_class` TEXT NULL,      -- 'hot','warm','cold','glacial'

  -- Timestamps
  `store_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `store_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `store_last_modified` DATETIME DEFAULT CURRENT_TIMESTAMP,

  `store_scratch` TEXT NULL
);
-- BREAK


-- -----------------------------------------------------
-- Table `folders`
-- Hierarchical folders within a store (optional; governed by store capability + triggers)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `folders` (
  `folder_id` INTEGER PRIMARY KEY,

  `folder_store_id` INT NOT NULL,
  `folder_parent_id` INT NULL,

  `folder_name` TEXT NOT NULL,          -- single path segment
  `folder_relpath` TEXT NULL,           -- cached relative path inside store

  `folder_policy_json` TEXT NULL,       -- optional overrides for store policy
  `folder_last_seen_at` DATETIME NULL,  -- telemetry

  `folder_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `folder_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `folder_last_modified` DATETIME DEFAULT CURRENT_TIMESTAMP,

  `folder_scratch` TEXT NULL,

  CONSTRAINT `folder_store_fk`
    FOREIGN KEY (`folder_store_id`)
    REFERENCES `stores` (`store_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `folder_parent_fk`
    FOREIGN KEY (`folder_parent_id`)
    REFERENCES `folders` (`folder_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS `idx_folders_unique_sibling_name`
ON `folders` (`folder_store_id`, `folder_parent_id`, `folder_name`);

CREATE UNIQUE INDEX IF NOT EXISTS `idx_folders_unique_relpath_per_store`
ON `folders` (`folder_store_id`, `folder_relpath`);
-- BREAK


-- -----------------------------------------------------
-- Table `files`
-- Stored binary objects, optionally tied to Items
-- Canonical locator: (file_store_id, file_storage_key)
-- file_storage_key is RELATIVE to stores.store_root_uri
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `files` (
  `file_id` INTEGER PRIMARY KEY,

  -- Relations
  `file_item_id`   INT NULL,
  `file_store_id`  INT NOT NULL,
  `file_folder_id` INT NULL,

  -- Locator (authoritative, RELATIVE)
  `file_storage_key` TEXT NOT NULL,

  -- Naming (UI / compatibility)
  `file_name` TEXT NULL,
  `file_base_name` TEXT NULL,
  `file_extension` TEXT NULL,
  `file_tag` TEXT NULL,
  `file_auto_name` TEXT NULL,
  `file_use_auto_name` INT DEFAULT 1,

  -- Type / role / classification
  `file_mime_type` TEXT NULL,
  `file_role` TEXT NULL,                 -- 'content', 'cover', 'aux', 'parity', 'index', ...
  `file_media_category` TEXT NULL,       -- 'ebook_text','scan','cover','video','audio','metadata', ...
  `file_class_mask` INTEGER NULL,        -- placement/category bitmask
  `file_visibility_mask` INTEGER NULL,   -- privacy/visibility bitmask
  `file_critical` TINYINT(1) NULL DEFAULT 1,

  -- Size / integrity
  `file_size_bytes` INTEGER NULL,
  `file_hash_sha256` TEXT NULL,
  `file_hash_blake3` TEXT NULL,
  `file_phash` TEXT NULL,

  `file_corrupt` INT UNSIGNED NULL,
  `file_integrity_status` TEXT NULL,     -- 'ok','missing','hash_mismatch','pending','unknown'
  `file_last_seen_at` DATETIME NULL,
  `file_last_integrity_check` DATETIME NULL,

  -- Provenance / ingestion
  `file_acquired_timestamp` TEXT NULL,
  `file_source` TEXT NULL,
  `file_original_name` TEXT NULL,
  `file_original_path` TEXT NULL,

  -- Processing / lineage placeholders
  `file_anthology` TINYINT(1) NULL,
  `file_parent` TEXT NULL,
  `file_conversion_settings` TEXT NULL,
  `file_processed` TINYINT(1) NULL DEFAULT 0,

  -- Timestamps
  `file_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `file_datestamp`         DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `file_last_modified`     DATETIME DEFAULT CURRENT_TIMESTAMP,

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

-- Uniqueness: no duplicate key within the same store
CREATE UNIQUE INDEX IF NOT EXISTS `idx_files_unique_store_key`
ON `files` (`file_store_id`, `file_storage_key`);

-- Common joins
CREATE INDEX IF NOT EXISTS `idx_files_item_id`
ON `files` (`file_item_id`);

CREATE INDEX IF NOT EXISTS `idx_files_folder_id`
ON `files` (`file_folder_id`);

-- Integrity / dedupe
CREATE INDEX IF NOT EXISTS `idx_files_hash_sha256`
ON `files` (`file_hash_sha256`);

-- Policy filters
CREATE INDEX IF NOT EXISTS `idx_files_class_mask`
ON `files` (`file_class_mask`);

CREATE INDEX IF NOT EXISTS `idx_files_visibility_mask`
ON `files` (`file_visibility_mask`);

CREATE INDEX IF NOT EXISTS `idx_files_role`
ON `files` (`file_role`);
-- BREAK
