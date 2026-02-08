-- LiuXin-alpha (FRBR + Storage) : Main tables only
-- Notes:
--  - SQLite DDL
--  - "Main tables" only: no lookup/type tables, no subtables, no link tables
--  - Foreign keys require PRAGMA foreign_keys=ON per connection
--  - Use the -- BREAK markers as section boundaries

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
  `file_item_id`   INTEGER NULL,
  `file_store_id`  INTEGER NOT NULL,
  `file_folder_id` INTEGER NULL,

  -- Locator (authoritative, RELATIVE)
  `file_storage_key` TEXT NOT NULL,

  -- Naming (UI / compatibility)
  `file_name` TEXT NULL,
  `file_base_name` TEXT NULL,
  `file_extension` TEXT NULL,
  `file_tag` TEXT NULL,
  `file_auto_name` TEXT NULL,
  `file_use_auto_name` INTEGER DEFAULT 1,

  -- Type / role / classification
  `file_mime_type` TEXT NULL,
  `file_role` TEXT NULL,                 -- 'content', 'cover', 'aux', 'parity', 'index', ...
  `file_media_category` TEXT NULL,       -- 'ebook_text','scan','cover','video','audio','metadata', ...
  `file_class_mask` INTEGER NULL,        -- placement/category bitmask
  `file_visibility_mask` INTEGER NULL,   -- privacy/visibility bitmask
  `file_critical` INTEGER NULL DEFAULT 1,

  -- Size / integrity
  `file_size_bytes` INTEGER NULL,
  `file_hash_sha256` TEXT NULL,
  `file_hash_blake3` TEXT NULL,
  `file_phash` TEXT NULL,

  `file_corrupt` INTEGER NULL,
  `file_integrity_status` TEXT NULL,     -- 'ok','missing','hash_mismatch','pending','unknown'
  `file_last_seen_at` DATETIME NULL,
  `file_last_integrity_check` DATETIME NULL,

  -- Provenance / ingestion
  `file_acquired_timestamp` TEXT NULL,
  `file_source` TEXT NULL,
  `file_original_name` TEXT NULL,
  `file_original_path` TEXT NULL,

  -- Processing / lineage placeholders
  `file_anthology` INTEGER NULL,
  `file_parent` TEXT NULL,
  `file_conversion_settings` TEXT NULL,
  `file_processed` INTEGER NULL DEFAULT 0,

  -- timestamps (display DATETIME + epoch_ms source)
  `file_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `file_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

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


-- Uniqueness: no duplicate key within the same store
CREATE UNIQUE INDEX IF NOT EXISTS `idx_files_unique_store_key`
ON `files` (`file_store_id`, `file_storage_key`);

-- BREAK
-- BREAK

-- Common joins
CREATE INDEX IF NOT EXISTS `idx_files_item_id`
ON `files` (`file_item_id`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_files_folder_id`
ON `files` (`file_folder_id`);

-- BREAK
-- BREAK


-- Integrity / dedupe
CREATE INDEX IF NOT EXISTS `idx_files_hash_sha256`
ON `files` (`file_hash_sha256`);

-- BREAK
-- BREAK


-- Policy filters
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
