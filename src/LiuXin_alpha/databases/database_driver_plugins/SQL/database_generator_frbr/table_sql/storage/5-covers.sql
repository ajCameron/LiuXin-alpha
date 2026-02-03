-- LiuXin-alpha (FRBR + Storage) : Main tables only
-- Notes:
--  - SQLite DDL
--  - "Main tables" only: no lookup/type tables, no subtables, no link tables
--  - Foreign keys require PRAGMA foreign_keys=ON per connection
--  - Use the -- BREAK markers as section boundaries

-- BREAK


-- -----------------------------------------------------
-- Table `covers`
-- Stored binary objects, optionally tied to Items
-- Canonical locator: (cover_store_id, cover_storage_key)
-- cover_storage_key is RELATIVE to stores.store_root_uri
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `covers` (
  `cover_id` INTEGER PRIMARY KEY,

  -- Relations
  `cover_item_id`   INT NULL,
  `cover_store_id`  INT NOT NULL,
  `cover_folder_id` INT NULL,

  -- Locator (authoritative, RELATIVE)
  `cover_storage_key` TEXT NOT NULL,

  -- Naming (UI / compatibility)
  `cover_name` TEXT NULL,
  `cover_base_name` TEXT NULL,
  `cover_extension` TEXT NULL,
  `cover_tag` TEXT NULL,
  `cover_auto_name` TEXT NULL,
  `cover_use_auto_name` INT DEFAULT 1,

  -- Type / role / classification
  `cover_mime_type` TEXT NULL,
  `cover_role` TEXT NULL,                 -- 'content', 'cover', 'aux', 'parity', 'index', ...
  `cover_media_category` TEXT NULL,       -- 'ebook_text','scan','cover','video','audio','metadata', ...
  `cover_class_mask` INTEGER NULL,        -- placement/category bitmask
  `cover_visibility_mask` INTEGER NULL,   -- privacy/visibility bitmask
  `cover_critical` TINYINT(1) NULL DEFAULT 1,

  -- Size / integrity
  `cover_size_bytes` INTEGER NULL,
  `cover_hash_sha256` TEXT NULL,
  `cover_hash_blake3` TEXT NULL,
  `cover_phash` TEXT NULL,

  `cover_corrupt` INT UNSIGNED NULL,
  `cover_integrity_status` TEXT NULL,     -- 'ok','missing','hash_mismatch','pending','unknown'
  `cover_last_seen_at` DATETIME NULL,
  `cover_last_integrity_check` DATETIME NULL,

  -- Provenance / ingestion
  `cover_acquired_timestamp` TEXT NULL,
  `cover_source` TEXT NULL,
  `cover_original_name` TEXT NULL,
  `cover_original_path` TEXT NULL,

  -- Processing / lineage placeholders
  `cover_anthology` TINYINT(1) NULL,
  `cover_parent` TEXT NULL,
  `cover_conversion_settings` TEXT NULL,
  `cover_processed` TINYINT(1) NULL DEFAULT 0,

  -- Timestamps
  `cover_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `cover_datestamp`         DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `cover_last_modified`     DATETIME DEFAULT CURRENT_TIMESTAMP,

  -- timestamps (display DATETIME + epoch_ms source)
  `cover_created_timestamp` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `cover_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `cover_modified_timestamp` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `cover_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `cover_scratch` TEXT NULL,

  CONSTRAINT `cover_item_fk`
    FOREIGN KEY (`cover_item_id`)
    REFERENCES `items` (`item_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `cover_store_fk`
    FOREIGN KEY (`cover_store_id`)
    REFERENCES `stores` (`store_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `cover_folder_fk`
    FOREIGN KEY (`cover_folder_id`)
    REFERENCES `folders` (`folder_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE

);

-- BREAK
-- BREAK


-- Uniqueness: no duplicate key within the same store
CREATE UNIQUE INDEX IF NOT EXISTS `idx_covers_unique_store_key`
ON `covers` (`cover_store_id`, `cover_storage_key`);

-- BREAK
-- BREAK

-- Common joins
CREATE INDEX IF NOT EXISTS `idx_covers_item_id`
ON `covers` (`cover_item_id`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_covers_folder_id`
ON `covers` (`cover_folder_id`);

-- BREAK
-- BREAK


-- Integrity / dedupe
CREATE INDEX IF NOT EXISTS `idx_covers_hash_sha256`
ON `covers` (`cover_hash_sha256`);

-- BREAK
-- BREAK


-- Policy filters
CREATE INDEX IF NOT EXISTS `idx_covers_class_mask`
ON `covers` (`cover_class_mask`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_covers_visibility_mask`
ON `covers` (`cover_visibility_mask`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_covers_role`
ON `covers` (`cover_role`);
-- BREAK
