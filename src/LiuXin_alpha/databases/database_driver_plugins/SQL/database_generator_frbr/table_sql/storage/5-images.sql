-- LiuXin-alpha (FRBR + Storage) : Main tables only
-- Notes:
--  - SQLite DDL
--  - "Main tables" only: no lookup/type tables, no subtables, no link tables
--  - Foreign keys require PRAGMA foreign_keys=ON per connection
--  - Use the -- BREAK markers as section boundaries

-- BREAK


-- -----------------------------------------------------
-- Table `images`
-- Stored binary objects, optionally tied to Items
-- Canonical locator: (image_store_id, image_storage_key)
-- image_storage_key is RELATIVE to stores.store_root_uri
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `images` (
  `image_id` INTEGER PRIMARY KEY,

  -- Relations
  `image_item_id`   INTEGER NULL,
  `image_store_id`  INTEGER NOT NULL,
  `image_folder_id` INTEGER NULL,

  -- Locator (authoritative, RELATIVE)
  `image_storage_key` TEXT NOT NULL,

  -- Naming (UI / compatibility)
  `image_name` TEXT NULL,
  `image_base_name` TEXT NULL,
  `image_extension` TEXT NULL,
  `image_tag` TEXT NULL,
  `image_auto_name` TEXT NULL,
  `image_use_auto_name` INTEGER DEFAULT 1,

  -- Type / role / classification
  `image_mime_type` TEXT NULL,
  `image_role` TEXT NULL,                 -- 'content', 'image', 'aux', 'parity', 'index', ...
  `image_media_category` TEXT NULL,       -- 'ebook_text','scan','image','video','audio','metadata', ...
  `image_class_mask` INTEGER NULL,        -- placement/category bitmask
  `image_visibility_mask` INTEGER NULL,   -- privacy/visibility bitmask
  `image_critical` INTEGER NULL DEFAULT 1,

  -- Size / integrity
  `image_size_bytes` INTEGER NULL,
  `image_hash_sha256` TEXT NULL,
  `image_hash_blake3` TEXT NULL,
  `image_phash` TEXT NULL,

  `image_corrupt` INTEGER NULL,
  `image_integrity_status` TEXT NULL,     -- 'ok','missing','hash_mismatch','pending','unknown'
  `image_last_seen_timestamp_ep_k` INTEGER NULL,
  `image_last_integrity_check_timestamp_ep_k` INTEGER NULL,

  -- Provenance / ingestion
  `image_acquired_timestamp_ep_k` INTEGER NULL,
  `image_source` TEXT NULL,
  `image_original_name` TEXT NULL,
  `image_original_path` TEXT NULL,

  -- Processing / lineage placeholders
  `image_anthology` INTEGER NULL,
  `image_parent` TEXT NULL,
  `image_conversion_settings` TEXT NULL,
  `image_processed` INTEGER NULL DEFAULT 0,

  -- timestamps (epoch_ms)
  `image_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `image_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `image_source_created_datestamp_ep_k` INTEGER NULL,
  `image_source_modified_datestamp_ep_k` INTEGER NULL,

  `image_scratch` TEXT NULL,

  CONSTRAINT `image_item_fk`
    FOREIGN KEY (`image_item_id`)
    REFERENCES `items` (`item_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `image_store_fk`
    FOREIGN KEY (`image_store_id`)
    REFERENCES `stores` (`store_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `image_folder_fk`
    FOREIGN KEY (`image_folder_id`)
    REFERENCES `folders` (`folder_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE

);

-- BREAK
-- BREAK


-- Uniqueness: no duplicate key within the same store
CREATE UNIQUE INDEX IF NOT EXISTS `idx_images_unique_store_key`
ON `images` (`image_store_id`, `image_storage_key`);

-- BREAK
-- BREAK

-- Common joins
CREATE INDEX IF NOT EXISTS `idx_images_item_id`
ON `images` (`image_item_id`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_images_folder_id`
ON `images` (`image_folder_id`);

-- BREAK
-- BREAK


-- Integrity / dedupe
CREATE INDEX IF NOT EXISTS `idx_images_hash_sha256`
ON `images` (`image_hash_sha256`);

-- BREAK
-- BREAK


-- Policy filters
CREATE INDEX IF NOT EXISTS `idx_images_class_mask`
ON `images` (`image_class_mask`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_images_visibility_mask`
ON `images` (`image_visibility_mask`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_images_role`
ON `images` (`image_role`);
-- BREAK
