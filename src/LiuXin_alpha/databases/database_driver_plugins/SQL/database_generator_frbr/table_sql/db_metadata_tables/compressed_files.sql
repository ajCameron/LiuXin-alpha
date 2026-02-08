-- BREAK

-- -----------------------------------------------------
-- Table `compressed_files`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `compressed_files` (
  `compressed_file_id` INTEGER PRIMARY KEY,

  `compressed_file_name` TEXT,
  `compressed_file_extension` INTEGER NULL DEFAULT 0,
  `compressed_file_path` INTEGER NULL,

  `compressed_file_hash_1` TEXT NULL,
  `compressed_file_hash_2` INTEGER,

  `compressed_file_size` INTEGER,
  `compressed_file_group_id` INTEGER,

  `compressed_file_folder` INTEGER,

  `compressed_file_cached` INTEGER NULL DEFAULT 0,

  `compressed_file_cache_attempted` INTEGER NULL DEFAULT 0,

  -- timestamps (display DATETIME + epoch_ms source)
  `compressed_file_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `compressed_file_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `compressed_file_scratch` TEXT NULL)
;


-- BREAK