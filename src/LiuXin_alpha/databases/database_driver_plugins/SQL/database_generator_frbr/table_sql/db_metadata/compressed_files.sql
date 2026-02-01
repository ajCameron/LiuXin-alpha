-- BREAK

-- -----------------------------------------------------
-- Table `compressed_files`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `compressed_files` (
  `compressed_file_id` INTEGER PRIMARY KEY,

  `compressed_file_name` TEXT,
  `compressed_file_extension` INT NULL DEFAULT 0,
  `compressed_file_path` INT UNSIGNED NULL,

  `compressed_file_hash_1` TEXT NULL,
  `compressed_file_hash_2` INTEGER,

  `compressed_file_size` INTEGER,
  `compressed_file_group_id` INTEGER,

  `compressed_file_folder` INTEGER,

  `compressed_file_cached` INT NULL DEFAULT 0,

  `compressed_file_cache_attempted` INT NULL DEFAULT 0,

  `compressed_file_datestamp` DATETIME DEFAULT (STRFTIME('%s','now')),
  `compressed_file_created_datestamp` DATETIME DEFAULT (STRFTIME('%s','now')),

    `compressed_file_scratch` TEXT NULL)
;


-- BREAK