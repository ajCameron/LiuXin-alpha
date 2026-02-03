-- BREAK

-- -----------------------------------------------------
-- Table `metadata_dirtied_books`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `metadata_dirtied_books` (
  `metadata_dirtied_id` TEXT PRIMARY KEY,

  `metadata_dirtied_table_id` INT NULL,
  `metadata_dirtied_table` TEXT NULL,

  -- timestamps (display DATETIME + epoch_ms source)
  `metadata_dirtied_created_timestamp` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `metadata_dirtied_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `metadata_dirtied_modified_timestamp` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `metadata_dirtied_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `metadata_dirtied_scratch` TEXT NULL

)
;


-- BREAK