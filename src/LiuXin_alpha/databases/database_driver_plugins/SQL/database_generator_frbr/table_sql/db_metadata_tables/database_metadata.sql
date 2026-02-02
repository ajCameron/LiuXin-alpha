
-- BREAK

-- -----------------------------------------------------
-- Table `database_metadata`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `database_metadata` (
  `database_metadata_id` INTEGER PRIMARY KEY,
  `database_metadata_unique_id` TEXT NULL,

  `database_metadata_parent_LiuXin_instance` TEXT NULL,

  `database_metadata_db_name` TEXT NULL,

  -- timestamps (display DATETIME + epoch_ms source)
  database_metadata_created_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  database_metadata_created_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  database_metadata_modified_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  database_metadata_modified_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `database_metadata_scratch` TEXT NULL)
;


-- BREAK