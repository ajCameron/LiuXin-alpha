-- BREAK

-- -----------------------------------------------------
-- Table `database_version`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `database_version` (
  `database_version_id` TEXT PRIMARY KEY,

  `database_version_version` TEXT NULL,

  -- timestamps (display DATETIME + epoch_ms source)
  database_version_created_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  database_version_created_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  database_version_modified_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  database_version_modified_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

    )
;


-- BREAK