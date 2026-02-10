-- BREAK

-- -----------------------------------------------------
-- Table `database_version`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `database_version` (
  `database_version_id` TEXT PRIMARY KEY,

  `database_version_version` TEXT NULL,

  -- timestamps
  `database_version_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `database_version_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  `database_version_source_created_datestamp_ep_k` INTEGER NULL,
  `database_version_source_modified_datestamp_ep_k` INTEGER NULL,

    )
;


-- BREAK