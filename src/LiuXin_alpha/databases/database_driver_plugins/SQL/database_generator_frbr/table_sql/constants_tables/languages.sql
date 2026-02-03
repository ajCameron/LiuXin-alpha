-- BREAK



-- -----------------------------------------------------
-- Table `languages`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `languages` (
  `language_id` INTEGER PRIMARY KEY,

  `language` TEXT NULL,
  `language_code` TEXT NULL,

  -- timestamps (display DATETIME + epoch_ms source)
  `language_created_timestamp` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `language_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `language_modified_timestamp` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `language_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `language_scratch` TEXT NULL
)
;


-- BREAK