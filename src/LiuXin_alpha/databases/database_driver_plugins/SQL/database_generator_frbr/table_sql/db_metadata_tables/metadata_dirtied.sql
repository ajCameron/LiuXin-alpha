-- BREAK

-- -----------------------------------------------------
-- Table `metadata_dirtied_books`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `metadata_dirtied_books` (
  `metadata_dirtied_id` TEXT PRIMARY KEY,

  `metadata_dirtied_table_id` INT NULL,
  `metadata_dirtied_table` TEXT NULL,
  `metadata_drtied_reason` TEXT NULL,

  -- timestamps (display DATETIME + epoch_ms source)
  `metadata_dirtied_created_timestamp` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `metadata_dirtied_modified_timestamp` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  `metadata_dirtied_scratch` TEXT NULL

)
;


-- BREAK