-- BREAK

-- -----------------------------------------------------
-- Table `metadata_dirtied_books`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `metadata_dirtied_books` (
  `metadata_dirtied_id` TEXT PRIMARY KEY,

  `metadata_dirtied_datestamp` DATETIME DEFAULT (STRFTIME('%s','now')),
  `metadata_dirtied_created_datestamp` DATETIME DEFAULT (STRFTIME('%s','now')),

  `metadata_dirtied_table_id` INT NULL,
  `metadata_dirtied_table` TEXT NULL,

    `metadata_dirtied_scratch` TEXT NULL

)
;


-- BREAK