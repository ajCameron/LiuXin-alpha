
-- BREAK

-- -----------------------------------------------------
-- Table `database_metadata`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `database_metadata` (
  `database_metadata_id` INTEGER PRIMARY KEY,
  `database_metadata_unique_id` TEXT NULL,

  `database_metadata_parent_LiuXin_instance` TEXT NULL,

  `database_metadata_db_name` TEXT NULL,

  `database_metadata_datestamp` DATETIME DEFAULT (STRFTIME('%s','now')),
  `database_metadata_created_datestamp` DATETIME DEFAULT (STRFTIME('%s','now')),

    `database_metadata_scratch` TEXT NULL)
;


-- BREAK