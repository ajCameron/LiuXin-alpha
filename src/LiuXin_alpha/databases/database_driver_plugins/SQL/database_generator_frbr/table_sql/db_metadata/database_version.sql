-- BREAK

-- -----------------------------------------------------
-- Table `database_version`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `database_version` (
  `database_version_id` TEXT PRIMARY KEY,

  `database_version_version` TEXT NULL,

  `database_version_datestamp` DATETIME DEFAULT (STRFTIME('%s','now')),
  `database_version_created_datestamp` DATETIME DEFAULT (STRFTIME('%s','now'))

    )
;


-- BREAK