-- BREAK



-- -----------------------------------------------------
-- Table `languages`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `languages` (
  `language_id` INTEGER PRIMARY KEY ,

  `language` TEXT NULL,
  `language_code` TEXT NULL,

  `language_created_datestamp` DATETIME DEFAULT (STRFTIME('%s','now')),

  `language_scratch` TEXT NULL
)
;


-- BREAK