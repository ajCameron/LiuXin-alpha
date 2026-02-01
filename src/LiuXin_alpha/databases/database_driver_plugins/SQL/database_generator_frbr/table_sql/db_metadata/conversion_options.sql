-- BREAK

-- -----------------------------------------------------
-- Table `conversion_options`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `conversion_options` (
  `conversion_option_id` INTEGER PRIMARY KEY,

  `conversion_option_format` TEXT NOT NULL COLLATE NOCASE,

  `conversion_option_book` INTEGER,
  `conversion_option_data` BLOB NOT NULL,

  `conversion_option_datestamp` DATETIME DEFAULT (STRFTIME('%s','now')),
  `conversion_option_created_datestamp` DATETIME DEFAULT (STRFTIME('%s','now')),


  UNIQUE(conversion_option_format,conversion_option_book))
;


-- BREAK