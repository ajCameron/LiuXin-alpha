-- BREAK - FOR CALIBRE EMULATION

-- -----------------------------------------------------
-- Table `books_plugin_data`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `books_plugin_data` (
  `book_plugin_data_id` INTEGER PRIMARY KEY ,

  `book_plugin_data_book` INT UNSIGNED NULL,

  `book_plugin_data_name` TEXT NULL,
  `book_plugin_data_val` TEXT NULL,

  `book_plugin_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `book_plugin_created_datestamp` DATETIME DEFAULT (STRFTIME('%s','now')),

    `book_plugin_scratch` TEXT NULL)
;

-- BREAK