-- BREAK - FOR CALIBRE EMULATION

-- -----------------------------------------------------
-- Table `books_plugin_data`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `books_plugin_data` (
  `book_plugin_data_id` INTEGER PRIMARY KEY ,

  `book_plugin_data_book` INT UNSIGNED NULL,

  `book_plugin_data_name` TEXT NULL,
  `book_plugin_data_val` TEXT NULL,

  -- timestamps (display DATETIME + epoch_ms source)
  book_plugin_data_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  book_plugin_data_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  book_plugin_data_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  book_plugin_data_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `book_plugin_scratch` TEXT NULL)
;

-- BREAK