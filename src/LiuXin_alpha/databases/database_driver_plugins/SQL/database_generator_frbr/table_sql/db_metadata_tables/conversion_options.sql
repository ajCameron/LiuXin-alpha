-- BREAK

-- -----------------------------------------------------
-- Table `conversion_options`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `conversion_options` (
  `conversion_option_id` INTEGER PRIMARY KEY,

  `conversion_option_format` TEXT NOT NULL COLLATE NOCASE,

  `conversion_option_book` INTEGER,
  `conversion_option_data` BLOB NOT NULL,

  -- timestamps (display DATETIME + epoch_ms source)
  conversion_option_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  conversion_option_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  conversion_option_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  conversion_option_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),


  UNIQUE(conversion_option_format,conversion_option_book))
;


-- BREAK