-- BREAK

-- -----------------------------------------------------
-- Table `new_books`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `new_books` (
  `new_book_id` INTEGER PRIMARY KEY,

  `new_book_name` TEXT,
  `new_book_extension` INT NULL DEFAULT 0,

  `new_book_path` INT UNSIGNED NULL,

  `new_book_hash_1` TEXT NULL,
  `new_book_hash_2` INTEGER,
  `new_book_size` INTEGER,

  `new_book_group_id` INTEGER,

  `new_book_cached` INT NULL DEFAULT 0,
  `new_book_cache_attempted` INT NULL DEFAULT 0,

  -- timestamps (display DATETIME + epoch_ms source)
  new_book_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  new_book_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  new_book_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  new_book_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

    `new_book_scratch` TEXT NULL)
;


-- BREAK