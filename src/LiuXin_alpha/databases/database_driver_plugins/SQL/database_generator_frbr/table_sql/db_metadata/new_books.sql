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

  `new_book_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `new_book_created_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),

    `new_book_scratch` TEXT NULL)
;


-- BREAK