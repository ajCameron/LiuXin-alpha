-- BREAK

-- -----------------------------------------------------
-- Table `genres`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `genres` (
  `genre_id` INTEGER PRIMARY KEY,

  `genre` TEXT NULL,
  `genre_sort` TEXT NULL,
  `genre_phash` TEXT NULL,

  `genre_parent_id` INT UNSIGNED NULL,
  `genre_position` INT UNSIGNED NULL,
  `genre_tree_id` INT UNSIGNED NULL,
  `genre_full` TEXT NULL,

  `genre_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `genre_created_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),

  -- timestamps (display DATETIME + epoch_ms source)
  `genre_created_timestamp` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `genre_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `genre_modified_timestamp` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `genre_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `genre_scratch` TEXT NULL,

  CONSTRAINT `genre_parent_id`
    FOREIGN KEY (`genre_parent_id`)
    REFERENCES `genres` (`genre_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `genre_parent_index`
ON `genres` (`genre_parent_id`);

-- BREAK