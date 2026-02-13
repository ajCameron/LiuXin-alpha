-- BREAK

-- -----------------------------------------------------
-- Table `genres`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `genres` (
  `genre_id` INTEGER PRIMARY KEY,

  `genre` TEXT NULL,
  `genre_sort` TEXT NULL,
  `genre_phash` TEXT NULL,

  `genre_parent_id` INTEGER NULL,
  `genre_position` INTEGER NULL,
  `genre_tree_id` INTEGER NULL,
  `genre_full` TEXT NULL,


  -- timestamps (epoch_ms)
  `genre_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `genre_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `genre_source_created_datestamp_ep_k` INTEGER NULL,
  `genre_source_modified_datestamp_ep_k` INTEGER NULL,

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
-- BREAK

CREATE UNIQUE INDEX IF NOT EXISTS `idx_genres_unique_parent_sort`
ON `genres` (`genre_parent_id`, `genre_sort` COLLATE NOCASE)
WHERE `genre_sort` IS NOT NULL;

-- BREAK
-- BREAK

CREATE UNIQUE INDEX IF NOT EXISTS `idx_genres_unique_parent_name`
ON `genres` (`genre_parent_id`, `genre` COLLATE NOCASE)
WHERE `genre` IS NOT NULL;

-- BREAK
-- BREAK

CREATE UNIQUE INDEX IF NOT EXISTS `idx_genres_unique_full`
ON `genres` (`genre_full` COLLATE NOCASE)
WHERE `genre_full` IS NOT NULL;

-- BREAK
