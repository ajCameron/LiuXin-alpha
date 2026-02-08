-- BREAK

-- -----------------------------------------------------
-- Table `series`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `series` (
  `series_id` INTEGER PRIMARY KEY ,

  `series` TEXT NULL,
  `series_name_norm` TEXT NULL,
  `series_sort` TEXT NULL,
  `series_phash` TEXT NULL,

  `series_over_author` INT DEFAULT 0,

  `series_parent_id` INT UNSIGNED NULL,
  `series_parent_position` INT UNSIGNED NULL,
  `series_tree_id` TEXT NULL,
  `series_full` TEXT NULL,

  `series_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,

  -- timestamps (display DATETIME + epoch_ms source)
  `series_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `series_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `series_scratch` TEXT NULL,

  CONSTRAINT `series_unique` UNIQUE (`series`),
  CONSTRAINT `series_parent_id`
    FOREIGN KEY (`series_parent_id`)
    REFERENCES `series` (`series_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE

);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `series_parent_index`
ON `series` (`series_parent_id`);

-- BREAK