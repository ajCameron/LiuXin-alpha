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

  `series_over_author` INTEGER DEFAULT 0,

  `series_parent_id` INTEGER NULL,
  `series_parent_position` INTEGER NULL,
  `series_tree_id` TEXT NULL,
  `series_full` TEXT NULL,


  -- timestamps (epoch_ms)
  `series_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `series_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `series_source_created_datestamp_ep_k` INTEGER NULL,
  `series_source_modified_datestamp_ep_k` INTEGER NULL,

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
-- BREAK

CREATE UNIQUE INDEX IF NOT EXISTS `idx_series_unique_name_norm`
ON `series` (`series_name_norm`)
WHERE `series_name_norm` IS NOT NULL;

-- BREAK
-- BREAK

CREATE UNIQUE INDEX IF NOT EXISTS `idx_series_unique_full`
ON `series` (`series_full` COLLATE NOCASE)
WHERE `series_full` IS NOT NULL;

-- BREAK
