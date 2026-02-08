-- BREAK

-- -----------------------------------------------------
-- Table `ratings`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `ratings` (
  `rating_id` INTEGER PRIMARY KEY,

  `rating` INT NULL,
  `rating_source` TEXT NULL,

  `rating_datestamp`  INTEGER  DEFAULT (STRFTIME('%s','now')),

      -- timestamps (display DATETIME + epoch_ms source)
  `rating_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `rating_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `rating_scratch` TEXT NULL

);

-- BREAK