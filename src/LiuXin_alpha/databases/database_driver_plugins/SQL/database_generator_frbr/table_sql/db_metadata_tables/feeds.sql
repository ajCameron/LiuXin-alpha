-- BREAK

-- -----------------------------------------------------
-- Table `feeds`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `feeds` (
  `feed_id` INTEGER PRIMARY KEY,

  `feed_title` TEXT NULL,

  `feed_script` TEXT NULL,

  -- timestamps (display DATETIME + epoch_ms source)
  feed_created_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  feed_created_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  feed_modified_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  feed_modified_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `feed_scratch` TEXT NULL

    );

-- BREAK