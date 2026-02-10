-- BREAK

-- -----------------------------------------------------
-- Table `feeds`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `feeds` (
  `feed_id` INTEGER PRIMARY KEY,

  `feed_title` TEXT NULL,

  `feed_script` TEXT NULL,

  -- timestamps (epoch_ms)
  `feed_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `feed_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `feed_source_created_datestamp_ep_k` INTEGER NULL,
  `feed_source_modified_datestamp_ep_k` INTEGER NULL,

  `feed_scratch` TEXT NULL

    );

-- BREAK