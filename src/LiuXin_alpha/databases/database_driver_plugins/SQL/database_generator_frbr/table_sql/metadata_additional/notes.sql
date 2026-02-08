-- BREAK

-- -----------------------------------------------------
-- Table `notes`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `notes` (
  `note_id` INTEGER PRIMARY KEY,

  `note` TEXT NULL,

  `note_datestamp` INTEGER  DEFAULT (STRFTIME('%s','now')),
  `note_created_datestamp` INTEGER  DEFAULT (STRFTIME('%s','now')),

  -- timestamps (display DATETIME + epoch_ms source)
  `note_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `note_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `note_scratch` TEXT NULL

);

-- BREAK