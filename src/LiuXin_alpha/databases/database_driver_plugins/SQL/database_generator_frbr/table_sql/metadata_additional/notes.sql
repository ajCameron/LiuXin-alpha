-- BREAK

-- -----------------------------------------------------
-- Table `notes`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `notes` (
  `note_id` INTEGER PRIMARY KEY,

  `note` TEXT NULL,


  -- timestamps (epoch_ms)
  `note_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `note_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `note_source_created_datestamp_ep_k` INTEGER NULL,
  `note_source_modified_datestamp_ep_k` INTEGER NULL,

  `note_scratch` TEXT NULL

);

-- BREAK