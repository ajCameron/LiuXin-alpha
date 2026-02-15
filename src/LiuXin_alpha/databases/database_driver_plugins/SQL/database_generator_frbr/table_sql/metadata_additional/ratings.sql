-- BREAK

-- -----------------------------------------------------
-- Table `ratings`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `ratings` (
  `rating_id` INTEGER PRIMARY KEY,

  `rating` FLOAT NULL,
  `rating_out_of` INT NULL, -- Is the rating out of five or ten? (might be a bad idea - just normalize)
  `rating_for_calibre_tag_viewer` INT NULL, -- Just cast the rating to float.

  `rating_source` TEXT NULL,


      -- timestamps (epoch_ms)
  `rating_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `rating_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `rating_source_created_datestamp_ep_k` INTEGER NULL,
  `rating_source_modified_datestamp_ep_k` INTEGER NULL,

  `rating_scratch` TEXT NULL

);

-- BREAK