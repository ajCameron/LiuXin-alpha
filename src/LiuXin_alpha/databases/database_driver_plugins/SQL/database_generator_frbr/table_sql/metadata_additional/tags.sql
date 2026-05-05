-- BREAK

-- -----------------------------------------------------
-- Table `tags`
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS `tags` (

  `tag_id` INTEGER PRIMARY KEY,

  -- NOTE: kept nullable so DriverWrapper.get_blank_row() can insert a placeholder row.
  -- Application logic can enforce presence later.
  `tag` TEXT NULL,
  `tag_phash` TEXT NULL,
  `tag_description` TEXT NULL,

  `tag_scratch` TEXT NULL,

    -- timestamps (epoch_ms)
  `tag_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `tag_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `tag_source_created_datestamp_ep_k` INTEGER NULL,
  `tag_source_modified_datestamp_ep_k` INTEGER NULL

);

-- BREAK
-- BREAK


CREATE UNIQUE INDEX IF NOT EXISTS `idx_tags_unique_phash`
ON `tags`(`tag_phash`);

-- BREAK
