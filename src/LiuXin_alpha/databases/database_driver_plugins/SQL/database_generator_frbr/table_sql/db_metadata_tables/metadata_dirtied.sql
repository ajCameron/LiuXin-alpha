-- BREAK

-- -----------------------------------------------------
-- Table `metadata_dirtied_books`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `metadata_dirtied_books` (
  `metadata_dirtied_id` TEXT PRIMARY KEY,

  `metadata_dirtied_table_id` INTEGER NULL,
  `metadata_dirtied_table` TEXT NULL,
  `metadata_drtied_reason` TEXT NULL,


  -- timestamps (epoch_ms)
  `metadata_dirtied_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `metadata_dirtied_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `metadata_dirtied_source_created_datestamp_ep_k` INTEGER NULL,
  `metadata_dirtied_source_modified_datestamp_ep_k` INTEGER NULL,
  `metadata_dirtied_scratch` TEXT NULL

)
;


-- BREAK