-- BREAK

-- -----------------------------------------------------
-- Table `synopses`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `synopses` (
  `synopsis_id` INTEGER PRIMARY KEY,

  `synopsis` TEXT NULL,

  -- timestamps (epoch_ms)
  `synopsis_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `synopsis_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `synopsis_source_created_datestamp_ep_k` INTEGER NULL,
  `synopsis_source_modified_datestamp_ep_k` INTEGER NULL,

  `synopsis_scratch` TEXT NULL

);

-- BREAK