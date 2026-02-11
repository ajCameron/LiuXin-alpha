-- BREAK

-- -----------------------------------------------------
-- Table `devices`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `devices` (
  `device_id` INTEGER PRIMARY KEY,

  `device_type` TEXT NULL,


  -- timestamps (epoch_ms)
  `device_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `device_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `device_source_created_datestamp_ep_k` INTEGER NULL,
  `device_source_modified_datestamp_ep_k` INTEGER NULL,

  `device_scratch` TEXT NULL

);

-- BREAK