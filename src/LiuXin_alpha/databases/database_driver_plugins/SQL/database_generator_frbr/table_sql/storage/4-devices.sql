-- BREAK

-- -----------------------------------------------------
-- Table `devices`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `devices` (
  `device_id` INTEGER PRIMARY KEY,

  `device_type` TEXT NULL,

  `device_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `device_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),

  -- timestamps (display DATETIME + epoch_ms source)
  `device_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `device_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `device_scratch` TEXT NULL

);

-- BREAK