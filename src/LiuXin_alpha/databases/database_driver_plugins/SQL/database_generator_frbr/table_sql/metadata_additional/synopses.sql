-- BREAK

-- -----------------------------------------------------
-- Table `synopses`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `synopses` (
  `synopsis_id` INTEGER PRIMARY KEY,

  `synopsis` TEXT NULL,

  -- timestamps (display DATETIME + epoch_ms source)
  synops_created_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  synops_created_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

    synops_modified_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  synops_modified_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `synopsis_scratch` TEXT NULL

);

-- BREAK