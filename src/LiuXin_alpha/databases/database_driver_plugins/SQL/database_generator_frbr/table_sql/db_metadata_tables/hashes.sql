-- BREAK

-- -----------------------------------------------------
-- Table `hashes`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `hashes` (
  `hash_id` TEXT PRIMARY KEY,

  `hash` TEXT NULL,

  -- timestamps (display DATETIME + epoch_ms source)
  hash_created_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  hash_created_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  hash_modified_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  hash_modified_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

    `hash_scratch` TEXT NULL)
;

-- BREAK