-- BREAK

-- -----------------------------------------------------
-- Table `hashes`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `hashes` (
  `hash_id` TEXT PRIMARY KEY,
  `hash` TEXT NULL,
  -- timestamps (epoch_ms)
  `hash_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `hash_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `hash_source_created_datestamp_ep_k` INTEGER NULL,
  `hash_source_modified_datestamp_ep_k` INTEGER NULL,
  `hash_scratch` TEXT NULL)
;

-- BREAK