-- BREAK

-- -----------------------------------------------------
-- Table `preferences`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `preferences` (
  `preference_id` INTEGER PRIMARY KEY,

  `preference_key` TEXT NULL,
  `preference_value` TEXT NULL,
  `preference_value_type` TEXT NULL,

  `preference_parent_liuxin_instance` TEXT NULL,

  -- timestamps (display DATETIME + epoch_ms source)
  preference_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  preference_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  preference_modified_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  preference_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `preference_scratch` TEXT NULL)
;


-- BREAK