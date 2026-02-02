-- BREAK

-- -----------------------------------------------------
-- Table `library_id`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `library_id` (

  `library_id` TEXT PRIMARY KEY,
  `library_id_uuid` TEXT NULL,
  `library_id_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now'),

  -- timestamps (display DATETIME + epoch_ms source)
  library_id_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  library_id_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  library_id_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  library_id_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `library_id_scratch` TEXT NULL

    )
;


-- BREAK