
-- BREAK

-- -----------------------------------------------------
-- Table `library_id`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `library_id` (

  `library_id` INTEGER PRIMARY KEY,
  `library_id_uuid` TEXT NULL,

  -- timestamps
  `library_id_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `library_id_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `library_id_scratch` TEXT NULL

    )
;


-- BREAK