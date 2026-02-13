-- BREAK



-- -----------------------------------------------------
-- Table `languages`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `languages` (
  `language_id` INTEGER PRIMARY KEY,

  `language` TEXT NULL,
  `language_code` TEXT NULL,

  -- timestamps (display needs to be generated from epoch time)
  `language_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `language_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `language_source_created_datestamp_ep_k` INTEGER NULL,
  `language_source_modified_datestamp_ep_k` INTEGER NULL,

  `language_scratch` TEXT NULL,

  CONSTRAINT `language_code_unique`
    UNIQUE (`language_code`)
)
;


-- BREAK