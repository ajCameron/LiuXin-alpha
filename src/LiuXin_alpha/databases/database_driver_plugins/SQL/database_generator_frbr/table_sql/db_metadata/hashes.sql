-- BREAK

-- -----------------------------------------------------
-- Table `hashes`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `hashes` (
  `hash_id` TEXT PRIMARY KEY,

  `hash` TEXT NULL,

  `hash_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),

    `hash_scratch` TEXT NULL)
;

-- BREAK