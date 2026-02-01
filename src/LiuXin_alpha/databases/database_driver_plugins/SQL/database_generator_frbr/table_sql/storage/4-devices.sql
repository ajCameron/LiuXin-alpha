-- BREAK

-- -----------------------------------------------------
-- Table `devices`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `devices` (
  `device_id` INTEGER PRIMARY KEY,

  `device_type` TEXT NULL,

  `device_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `device_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),

  `device_scratch` TEXT NULL );

-- BREAK