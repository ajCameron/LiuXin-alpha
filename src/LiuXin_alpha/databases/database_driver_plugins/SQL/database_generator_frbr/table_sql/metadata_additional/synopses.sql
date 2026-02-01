-- BREAK

-- -----------------------------------------------------
-- Table `synopses`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `synopses` (
  `synopsis_id` INTEGER PRIMARY KEY,

  `synopsis` TEXT NULL,

  `synopsis_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `synopsis_scratch` TEXT NULL );

-- BREAK