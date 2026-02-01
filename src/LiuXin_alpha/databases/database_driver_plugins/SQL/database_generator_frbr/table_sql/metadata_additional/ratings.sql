-- BREAK

-- -----------------------------------------------------
-- Table `ratings`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `ratings` (
  `rating_id` INTEGER PRIMARY KEY,

  `rating` INT NULL,
  `rating_source` TEXT NULL,

  `rating_datestamp`  INTEGER  DEFAULT (STRFTIME('%s','now')),


  `rating_scratch` TEXT NULL );

-- BREAK