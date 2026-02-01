-- BREAK

-- -----------------------------------------------------
-- Table `notes`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `notes` (
  `note_id` INTEGER PRIMARY KEY,

  `note` TEXT NULL,

  `note_datestamp` INTEGER  DEFAULT (STRFTIME('%s','now')),
  `note_created_datestamp` INTEGER  DEFAULT (STRFTIME('%s','now')),

  `note_scratch` TEXT NULL );

-- BREAK