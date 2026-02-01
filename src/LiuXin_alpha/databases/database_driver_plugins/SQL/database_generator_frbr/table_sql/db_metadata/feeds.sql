-- BREAK

-- -----------------------------------------------------
-- Table `feeds`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `feeds` (
  `feed_id` INTEGER PRIMARY KEY,

  `feed_title` TEXT NULL,
  `feed_script` TEXT NULL,

  `feed_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `feed_created_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),

    `feed_scratch` TEXT NULL);

-- BREAK