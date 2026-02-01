-- BREAK - FOR EXPLANATIONS SEE LIUXIN.DOCS.TABLE_EXPLANATIONS.TXT

-- -----------------------------------------------------
-- Table `comments`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `comments` (
  `comment_id` INTEGER PRIMARY KEY,

  `comment` TEXT NULL,

  `comment_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `comment_created_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),

  `comment_scratch` TEXT NULL);

-- BREAK