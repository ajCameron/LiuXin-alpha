-- BREAK - FOR EXPLANATIONS SEE LIUXIN.DOCS.TABLE_EXPLANATIONS.TXT

-- -----------------------------------------------------
-- Table `comments`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `comments` (
  `comment_id` INTEGER PRIMARY KEY,

  `comment` TEXT NULL,

  `comment_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `comment_created_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),

  -- timestamps (display DATETIME + epoch_ms source)
  `comment_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `comment_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `comment_scratch` TEXT NULL

);

-- BREAK