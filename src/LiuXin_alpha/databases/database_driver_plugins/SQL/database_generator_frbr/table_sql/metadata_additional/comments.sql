-- BREAK - FOR EXPLANATIONS SEE LIUXIN.DOCS.TABLE_EXPLANATIONS.TXT

-- -----------------------------------------------------
-- Table `comments`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `comments` (
  `comment_id` INTEGER PRIMARY KEY,

  `comment` TEXT NULL,


  -- timestamps (epoch_ms)
  `comment_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `comment_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `comment_source_created_datestamp_ep_k` INTEGER NULL,
  `comment_source_modified_datestamp_ep_k` INTEGER NULL,

  `comment_scratch` TEXT NULL

);

-- BREAK