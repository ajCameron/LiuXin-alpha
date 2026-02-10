
-- BREAK

-- -----------------------------------------------------
-- Table `subjects`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `subjects` (
  `subject_id` INTEGER PRIMARY KEY,

  `subject` TEXT NULL,
  `subject_phash` TEXT NULL,
  `subject_sort` TEXT NULL,

  `subject_parent_id` INTEGER NULL,
  `subject_parent_position` INTEGER NULL,
  `subject_tree_id` TEXT NULL,
  `subject_full` TEXT NULL,


  -- timestamps (epoch_ms)
  `subject_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `subject_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `subject_source_created_datestamp_ep_k` INTEGER NULL,
  `subject_source_modified_datestamp_ep_k` INTEGER NULL,

  `subject_scratch` TEXT NULL,
  CONSTRAINT `subject_parent_id`
    FOREIGN KEY (`subject_parent_id`)
    REFERENCES `subjects` (`subject_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `subject_parent_index`
ON `subjects` (`subject_parent_id`);

-- BREAK
