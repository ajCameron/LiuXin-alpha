
-- BREAK

-- -----------------------------------------------------
-- Table `subjects`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `subjects` (
  `subject_id` INTEGER PRIMARY KEY,

  `subject` TEXT NULL,
  `subject_phash` TEXT NULL,
  `subject_sort` TEXT NULL,

  `subject_parent_id` INT UNSIGNED NULL,
  `subject_parent_position` INT UNSIGNED NULL,
  `subject_tree_id` TEXT NULL,
  `subject_full` TEXT NULL,

  `subject_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,

  `subject_scratch` TEXT NULL,
  CONSTRAINT `subject_parent`
    FOREIGN KEY (`subject_parent`)
    REFERENCES `subjects` (`subject_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE);

-- BREAK
-- BREAK

CREATE INDEX `subject_parent_index` ON `subjects` (`subject_parent` ASC);

-- BREAK
