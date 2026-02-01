-- BREAK

-- -----------------------------------------------------
-- Table `genres`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `genres` (
  `genre_id` INTEGER PRIMARY KEY,

  `genre` TEXT NULL,
  `genre_sort` TEXT NULL,
  `genre_phash` TEXT NULL,

  `genre_parent_id` INT UNSIGNED NULL,
  `genre_position` INT UNSIGNED NULL,
  `genre_tree_id` INT UNSIGNED NULL,
  `genre_full` TEXT NULL,

  `genre_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `genre_created_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),

  `genre_scratch` TEXT NULL,

  CONSTRAINT `genre_parent`
    FOREIGN KEY (`genre_parent`)
    REFERENCES `genres` (`genre_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE);

-- BREAK
-- BREAK

CREATE INDEX `genre_parent_index` ON `genres` (`genre_parent` ASC);

-- BREAK