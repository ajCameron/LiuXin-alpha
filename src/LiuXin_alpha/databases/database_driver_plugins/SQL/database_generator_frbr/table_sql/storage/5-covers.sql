
-- BREAK


-- -----------------------------------------------------
-- Table `covers`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `covers` (
  `cover_id` INTEGER PRIMARY KEY,

  `cover_name` TEXT NULL,
  `cover_extension` TEXT NULL,
  `cover_path` TEXT NULL,
  `cover_use_auto_name` TINYINT(1) DEFAULT 1,

  `cover_hash` TEXT NULL,
  `cover_new_hash` TEXT NULL,
  `cover_corrupt` TINYINT(1) DEFAULT 0,

  `cover_original_path` TEXT NULL,

  `cover_local` TINYINT(1) NULL,

  `cover_base_folder` INT UNSIGNED NULL,
  `cover_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `cover_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),

  `cover_scratch` TEXT NULL,

  CONSTRAINT `cover_folder_link`
    FOREIGN KEY (`cover_base_folder`)
    REFERENCES `folders` (`folder_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
;



-- BREAK
