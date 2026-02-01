-- BREAK

-- -----------------------------------------------------
-- Table `library_id`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `library_id` (
  `library_id` TEXT PRIMARY KEY,
  `library_id_uuid` TEXT NULL,
  `library_id_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')

    )
;


-- BREAK