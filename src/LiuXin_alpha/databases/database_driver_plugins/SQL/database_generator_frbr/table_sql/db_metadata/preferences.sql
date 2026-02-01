-- BREAK

-- -----------------------------------------------------
-- Table `preferences`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `preferences` (
  `preference_id` INTEGER PRIMARY KEY,

  `preference_key` TEXT NULL,
  `preference_value` TEXT NULL,
  `preference_value_type` TEXT NULL,

  `preference_parent_liuxin_instance` TEXT NULL,

  `preference_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),

  `preference_scratch` TEXT NULL)
;


-- BREAK