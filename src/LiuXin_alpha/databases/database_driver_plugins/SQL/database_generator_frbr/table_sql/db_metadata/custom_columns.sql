-- BREAK

-- -----------------------------------------------------
-- Table `custom_columns`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `custom_columns` (
  `custom_column_id` INTEGER PRIMARY KEY,

  `custom_column_mark_for_delete` INT NULL DEFAULT 0,

  `custom_column_in_table` TEXT NULL,

  `custom_column_label` TEXT NULL,
  `custom_column_name` TEXT NULL,

  `custom_column_datatype` TEXT NULL,
  `custom_column_db_datatype` TEXT NULL,
  `custom_column_is_multiple` INT NULL DEFAULT 0,
  `custom_column_normalized` INT NULL DEFAULT 0,

  `custom_column_editable` INT NULL DEFAULT 1,

  `custom_column_display` TEXT NULL,
  `custom_column_display_sort` INT NULL DEFAULT 0,
  `custom_column_ordered` INT NULL DEFAULT 0,

  `custom_column_datestamp` DATETIME DEFAULT (STRFTIME('%s','now')),
  `custom_column_created_datestamp` DATETIME DEFAULT (STRFTIME('%s','now')),

  `custom_column_scratch` TEXT NULL,

  CONSTRAINT `custom_column_name_unique` UNIQUE (`custom_column_name`),
  CONSTRAINT `custom_column_label_unique` UNIQUE (`custom_column_label`)
  )
;


-- BREAK
