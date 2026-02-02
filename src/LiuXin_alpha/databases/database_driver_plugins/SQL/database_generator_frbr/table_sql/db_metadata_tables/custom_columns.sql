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

  -- timestamps (display DATETIME + epoch_ms source)
  custom_column_created_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  custom_column_created_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  custom_column_modified_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  custom_column_modified_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `custom_column_scratch` TEXT NULL,

  CONSTRAINT `custom_column_name_unique` UNIQUE (`custom_column_name`),
  CONSTRAINT `custom_column_label_unique` UNIQUE (`custom_column_label`)
  )
;


-- BREAK
