-- BREAK

-- -----------------------------------------------------
-- Table `custom_columns`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `custom_columns` (
  `custom_column_id` INTEGER PRIMARY KEY,

  `custom_column_mark_for_delete` INTEGER NULL DEFAULT 0,

  `custom_column_in_table` TEXT NULL,

  `custom_column_label` TEXT NULL,
  `custom_column_name` TEXT NULL,

  `custom_column_datatype` TEXT NULL,
  `custom_column_db_datatype` TEXT NULL,
  `custom_column_is_multiple` INTEGER NULL DEFAULT 0,
  `custom_column_normalized` INTEGER NULL DEFAULT 0,

  `custom_column_editable` INTEGER NULL DEFAULT 1,

  `custom_column_display` TEXT NULL,
  `custom_column_display_sort` INTEGER NULL DEFAULT 0,
  `custom_column_ordered` INTEGER NULL DEFAULT 0,

  -- timestamps (epoch_ms)
  `custom_column_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `custom_column_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `custom_column_source_created_datestamp_ep_k` INTEGER NULL,
  `custom_column_source_modified_datestamp_ep_k` INTEGER NULL,

  `custom_column_scratch` TEXT NULL,

  CONSTRAINT `custom_column_name_unique` UNIQUE (`custom_column_name`),
  CONSTRAINT `custom_column_label_unique` UNIQUE (`custom_column_label`)
  )
;


-- BREAK
