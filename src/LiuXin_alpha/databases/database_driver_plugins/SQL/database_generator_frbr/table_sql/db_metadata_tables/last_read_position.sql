
-- BREAK

CREATE TABLE IF NOT EXISTS `last_read_positions` (
  `last_read_position_id` INTEGER PRIMARY KEY,

  -- NOTE: kept nullable so DriverWrapper.get_blank_row() can insert a placeholder row.
  -- Application logic can enforce presence later.
  `last_read_position_book` INTEGER NULL,
  `last_read_position_format` TEXT NULL COLLATE `NOCASE`,
  `last_read_position_user` TEXT NULL,
  `last_read_position_device` TEXT NULL,
  `last_read_position_cfi` TEXT NULL,

  `last_read_position_epoch` REAL NULL,

  `last_read_position_pos_frac` REAL NOT NULL DEFAULT 0,

    -- timestamps (epoch_ms)
  `last_read_position_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `last_read_position_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `last_read_position_source_created_datestamp_ep_k` INTEGER NULL,
  `last_read_position_source_modified_datestamp_ep_k` INTEGER NULL,

  `last_read_positions_scratch` TEXT NULL,

  UNIQUE(`last_read_position_user`, `last_read_position_device`, `last_read_position_book`, `last_read_position_format`)

);


-- BREAK
