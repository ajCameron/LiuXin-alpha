
-- BREAK

CREATE TABLE IF NOT EXISTS `last_read_positions` (
  `last_read_position_id` INTEGER PRIMARY KEY,

  `last_read_position_book` INTEGER NOT NULL,
  `last_read_position_format` TEXT NOT NULL COLLATE `NOCASE`,
  `last_read_position_user` TEXT NOT NULL,
  `last_read_position_device` TEXT NOT NULL,
  `last_read_position_cfi` TEXT NOT NULL,

  `last_read_position_epoch` REAL NOT NULL,

  `last_read_position_pos_frac` REAL NOT NULL DEFAULT 0,

    -- timestamps (epoch_ms)
  `last_read_position_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `last_read_position_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `last_read_position_source_created_datestamp_ep_k` INTEGER NULL,
  `last_read_position_source_modified_datestamp_ep_k` INTEGER NULL,

  `last_read_positions_scratch` TEXT NULL,

  UNIQUE(`user`, `device`, `book`, `format`)

);


-- BREAK
