
-- BREAK

CREATE TABLE IF NOT EXISTS `last_read_positions` (
  `id` INTEGER PRIMARY KEY,

  `book` INTEGER NOT NULL,
  `format` TEXT NOT NULL COLLATE NOCASE,
  `user` TEXT NOT NULL,
  `device` TEXT NOT NULL,
  `cfi` TEXT NOT NULL,
  `epoch` REAL NOT NULL,
  `pos_frac` REAL NOT NULL DEFAULT 0,
  `datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),

    -- timestamps (display DATETIME + epoch_ms source)
  last_read_position_created_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_read_position_created_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

    last_read_position_modified_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_read_position_modified_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

    `last_read_positions_scratch` TEXT NULL,


  UNIQUE(user, device, book, format)

);


-- BREAK
