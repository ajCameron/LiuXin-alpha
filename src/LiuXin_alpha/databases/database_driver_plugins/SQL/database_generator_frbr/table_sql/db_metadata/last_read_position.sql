
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

  UNIQUE(user, device, book, format)
);


-- BREAK
