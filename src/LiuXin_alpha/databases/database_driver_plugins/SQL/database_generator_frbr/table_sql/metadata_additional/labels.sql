-- BREAK

-- -----------------------------------------------------
-- Table `labels` (tags... ish)
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS labels (
  label_id INTEGER PRIMARY KEY,

  label_text TEXT NOT NULL,
  label_text_norm TEXT NOT NULL,
  label_description TEXT NULL,

  label_datestamp         INTEGER  DEFAULT (STRFTIME('%s','now')),
  label_created_datestamp DATETIME DEFAULT (STRFTIME('%s', 'now')),

  label_scratch TEXT NULL,

  CONSTRAINT labels_text_nonempty CHECK (LENGTH(TRIM(label_text)) > 0),
  CONSTRAINT labels_norm_nonempty CHECK (LENGTH(TRIM(label_text_norm)) > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_labels_unique_norm
ON labels(label_text_norm);

-- BREAK