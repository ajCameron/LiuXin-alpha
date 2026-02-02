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

    -- timestamps (display DATETIME + epoch_ms source)
  label_created_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  label_created_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

    label_modified_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  label_modified_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  CONSTRAINT labels_text_nonempty CHECK (LENGTH(TRIM(label_text)) > 0),
  CONSTRAINT labels_norm_nonempty CHECK (LENGTH(TRIM(label_text_norm)) > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_labels_unique_norm
ON labels(label_text_norm);

-- BREAK