-- BREAK

-- -----------------------------------------------------
-- Table `labels` (tags... ish)
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS labels (

  `label_id` INTEGER PRIMARY KEY,

  `label_text` TEXT NOT NULL,
  `label_text_norm` TEXT NOT NULL,
  `label_description` TEXT NULL,


  `label_scratch` TEXT NULL,

    -- timestamps (epoch_ms)
  `label_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `label_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  `label_source_created_datestamp_ep_k` INTEGER NULL,
  `label_source_modified_datestamp_ep_k` INTEGER NULL,

);

-- BREAK
-- BREAK


CREATE UNIQUE INDEX IF NOT EXISTS idx_labels_unique_norm
ON labels(label_text_norm);

-- BREAK