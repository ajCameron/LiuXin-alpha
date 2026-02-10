
-- =====================================================
-- 5) FILE LINEAGE (explicit derivation edges)
-- =====================================================

CREATE TABLE IF NOT EXISTS file_derivations (

  `file_derivation_id` INTEGER PRIMARY KEY,

  `file_derivation_parent_file_id` INTEGER NOT NULL,
  `file_derivation_child_file_id`  INTEGER NOT NULL,

  `file_derivation_run_id` INTEGER NULL,     -- optional: link to the transform run that produced it
  `file_derivation_kind` TEXT NULL,          -- 'converted','ocr_text','thumbnail','repacked','repaired','extracted', ...
  `file_derivation_note` TEXT NULL,

  -- timestamps (epoch_ms)
  `file_derivation_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `file_derivation_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `file_derivation_source_created_datestamp_ep_k` INTEGER NULL,
  `file_derivation_source_modified_datestamp_ep_k` INTEGER NULL,
  `file_derivation_started_timestamp_ep_k` INTEGER NULL,
  `file_derivation_finished_timestamp_ep_k` INTEGER NULL,

  `file_derivation_scratch` TEXT NULL,

  CONSTRAINT `file_derviation_parent_file_id_fk`
    FOREIGN KEY (`file_derivation_parent_file_id`)
    REFERENCES `files`(`file_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT fd_child_fk
    FOREIGN KEY (file_derivation_child_file_id)
    REFERENCES files(file_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT fd_run_fk
    FOREIGN KEY (file_derivation_run_id)
    REFERENCES transform_runs(transform_run_id)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT fd_no_self
    CHECK (file_derivation_parent_file_id != file_derivation_child_file_id)

);

CREATE UNIQUE INDEX IF NOT EXISTS idx_file_derivations_unique
ON file_derivations(file_derivation_parent_file_id, file_derivation_child_file_id);

CREATE INDEX IF NOT EXISTS idx_file_derivations_parent
ON file_derivations(file_derivation_parent_file_id);

CREATE INDEX IF NOT EXISTS idx_file_derivations_child
ON file_derivations(file_derivation_child_file_id);

