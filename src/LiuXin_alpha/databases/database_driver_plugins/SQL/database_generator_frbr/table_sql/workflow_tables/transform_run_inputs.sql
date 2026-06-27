
-- BREAK

-- =====================================================
-- 4) TRANSFORM INPUTS / OUTPUTS (many-to-many)
-- =====================================================

CREATE TABLE IF NOT EXISTS `transform_run_inputs` (
  `transform_run_input_id` INTEGER PRIMARY KEY,

  `transform_run_input_run_id` INTEGER NOT NULL,
  `transform_run_input_digital_asset_id` INTEGER NOT NULL,

  `transform_run_input_role` TEXT NULL,      -- 'primary','aux','sidecar','dictionary','cover_source', ...
  `transform_run_input_note` TEXT NULL,

  -- timestamps (epoch_ms)
  `transform_run_input_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `transform_run_input_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `transform_run_input_source_created_datestamp_ep_k` INTEGER NULL,
  `transform_run_input_source_modified_datestamp_ep_k` INTEGER NULL,
  `transform_run_input_started_timestamp_ep_k` INTEGER NULL,
  `transform_run_input_finished_timestamp_ep_k` INTEGER NULL,

  CONSTRAINT `tri_run_fk`
    FOREIGN KEY (`transform_run_input_run_id`)
    REFERENCES `transform_runs`(`transform_run_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `tri_file_fk`
    FOREIGN KEY (`transform_run_input_digital_asset_id`)
    REFERENCES `digital_assets`(`digital_asset_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

-- BREAK
-- BREAK

CREATE UNIQUE INDEX IF NOT EXISTS `idx_transform_run_inputs_unique`
ON `transform_run_inputs`(`transform_run_input_run_id`, `transform_run_input_digital_asset_id`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_transform_run_inputs_digital_asset`
ON `transform_run_inputs`(`transform_run_input_digital_asset_id`);

-- BREAK