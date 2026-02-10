
-- BREAK


CREATE TABLE IF NOT EXISTS `transform_run_outputs` (
  `transform_run_output_id` INTEGER PRIMARY KEY,

  `transform_run_output_run_id` INTEGER NOT NULL,
  `transform_run_output_file_id` INTEGER NOT NULL,

  `transform_run_output_role` TEXT NULL,     -- 'primary','sidecar','thumbnail','log','report'
  `transform_run_output_note` TEXT NULL,

  -- timestamps (epoch_ms)
  `transform_run_output_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `transform_run_output_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `transform_run_output_source_created_datestamp_ep_k` INTEGER NULL,
  `transform_run_output_source_modified_datestamp_ep_k` INTEGER NULL,
  `transform_run_output_started_timestamp_ep_k` INTEGER NULL,
  `transform_run_output_finished_timestamp_ep_k` INTEGER NULL,


  CONSTRAINT `tro_run_fk`
    FOREIGN KEY (`transform_run_output_run_id`)
    REFERENCES `transform_runs`(`transform_run_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `tro_file_fk`
    FOREIGN KEY (`transform_run_output_file_id`)
    REFERENCES `files`(`file_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

-- BREAK
-- BREAK

CREATE UNIQUE INDEX IF NOT EXISTS `idx_transform_run_outputs_unique`
ON `transform_run_outputs`(`transform_run_output_run_id`, `transform_run_output_file_id`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_transform_run_outputs_file`
ON `transform_run_outputs`(`transform_run_output_file_id`);

-- BREAK