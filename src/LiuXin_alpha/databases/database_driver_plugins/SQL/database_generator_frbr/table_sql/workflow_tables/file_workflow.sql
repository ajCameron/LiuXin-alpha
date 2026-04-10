

-- BREAK

-- =====================================================
-- 1) FILE WORKFLOW: current state (materialized)
-- =====================================================

-- Status vocabulary: 'todo','doing','done','blocked','skipped','failed'
CREATE TABLE IF NOT EXISTS `digital_asset_workflow` (
  `digital_asset_workflow_id` INTEGER PRIMARY KEY,

  -- NOTE: kept nullable so DriverWrapper.get_blank_row() can insert a placeholder row.
  -- Application logic can enforce presence later.
  `digital_asset_workflow_file_id` INTEGER NULL,
  `digital_asset_workflow_step_id` INTEGER NULL,

  `digital_asset_workflow_status` TEXT NOT NULL DEFAULT 'todo',

  `digital_asset_workflow_priority` INTEGER NOT NULL DEFAULT 0,
  `digital_asset_workflow_assigned_to` TEXT NULL,        -- later: user_id / agent_id
  `digital_asset_workflow_reason` TEXT NULL,             -- why blocked/failed/skipped
  `digital_asset_workflow_progress` REAL NULL,           -- 0..1

  -- timestamps (epoch_ms)
  `digital_asset_workflow_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `digital_asset_workflow_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `digital_asset_workflow_source_created_datestamp_ep_k` INTEGER NULL,
  `digital_asset_workflow_source_modified_datestamp_ep_k` INTEGER NULL,
  `digital_asset_workflow_started_timestamp_ep_k` INTEGER NULL,
  `digital_asset_workflow_finished_timestamp_ep_k` INTEGER NULL,

  `digital_asset_workflow_scratch` TEXT NULL,

  CONSTRAINT `digital_asset_workflow_file_fk`
    FOREIGN KEY (`digital_asset_workflow_file_id`)
    REFERENCES `digital_assets`(`digital_asset_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `digital_asset_workflow_step_fk`
    FOREIGN KEY (`digital_asset_workflow_step_id`)
    REFERENCES `workflow_steps`(`workflow_step_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `digital_asset_workflow_status_check`
    CHECK (`digital_asset_workflow_status` IN ('todo','doing','done','blocked','skipped','failed')),

  CONSTRAINT `digital_asset_workflow_priority_check`
    CHECK (`digital_asset_workflow_priority` >= 0),

  CONSTRAINT `digital_asset_workflow_progress_check`
    CHECK (`digital_asset_workflow_progress` IS NULL OR (`digital_asset_workflow_progress` >= 0 AND `digital_asset_workflow_progress` <= 1))
);

-- BREAK
-- BREAK


-- One row per (file, step)
CREATE UNIQUE INDEX IF NOT EXISTS `idx_digital_asset_workflow_unique`
ON `digital_asset_workflow`(`digital_asset_workflow_file_id`, `digital_asset_workflow_step_id`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_digital_asset_workflow_by_status`
ON `digital_asset_workflow`(`digital_asset_workflow_status`, `digital_asset_workflow_priority`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_digital_asset_workflow_by_file`
ON `digital_asset_workflow`(`digital_asset_workflow_file_id`);

-- BREAK
