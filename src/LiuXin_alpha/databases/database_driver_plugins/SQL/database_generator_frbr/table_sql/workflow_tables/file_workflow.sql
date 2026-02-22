

-- BREAK

-- =====================================================
-- 1) FILE WORKFLOW: current state (materialized)
-- =====================================================

-- Status vocabulary: 'todo','doing','done','blocked','skipped','failed'
CREATE TABLE IF NOT EXISTS `file_workflow` (
  `file_workflow_id` INTEGER PRIMARY KEY,

  -- NOTE: kept nullable so DriverWrapper.get_blank_row() can insert a placeholder row.
  -- Application logic can enforce presence later.
  `file_workflow_file_id` INTEGER NULL,
  `file_workflow_step_id` INTEGER NULL,

  `file_workflow_status` TEXT NOT NULL DEFAULT 'todo',

  `file_workflow_priority` INTEGER NOT NULL DEFAULT 0,
  `file_workflow_assigned_to` TEXT NULL,        -- later: user_id / agent_id
  `file_workflow_reason` TEXT NULL,             -- why blocked/failed/skipped
  `file_workflow_progress` REAL NULL,           -- 0..1

  -- timestamps (epoch_ms)
  `file_workflow_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `file_workflow_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `file_workflow_source_created_datestamp_ep_k` INTEGER NULL,
  `file_workflow_source_modified_datestamp_ep_k` INTEGER NULL,
  `file_workflow_started_timestamp_ep_k` INTEGER NULL,
  `file_workflow_finished_timestamp_ep_k` INTEGER NULL,

  `file_workflow_scratch` TEXT NULL,

  CONSTRAINT `file_workflow_file_fk`
    FOREIGN KEY (`file_workflow_file_id`)
    REFERENCES `files`(`file_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `file_workflow_step_fk`
    FOREIGN KEY (`file_workflow_step_id`)
    REFERENCES `workflow_steps`(`workflow_step_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `file_workflow_status_check`
    CHECK (`file_workflow_status` IN ('todo','doing','done','blocked','skipped','failed')),

  CONSTRAINT `file_workflow_priority_check`
    CHECK (`file_workflow_priority` >= 0),

  CONSTRAINT `file_workflow_progress_check`
    CHECK (`file_workflow_progress` IS NULL OR (`file_workflow_progress` >= 0 AND `file_workflow_progress` <= 1))
);

-- BREAK
-- BREAK


-- One row per (file, step)
CREATE UNIQUE INDEX IF NOT EXISTS `idx_file_workflow_unique`
ON `file_workflow`(`file_workflow_file_id`, `file_workflow_step_id`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_file_workflow_by_status`
ON `file_workflow`(`file_workflow_status`, `file_workflow_priority`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_file_workflow_by_file`
ON `file_workflow`(`file_workflow_file_id`);

-- BREAK
