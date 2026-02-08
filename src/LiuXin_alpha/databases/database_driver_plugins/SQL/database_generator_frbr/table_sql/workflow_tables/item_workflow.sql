
-- BREAK


-- Status vocabulary: 'todo', 'doing', 'done', 'blocked', 'skipped', 'failed'
CREATE TABLE IF NOT EXISTS `item_workflow` (
  `item_workflow_id` INTEGER PRIMARY KEY,

  `item_workflow_item_id` INTEGER NOT NULL,
  `item_workflow_step_id` INTEGER NOT NULL,

  `item_workflow_status` TEXT NOT NULL DEFAULT 'todo',

  -- lightweight fields for assignment / triage
  `item_workflow_priority` INTEGER NOT NULL DEFAULT 0,
  `item_workflow_assigned_to` TEXT NULL,     -- later: user_id/agent_id
  `item_workflow_reason` TEXT NULL,          -- why blocked/failed/skipped
  `item_workflow_progress` REAL NULL,        -- 0..1 for long-running tasks

  -- Useful timestamps

  `item_workflow_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `item_workflow_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `item_workflow_started_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `item_workflow_finished_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `item_workflow_scratch` TEXT NULL,

  CONSTRAINT `item_workflow_item_fk`
    FOREIGN KEY (`item_workflow_item_id`)
    REFERENCES `items`(`item_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `item_workflow_step_fk`
    FOREIGN KEY (`item_workflow_step_id`)
    REFERENCES `workflow_steps`(`workflow_step_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `item_workflow_status_check`
    CHECK (`item_workflow_status` IN ('todo','doing','done','blocked','skipped','failed')),

  CONSTRAINT `item_workflow_priority_check`
    CHECK (`item_workflow_priority` >= 0),

  CONSTRAINT `item_workflow_progress_check`
    CHECK (`item_workflow_progress` IS NULL OR (`item_workflow_progress` >= 0 AND `item_workflow_progress` <= 1))
);

-- BREAK
-- BREAK

-- One row per (item, step): the materialized current status
CREATE UNIQUE INDEX IF NOT EXISTS `idx_item_workflow_unique`
ON `item_workflow`(`item_workflow_item_id`, `item_workflow_step_id`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_item_workflow_by_status`
ON `item_workflow`(`item_workflow_status`, `item_workflow_priority`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_item_workflow_by_item`
ON `item_workflow`(`item_workflow_item_id`);

-- BREAK
-- BREAK


-- If a step is marked required and not skippable, forbid setting status to 'skipped'
CREATE TRIGGER IF NOT EXISTS trg_item_workflow_forbid_skipping_required
BEFORE UPDATE OF item_workflow_status ON item_workflow
WHEN NEW.item_workflow_status = 'skipped'
BEGIN
  SELECT CASE
    WHEN EXISTS (
      SELECT 1 FROM workflow_steps
      WHERE workflow_step_id = NEW.item_workflow_step_id
        AND workflow_step_is_required = 1
        AND workflow_step_is_skippable = 0
      LIMIT 1
    )
    THEN RAISE(ABORT, 'cannot skip a required non-skippable workflow step')
  END;
END;


-- BREAK
-- BREAK

PRAGMA foreign_keys = ON;


-- BREAK