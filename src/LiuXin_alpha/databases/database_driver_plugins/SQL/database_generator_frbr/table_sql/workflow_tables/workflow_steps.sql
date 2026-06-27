


-- BREAK


-- =====================================================
-- WORKFLOW DEFINITIONS
-- =====================================================

CREATE TABLE IF NOT EXISTS `workflow_steps` (
  `workflow_step_id` INTEGER PRIMARY KEY,

  -- Stable identifier used by code (e.g. 'ocr', 'dedupe', 'cover', 'metadata', 'reencode_epub')
  `workflow_step_code` TEXT NOT NULL UNIQUE,

  -- Human label
  `workflow_step_label` TEXT NOT NULL,

  -- Optional grouping (e.g. 'ingest', 'quality', 'derivation', 'export')
  `workflow_step_group` TEXT NULL,

  -- If TRUE, step must be done for the item to be "complete"
  `workflow_step_is_required` INTEGER NOT NULL DEFAULT 0,

  -- If TRUE, user/tool can "skip" this step explicitly
  `workflow_step_is_skippable` INTEGER NOT NULL DEFAULT 1,

  -- Optional ordering hint for UI
  `workflow_step_ord` INTEGER NULL,

  `workflow_step_scope` TEXT NULL,

  -- timestamps (epoch_ms)
  `workflow_step_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `workflow_step_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `workflow_step_source_created_datestamp_ep_k` INTEGER NULL,
  `workflow_step_source_modified_datestamp_ep_k` INTEGER NULL,

  CONSTRAINT `workflow_steps_bool_check`
    CHECK (`workflow_step_is_required` IN (0,1) AND `workflow_step_is_skippable` IN (0,1)),

  CONSTRAINT `workflow_steps_ord_check`
    CHECK (`workflow_step_ord` IS NULL OR `workflow_step_ord` >= 0)
);

-- BREAK


-- BREAK


CREATE TRIGGER IF NOT EXISTS `trg_workflow_steps_scope_check_upd`
BEFORE UPDATE OF `workflow_step_scope` ON `workflow_steps`
WHEN NEW.`workflow_step_scope` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.`workflow_step_scope` NOT IN ('item','digital_asset','both')
    THEN RAISE(ABORT, 'workflow_steps.workflow_step_scope must be one of: item, digital_asset, both')
  END;
END;


-- BREAK

-- BREAK


CREATE INDEX IF NOT EXISTS `idx_workflow_steps_group_ord`
ON `workflow_steps`(`workflow_step_group`, `workflow_step_ord`);

-- =====================================================
-- PER-ITEM WORKFLOW STATE (materialized "current state")
-- =====================================================

-- BREAK
-- BREAK


CREATE TRIGGER IF NOT EXISTS `trg_workflow_steps_scope_check`
BEFORE INSERT ON `workflow_steps`
WHEN NEW.`workflow_step_scope` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.`workflow_step_scope` NOT IN ('item','digital_asset','both')
    THEN RAISE(ABORT, 'workflow_steps.workflow_step_scope must be one of: item, digital_asset, both')
  END;
END;

-- BREAK
