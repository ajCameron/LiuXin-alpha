
-- BREAK

-- =====================================================
-- 6) TRIGGERS: keep digital_asset workflow scope sane + event logging + timestamps
-- =====================================================

-- Ensure digital_asset_workflow_step is usable for file/digital_asset scope
CREATE TRIGGER IF NOT EXISTS `trg_digital_asset_workflow_step_scope_check`
BEFORE INSERT ON `digital_asset_workflow`
BEGIN
  SELECT CASE
    WHEN EXISTS (
      SELECT 1 FROM `workflow_steps`
      WHERE `workflow_step_id` = NEW.`digital_asset_workflow_step_id`
        AND `workflow_step_scope` IS NOT NULL
        AND `workflow_step_scope` NOT IN ('file','digital_asset','both')
      LIMIT 1
    )
    THEN RAISE(ABORT, 'digital_asset_workflow step is not scoped for file/digital_asset/both')
  END;
END;

-- BREAK
-- BREAK

CREATE TRIGGER IF NOT EXISTS `trg_digital_asset_workflow_step_scope_check_upd`
BEFORE UPDATE OF `digital_asset_workflow_step_id` ON `digital_asset_workflow`
BEGIN
  SELECT CASE
    WHEN EXISTS (
      SELECT 1 FROM `workflow_steps`
      WHERE `workflow_step_id` = NEW.`digital_asset_workflow_step_id`
        AND `workflow_step_scope` IS NOT NULL
        AND `workflow_step_scope` NOT IN ('file','digital_asset','both')
      LIMIT 1
    )
    THEN RAISE(ABORT, 'digital_asset_workflow step is not scoped for file/digital_asset/both')
  END;
END;

-- BREAK
-- BREAK

-- Event on INSERT (initial state)
CREATE TRIGGER IF NOT EXISTS `trg_digital_asset_workflow_insert_event`
AFTER INSERT ON `digital_asset_workflow`
BEGIN
  INSERT INTO `digital_asset_workflow_events`(
    `digital_asset_workflow_event_digital_asset_id`,
    `digital_asset_workflow_event_step_id`,
    `digital_asset_workflow_event_from_status`,
    `digital_asset_workflow_event_to_status`,
    `digital_asset_workflow_event_actor`,
    `digital_asset_workflow_event_note`
  )
  VALUES(
    NEW.`digital_asset_workflow_digital_asset_id`,
    NEW.`digital_asset_workflow_step_id`,
    NULL,
    NEW.`digital_asset_workflow_status`,
    NEW.`digital_asset_workflow_assigned_to`,
    'initial state'
  );
END;

-- BREAK
-- BREAK

-- Status change -> event
CREATE TRIGGER IF NOT EXISTS `trg_digital_asset_workflow_status_change_event`
AFTER UPDATE OF `digital_asset_workflow_status` ON `digital_asset_workflow`
WHEN NEW.`digital_asset_workflow_status` != OLD.`digital_asset_workflow_status`
BEGIN
  INSERT INTO `digital_asset_workflow_events`(
    `digital_asset_workflow_event_digital_asset_id`,
    `digital_asset_workflow_event_step_id`,
    `digital_asset_workflow_event_from_status`,
    `digital_asset_workflow_event_to_status`,
    `digital_asset_workflow_event_actor`,
    `digital_asset_workflow_event_note`
  )
  VALUES(
    NEW.`digital_asset_workflow_digital_asset_id`,
    NEW.`digital_asset_workflow_step_id`,
    OLD.`digital_asset_workflow_status`,
    NEW.`digital_asset_workflow_status`,
    NEW.`digital_asset_workflow_assigned_to`,
    NEW.`digital_asset_workflow_reason`
  );
END;

-- BREAK
-- BREAK

-- Auto-set started/finished timestamps based on status transitions
CREATE TRIGGER IF NOT EXISTS `trg_digital_asset_workflow_autostamp_started`
AFTER UPDATE OF `digital_asset_workflow_status` ON `digital_asset_workflow`
WHEN NEW.`digital_asset_workflow_status` = 'doing' AND OLD.`digital_asset_workflow_status` != 'doing'
BEGIN
  UPDATE `digital_asset_workflow`
  SET `digital_asset_workflow_started_timestamp_ep_k` = COALESCE(`digital_asset_workflow_started_timestamp_ep_k`, (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))),
      `digital_asset_workflow_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `digital_asset_workflow_id` = NEW.`digital_asset_workflow_id`;
END;

-- BREAK
-- BREAK

CREATE TRIGGER IF NOT EXISTS `trg_digital_asset_workflow_autostamp_finished`
AFTER UPDATE OF `digital_asset_workflow_status` ON `digital_asset_workflow`
WHEN NEW.`digital_asset_workflow_status` IN ('done','failed','skipped') AND OLD.`digital_asset_workflow_status` NOT IN ('done','failed','skipped')
BEGIN
  UPDATE `digital_asset_workflow`
  SET `digital_asset_workflow_finished_timestamp_ep_k` = COALESCE(`digital_asset_workflow_finished_timestamp_ep_k`, (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))),
      `digital_asset_workflow_modified_timestamp_ep_k` = (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
  WHERE `digital_asset_workflow_id` = NEW.`digital_asset_workflow_id`;
END;

-- BREAK
-- BREAK

-- Prevent cycles in digital asset derivations (parent cannot be descendant of child)
CREATE TRIGGER IF NOT EXISTS `trg_digital_asset_derivations_no_cycles`
BEFORE INSERT ON `digital_asset_derivations`
BEGIN
  SELECT CASE
    WHEN EXISTS (
      WITH RECURSIVE `anc`(`id`) AS (
        SELECT NEW.`digital_asset_derivation_parent_digital_asset_id`
        UNION ALL
        SELECT `dad`.`digital_asset_derivation_parent_digital_asset_id`
        FROM `digital_asset_derivations` `dad`
        JOIN `anc` ON `dad`.`digital_asset_derivation_child_digital_asset_id` = `anc`.`id`
      )
      SELECT 1 FROM `anc` WHERE `id` = NEW.`digital_asset_derivation_child_digital_asset_id` LIMIT 1
    )
    THEN RAISE(ABORT, 'digital_asset_derivations cycle detected (cannot create a loop)')
  END;
END;

-- BREAK
