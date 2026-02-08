
-- BREAK

-- =====================================================
-- 6) TRIGGERS: keep workflow scope sane + event logging + timestamps
-- =====================================================

-- Ensure file_workflow_step is usable for file scope
CREATE TRIGGER IF NOT EXISTS trg_file_workflow_step_scope_check
BEFORE INSERT ON file_workflow
BEGIN
  SELECT CASE
    WHEN EXISTS (
      SELECT 1 FROM workflow_steps
      WHERE workflow_step_id = NEW.file_workflow_step_id
        AND workflow_step_scope IS NOT NULL
        AND workflow_step_scope NOT IN ('file','both')
      LIMIT 1
    )
    THEN RAISE(ABORT, 'file_workflow step is not scoped for file/both')
  END;
END;

-- BREAK
-- BREAK

CREATE TRIGGER IF NOT EXISTS trg_file_workflow_step_scope_check_upd
BEFORE UPDATE OF file_workflow_step_id ON file_workflow
BEGIN
  SELECT CASE
    WHEN EXISTS (
      SELECT 1 FROM workflow_steps
      WHERE workflow_step_id = NEW.file_workflow_step_id
        AND workflow_step_scope IS NOT NULL
        AND workflow_step_scope NOT IN ('file','both')
      LIMIT 1
    )
    THEN RAISE(ABORT, 'file_workflow step is not scoped for file/both')
  END;
END;

-- BREAK
-- BREAK

-- Event on INSERT (initial state)
CREATE TRIGGER IF NOT EXISTS trg_file_workflow_insert_event
AFTER INSERT ON file_workflow
BEGIN
  INSERT INTO file_workflow_events(
    file_workflow_event_file_id,
    file_workflow_event_step_id,
    file_workflow_event_from_status,
    file_workflow_event_to_status,
    file_workflow_event_actor,
    file_workflow_event_note
  )
  VALUES(
    NEW.file_workflow_file_id,
    NEW.file_workflow_step_id,
    NULL,
    NEW.file_workflow_status,
    NEW.file_workflow_assigned_to,
    'initial state'
  );
END;

-- BREAK
-- BREAK

-- Status change → event
CREATE TRIGGER IF NOT EXISTS trg_file_workflow_status_change_event
AFTER UPDATE OF file_workflow_status ON file_workflow
WHEN NEW.file_workflow_status != OLD.file_workflow_status
BEGIN
  INSERT INTO file_workflow_events(
    file_workflow_event_file_id,
    file_workflow_event_step_id,
    file_workflow_event_from_status,
    file_workflow_event_to_status,
    file_workflow_event_actor,
    file_workflow_event_note
  )
  VALUES(
    NEW.file_workflow_file_id,
    NEW.file_workflow_step_id,
    OLD.file_workflow_status,
    NEW.file_workflow_status,
    NEW.file_workflow_assigned_to,
    NEW.file_workflow_reason
  );
END;

-- BREAK
-- BREAK

-- Auto-set started/finished timestamps based on status transitions
CREATE TRIGGER IF NOT EXISTS trg_file_workflow_autostamp_started
AFTER UPDATE OF file_workflow_status ON file_workflow
WHEN NEW.file_workflow_status = 'doing' AND OLD.file_workflow_status != 'doing'
BEGIN
  UPDATE file_workflow
  SET file_workflow_started_datestamp = COALESCE(file_workflow_started_datestamp, CURRENT_TIMESTAMP),
      file_workflow_last_modified = CURRENT_TIMESTAMP
  WHERE file_workflow_id = NEW.file_workflow_id;
END;

-- BREAK
-- BREAK

CREATE TRIGGER IF NOT EXISTS trg_file_workflow_autostamp_finished
AFTER UPDATE OF file_workflow_status ON file_workflow
WHEN NEW.file_workflow_status IN ('done','failed','skipped') AND OLD.file_workflow_status NOT IN ('done','failed','skipped')
BEGIN
  UPDATE file_workflow
  SET file_workflow_finished_datestamp = COALESCE(file_workflow_finished_datestamp, CURRENT_TIMESTAMP),
      file_workflow_last_modified = CURRENT_TIMESTAMP
  WHERE file_workflow_id = NEW.file_workflow_id;
END;

-- BREAK
-- BREAK

-- Prevent cycles in file derivations (parent cannot be descendant of child)
CREATE TRIGGER IF NOT EXISTS trg_file_derivations_no_cycles
BEFORE INSERT ON file_derivations
BEGIN
  SELECT CASE
    WHEN EXISTS (
      WITH RECURSIVE anc(id) AS (
        SELECT NEW.file_derivation_parent_file_id
        UNION ALL
        SELECT fd.file_derivation_parent_file_id
        FROM file_derivations fd
        JOIN anc ON fd.file_derivation_child_file_id = anc.id
      )
      SELECT 1 FROM anc WHERE id = NEW.file_derivation_child_file_id LIMIT 1
    )
    THEN RAISE(ABORT, 'file_derivations cycle detected (cannot create a loop)')
  END;
END;

-- BREAK
-- BREAK


-- Enforce (by default) that parent and child files belong to the same item
-- (If you later want cross-item derivations, we can add a flag column to allow it explicitly.)
CREATE TRIGGER IF NOT EXISTS trg_file_derivations_same_item
BEFORE INSERT ON file_derivations
BEGIN
  SELECT CASE
    WHEN (SELECT file_item_id FROM files WHERE file_id = NEW.file_derivation_parent_file_id)
       != (SELECT file_item_id FROM files WHERE file_id = NEW.file_derivation_child_file_id)
    THEN RAISE(ABORT, 'file_derivations must stay within the same item (parent/child file_item_id mismatch)')
  END;
END;

-- BREAK