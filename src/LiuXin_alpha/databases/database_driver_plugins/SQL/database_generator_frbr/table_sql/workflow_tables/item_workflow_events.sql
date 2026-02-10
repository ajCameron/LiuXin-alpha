

-- BREAK

-- =====================================================
-- OPTIONAL: EVENT LOG (audit/provenance/debug)
-- =====================================================

CREATE TABLE IF NOT EXISTS item_workflow_events (
  item_workflow_event_id INTEGER PRIMARY KEY,

  item_workflow_event_item_id INTEGER NOT NULL,
  item_workflow_event_step_id INTEGER NOT NULL,

  item_workflow_event_from_status TEXT NULL,
  item_workflow_event_to_status   TEXT NOT NULL,

  item_workflow_event_actor TEXT NULL,        -- user/tool name
  item_workflow_event_note  TEXT NULL,

  -- Optional: store tool context deterministically
  item_workflow_event_tool TEXT NULL,         -- 'ocr_engine_v2', 'dedupe_pass_1', etc.
  item_workflow_event_run_id TEXT NULL,       -- correlate a batch run


  -- timestamps (epoch_ms)
  item_workflow_event_created_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  item_workflow_event_modified_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  item_workflow_event_source_created_datestamp_ep_k INTEGER NULL,
  item_workflow_event_source_modified_datestamp_ep_k INTEGER NULL,
  item_workflow_event_scratch TEXT NULL,

  CONSTRAINT item_workflow_events_item_fk
    FOREIGN KEY (item_workflow_event_item_id)
    REFERENCES items(item_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT item_workflow_events_step_fk
    FOREIGN KEY (item_workflow_event_step_id)
    REFERENCES workflow_steps(workflow_step_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT item_workflow_events_status_check
    CHECK (
      item_workflow_event_to_status IN ('todo','doing','done','blocked','skipped','failed') AND
      (item_workflow_event_from_status IS NULL OR item_workflow_event_from_status IN ('todo','doing','done','blocked','skipped','failed'))
    )
);


-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS idx_item_workflow_events_item_step_time
ON item_workflow_events(item_workflow_event_item_id, item_workflow_event_step_id, item_workflow_event_created_timestamp_ep_k);


-- BREAK
-- BREAK


-- =====================================================
-- TRIGGERS: keep state table + event log consistent (tight screws)
-- =====================================================

-- On INSERT to item_workflow, record an initial event
CREATE TRIGGER IF NOT EXISTS trg_item_workflow_insert_event
AFTER INSERT ON item_workflow
BEGIN
  INSERT INTO item_workflow_events(
    item_workflow_event_item_id,
    item_workflow_event_step_id,
    item_workflow_event_from_status,
    item_workflow_event_to_status,
    item_workflow_event_actor,
    item_workflow_event_note
  )
  VALUES(
    NEW.item_workflow_item_id,
    NEW.item_workflow_step_id,
    NULL,
    NEW.item_workflow_status,
    NEW.item_workflow_assigned_to,
    'initial state'
  );
END;


-- BREAK
-- BREAK


-- On status change, emit an event
CREATE TRIGGER IF NOT EXISTS trg_item_workflow_status_change_event
AFTER UPDATE OF item_workflow_status ON item_workflow
WHEN NEW.item_workflow_status != OLD.item_workflow_status
BEGIN
  INSERT INTO item_workflow_events(
    item_workflow_event_item_id,
    item_workflow_event_step_id,
    item_workflow_event_from_status,
    item_workflow_event_to_status,
    item_workflow_event_actor,
    item_workflow_event_note
  )
  VALUES(
    NEW.item_workflow_item_id,
    NEW.item_workflow_step_id,
    OLD.item_workflow_status,
    NEW.item_workflow_status,
    NEW.item_workflow_assigned_to,
    NEW.item_workflow_reason
  );
END;


-- BREAK
