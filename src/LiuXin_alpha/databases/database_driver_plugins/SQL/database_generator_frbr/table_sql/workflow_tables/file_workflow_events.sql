-- BREAK

-- =====================================================
-- 2) FILE WORKFLOW: event log (audit/provenance)
-- =====================================================

CREATE TABLE IF NOT EXISTS file_workflow_events (
  file_workflow_event_id INTEGER PRIMARY KEY,

  file_workflow_event_file_id INTEGER NOT NULL,
  file_workflow_event_step_id INTEGER NOT NULL,

  file_workflow_event_from_status TEXT NULL,
  file_workflow_event_to_status   TEXT NOT NULL,

  file_workflow_event_actor TEXT NULL,      -- user/tool name
  file_workflow_event_note  TEXT NULL,

  file_workflow_event_tool TEXT NULL,       -- 'ocr_engine_v2', 'hash_pass', 'convert_epub', ...
  file_workflow_event_run_id TEXT NULL,     -- correlate batch jobs

  file_workflow_event_created_datestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
  file_workflow_event_datestamp INTEGER DEFAULT (STRFTIME('%s','now')),

  file_workflow_event_scratch TEXT NULL,

  CONSTRAINT file_workflow_events_file_fk
    FOREIGN KEY (file_workflow_event_file_id)
    REFERENCES files(file_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT file_workflow_events_step_fk
    FOREIGN KEY (file_workflow_event_step_id)
    REFERENCES workflow_steps(workflow_step_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT file_workflow_events_status_check
    CHECK (
      file_workflow_event_to_status IN ('todo','doing','done','blocked','skipped','failed') AND
      (file_workflow_event_from_status IS NULL OR file_workflow_event_from_status IN ('todo','doing','done','blocked','skipped','failed'))
    )
);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS idx_file_workflow_events_file_step_time
ON file_workflow_events(file_workflow_event_file_id, file_workflow_event_step_id, file_workflow_event_datestamp);

-- BREAK
