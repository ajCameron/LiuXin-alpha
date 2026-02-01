

CREATE TABLE IF NOT EXISTS workflow_states (
  workflow_state_id INTEGER PRIMARY KEY,
  workflow_code TEXT NOT NULL UNIQUE,      -- 'needs-metadata','ocr-pending','dedupe-review', ...
  workflow_label TEXT NOT NULL,
  workflow_description TEXT NULL,
  workflow_is_terminal INTEGER NOT NULL DEFAULT 0,
  CONSTRAINT workflow_is_terminal_bool CHECK (workflow_is_terminal IN (0,1))
);


PRAGMA foreign_keys = ON;

-- =====================================================
-- WORKFLOW DEFINITIONS
-- =====================================================

CREATE TABLE IF NOT EXISTS workflow_steps (
  workflow_step_id INTEGER PRIMARY KEY,

  -- Stable identifier used by code (e.g. 'ocr', 'dedupe', 'cover', 'metadata', 'reencode_epub')
  workflow_step_code TEXT NOT NULL UNIQUE,

  -- Human label
  workflow_step_label TEXT NOT NULL,

  -- Optional grouping (e.g. 'ingest', 'quality', 'derivation', 'export')
  workflow_step_group TEXT NULL,

  -- If TRUE, step must be done for the item to be "complete"
  workflow_step_is_required INTEGER NOT NULL DEFAULT 0,

  -- If TRUE, user/tool can "skip" this step explicitly
  workflow_step_is_skippable INTEGER NOT NULL DEFAULT 1,

  -- Optional ordering hint for UI
  workflow_step_ord INTEGER NULL,

  workflow_step_created_datestamp DATETIME DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT workflow_steps_bool_check
    CHECK (workflow_step_is_required IN (0,1) AND workflow_step_is_skippable IN (0,1)),

  CONSTRAINT workflow_steps_ord_check
    CHECK (workflow_step_ord IS NULL OR workflow_step_ord >= 0)
);

CREATE INDEX IF NOT EXISTS idx_workflow_steps_group_ord
ON workflow_steps(workflow_step_group, workflow_step_ord);

-- =====================================================
-- PER-ITEM WORKFLOW STATE (materialized "current state")
-- =====================================================

-- Status vocabulary: 'todo', 'doing', 'done', 'blocked', 'skipped', 'failed'
CREATE TABLE IF NOT EXISTS item_workflow (
  item_workflow_id INTEGER PRIMARY KEY,

  item_workflow_item_id INTEGER NOT NULL,
  item_workflow_step_id INTEGER NOT NULL,

  item_workflow_status TEXT NOT NULL DEFAULT 'todo',

  -- lightweight fields for assignment / triage
  item_workflow_priority INTEGER NOT NULL DEFAULT 0,
  item_workflow_assigned_to TEXT NULL,     -- later: user_id/agent_id
  item_workflow_reason TEXT NULL,          -- why blocked/failed/skipped
  item_workflow_progress REAL NULL,        -- 0..1 for long-running tasks

  -- Useful timestamps
  item_workflow_created_datestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
  item_workflow_started_datestamp DATETIME NULL,
  item_workflow_finished_datestamp DATETIME NULL,
  item_workflow_datestamp INTEGER DEFAULT (STRFTIME('%s','now')),
  item_workflow_last_modified DATETIME DEFAULT CURRENT_TIMESTAMP,

  item_workflow_scratch TEXT NULL,

  CONSTRAINT item_workflow_item_fk
    FOREIGN KEY (item_workflow_item_id)
    REFERENCES items(item_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT item_workflow_step_fk
    FOREIGN KEY (item_workflow_step_id)
    REFERENCES workflow_steps(workflow_step_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT item_workflow_status_check
    CHECK (item_workflow_status IN ('todo','doing','done','blocked','skipped','failed')),

  CONSTRAINT item_workflow_priority_check
    CHECK (item_workflow_priority >= 0),

  CONSTRAINT item_workflow_progress_check
    CHECK (item_workflow_progress IS NULL OR (item_workflow_progress >= 0 AND item_workflow_progress <= 1))
);

-- One row per (item, step): the materialized current status
CREATE UNIQUE INDEX IF NOT EXISTS idx_item_workflow_unique
ON item_workflow(item_workflow_item_id, item_workflow_step_id);

CREATE INDEX IF NOT EXISTS idx_item_workflow_by_status
ON item_workflow(item_workflow_status, item_workflow_priority);

CREATE INDEX IF NOT EXISTS idx_item_workflow_by_item
ON item_workflow(item_workflow_item_id);

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

  item_workflow_event_created_datestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
  item_workflow_event_datestamp INTEGER DEFAULT (STRFTIME('%s','now')),

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

CREATE INDEX IF NOT EXISTS idx_item_workflow_events_item_step_time
ON item_workflow_events(item_workflow_event_item_id, item_workflow_event_step_id, item_workflow_event_datestamp);

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


PRAGMA foreign_keys = ON;

-- =====================================================
-- 0) ENHANCE workflow_steps to support scope (item/file/both)
-- =====================================================
-- If you already created workflow_steps, use ALTER TABLE.
-- Otherwise fold these columns into your CREATE TABLE for workflow_steps.

ALTER TABLE workflow_steps ADD COLUMN workflow_step_scope TEXT NULL;
-- suggested values: 'item','file','both'
-- NOTE: SQLite doesn't enforce CHECK on added columns retroactively; keep a trigger below.

CREATE TRIGGER IF NOT EXISTS trg_workflow_steps_scope_check
BEFORE INSERT ON workflow_steps
WHEN NEW.workflow_step_scope IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.workflow_step_scope NOT IN ('item','file','both')
    THEN RAISE(ABORT, 'workflow_steps.workflow_step_scope must be one of: item, file, both')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_workflow_steps_scope_check_upd
BEFORE UPDATE OF workflow_step_scope ON workflow_steps
WHEN NEW.workflow_step_scope IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.workflow_step_scope NOT IN ('item','file','both')
    THEN RAISE(ABORT, 'workflow_steps.workflow_step_scope must be one of: item, file, both')
  END;
END;


-- =====================================================
-- 1) FILE WORKFLOW: current state (materialized)
-- =====================================================

-- Status vocabulary: 'todo','doing','done','blocked','skipped','failed'
CREATE TABLE IF NOT EXISTS file_workflow (
  file_workflow_id INTEGER PRIMARY KEY,

  file_workflow_file_id INTEGER NOT NULL,
  file_workflow_step_id INTEGER NOT NULL,

  file_workflow_status TEXT NOT NULL DEFAULT 'todo',

  file_workflow_priority INTEGER NOT NULL DEFAULT 0,
  file_workflow_assigned_to TEXT NULL,        -- later: user_id / agent_id
  file_workflow_reason TEXT NULL,             -- why blocked/failed/skipped
  file_workflow_progress REAL NULL,           -- 0..1

  file_workflow_created_datestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
  file_workflow_started_datestamp DATETIME NULL,
  file_workflow_finished_datestamp DATETIME NULL,
  file_workflow_datestamp INTEGER DEFAULT (STRFTIME('%s','now')),
  file_workflow_last_modified DATETIME DEFAULT CURRENT_TIMESTAMP,

  file_workflow_scratch TEXT NULL,

  CONSTRAINT file_workflow_file_fk
    FOREIGN KEY (file_workflow_file_id)
    REFERENCES files(file_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT file_workflow_step_fk
    FOREIGN KEY (file_workflow_step_id)
    REFERENCES workflow_steps(workflow_step_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT file_workflow_status_check
    CHECK (file_workflow_status IN ('todo','doing','done','blocked','skipped','failed')),

  CONSTRAINT file_workflow_priority_check
    CHECK (file_workflow_priority >= 0),

  CONSTRAINT file_workflow_progress_check
    CHECK (file_workflow_progress IS NULL OR (file_workflow_progress >= 0 AND file_workflow_progress <= 1))
);

-- One row per (file, step)
CREATE UNIQUE INDEX IF NOT EXISTS idx_file_workflow_unique
ON file_workflow(file_workflow_file_id, file_workflow_step_id);

CREATE INDEX IF NOT EXISTS idx_file_workflow_by_status
ON file_workflow(file_workflow_status, file_workflow_priority);

CREATE INDEX IF NOT EXISTS idx_file_workflow_by_file
ON file_workflow(file_workflow_file_id);


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

CREATE INDEX IF NOT EXISTS idx_file_workflow_events_file_step_time
ON file_workflow_events(file_workflow_event_file_id, file_workflow_event_step_id, file_workflow_event_datestamp);


-- =====================================================
-- 3) TRANSFORM RUNS (what happened, with parameters and outcomes)
-- =====================================================

CREATE TABLE IF NOT EXISTS transform_runs (
  transform_run_id INTEGER PRIMARY KEY,

  transform_run_type TEXT NOT NULL,        -- 'ocr','convert','thumbnail','hash','dedupe','repair','extract', ...
  transform_run_tool TEXT NULL,            -- executable/tool name/version
  transform_run_profile TEXT NULL,         -- named preset ('epub->kepub', 'ocr-fast', ...)
  transform_run_params TEXT NULL,          -- JSON string OK here: deterministic tool params
  transform_run_params_hash TEXT NULL,     -- hash for caching / replay detection

  transform_run_actor TEXT NULL,           -- who/what initiated it
  transform_run_status TEXT NOT NULL DEFAULT 'started', -- 'started','succeeded','failed','aborted'
  transform_run_error TEXT NULL,

  transform_run_started_datestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
  transform_run_finished_datestamp DATETIME NULL,
  transform_run_datestamp INTEGER DEFAULT (STRFTIME('%s','now')),

  transform_run_scratch TEXT NULL,

  CONSTRAINT transform_run_status_check
    CHECK (transform_run_status IN ('started','succeeded','failed','aborted'))
);

CREATE INDEX IF NOT EXISTS idx_transform_runs_type_status
ON transform_runs(transform_run_type, transform_run_status);


-- =====================================================
-- 4) TRANSFORM INPUTS / OUTPUTS (many-to-many)
-- =====================================================

CREATE TABLE IF NOT EXISTS transform_run_inputs (
  transform_run_input_id INTEGER PRIMARY KEY,

  transform_run_input_run_id INTEGER NOT NULL,
  transform_run_input_file_id INTEGER NOT NULL,

  transform_run_input_role TEXT NULL,      -- 'primary','aux','sidecar','dictionary','cover_source', ...
  transform_run_input_note TEXT NULL,

  CONSTRAINT tri_run_fk
    FOREIGN KEY (transform_run_input_run_id)
    REFERENCES transform_runs(transform_run_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT tri_file_fk
    FOREIGN KEY (transform_run_input_file_id)
    REFERENCES files(file_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_transform_run_inputs_unique
ON transform_run_inputs(transform_run_input_run_id, transform_run_input_file_id);

CREATE INDEX IF NOT EXISTS idx_transform_run_inputs_file
ON transform_run_inputs(transform_run_input_file_id);


CREATE TABLE IF NOT EXISTS transform_run_outputs (
  transform_run_output_id INTEGER PRIMARY KEY,

  transform_run_output_run_id INTEGER NOT NULL,
  transform_run_output_file_id INTEGER NOT NULL,

  transform_run_output_role TEXT NULL,     -- 'primary','sidecar','thumbnail','log','report'
  transform_run_output_note TEXT NULL,

  CONSTRAINT tro_run_fk
    FOREIGN KEY (transform_run_output_run_id)
    REFERENCES transform_runs(transform_run_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT tro_file_fk
    FOREIGN KEY (transform_run_output_file_id)
    REFERENCES files(file_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_transform_run_outputs_unique
ON transform_run_outputs(transform_run_output_run_id, transform_run_output_file_id);

CREATE INDEX IF NOT EXISTS idx_transform_run_outputs_file
ON transform_run_outputs(transform_run_output_file_id);


-- =====================================================
-- 5) FILE LINEAGE (explicit derivation edges)
-- =====================================================

CREATE TABLE IF NOT EXISTS file_derivations (
  file_derivation_id INTEGER PRIMARY KEY,

  file_derivation_parent_file_id INTEGER NOT NULL,
  file_derivation_child_file_id  INTEGER NOT NULL,

  file_derivation_run_id INTEGER NULL,     -- optional: link to the transform run that produced it
  file_derivation_kind TEXT NULL,          -- 'converted','ocr_text','thumbnail','repacked','repaired','extracted', ...
  file_derivation_note TEXT NULL,

  file_derivation_created_datestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
  file_derivation_datestamp INTEGER DEFAULT (STRFTIME('%s','now')),

  file_derivation_scratch TEXT NULL,

  CONSTRAINT fd_parent_fk
    FOREIGN KEY (file_derivation_parent_file_id)
    REFERENCES files(file_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT fd_child_fk
    FOREIGN KEY (file_derivation_child_file_id)
    REFERENCES files(file_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT fd_run_fk
    FOREIGN KEY (file_derivation_run_id)
    REFERENCES transform_runs(transform_run_id)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT fd_no_self
    CHECK (file_derivation_parent_file_id != file_derivation_child_file_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_file_derivations_unique
ON file_derivations(file_derivation_parent_file_id, file_derivation_child_file_id);

CREATE INDEX IF NOT EXISTS idx_file_derivations_parent
ON file_derivations(file_derivation_parent_file_id);

CREATE INDEX IF NOT EXISTS idx_file_derivations_child
ON file_derivations(file_derivation_child_file_id);


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

CREATE TRIGGER IF NOT EXISTS trg_file_workflow_autostamp_finished
AFTER UPDATE OF file_workflow_status ON file_workflow
WHEN NEW.file_workflow_status IN ('done','failed','skipped') AND OLD.file_workflow_status NOT IN ('done','failed','skipped')
BEGIN
  UPDATE file_workflow
  SET file_workflow_finished_datestamp = COALESCE(file_workflow_finished_datestamp, CURRENT_TIMESTAMP),
      file_workflow_last_modified = CURRENT_TIMESTAMP
  WHERE file_workflow_id = NEW.file_workflow_id;
END;

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