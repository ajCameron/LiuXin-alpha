
-- BREAK


CREATE TABLE IF NOT EXISTS `workflow_states` (
  `workflow_state_id` INTEGER PRIMARY KEY,

  `workflow_state_code` TEXT NOT NULL UNIQUE,      -- 'needs-metadata','ocr-pending','dedupe-review', ...
  `workflow_state_label` TEXT NOT NULL,

  `workflow_state_description` TEXT NULL,

  `workflow_state_is_terminal` INTEGER NOT NULL DEFAULT 0,

  -- timestamps (epoch_ms)
  `workflow_state_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `workflow_state_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `workflow_state_source_created_datestamp_ep_k` INTEGER NULL,
  `workflow_state_source_modified_datestamp_ep_k` INTEGER NULL,
  CONSTRAINT `workflow_state_is_terminal_bool` CHECK (workflow_state_is_terminal IN (0,1))
);

-- BREAK