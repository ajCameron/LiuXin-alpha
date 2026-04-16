-- BREAK

-- =====================================================
-- 2) FILE WORKFLOW: event log (audit/provenance)
-- =====================================================

CREATE TABLE IF NOT EXISTS `digital_asset_workflow_events` (
  `digital_asset_workflow_event_id` INTEGER PRIMARY KEY,

  -- NOTE: kept nullable so DriverWrapper.get_blank_row() can insert a placeholder row.
  -- Application logic can enforce presence later.
  `digital_asset_workflow_event_digital_asset_id` INTEGER NULL,
  `digital_asset_workflow_event_step_id` INTEGER NULL,

  `digital_asset_workflow_event_from_status` TEXT NULL,
  `digital_asset_workflow_event_to_status`   TEXT NULL,

  `digital_asset_workflow_event_actor` TEXT NULL,      -- user/tool name
  `digital_asset_workflow_event_note`  TEXT NULL,

  `digital_asset_workflow_event_tool` TEXT NULL,       -- 'ocr_engine_v2', 'hash_pass', 'convert_epub', ...
  `digital_asset_workflow_event_run_id` TEXT NULL,     -- correlate batch jobs


  -- timestamps (epoch_ms)
  `digital_asset_workflow_event_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `digital_asset_workflow_event_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `digital_asset_workflow_event_source_created_datestamp_ep_k` INTEGER NULL,
  `digital_asset_workflow_event_source_modified_datestamp_ep_k` INTEGER NULL,
  `digital_asset_workflow_event_scratch` TEXT NULL,

  CONSTRAINT `digital_asset_workflow_events_digital_asset_fk`
    FOREIGN KEY (`digital_asset_workflow_event_digital_asset_id`)
    REFERENCES `digital_assets`(`digital_asset_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `digital_asset_workflow_events_step_fk`
    FOREIGN KEY (`digital_asset_workflow_event_step_id`)
    REFERENCES `workflow_steps`(`workflow_step_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `digital_asset_workflow_events_status_check`
    CHECK (
      `digital_asset_workflow_event_to_status` IN ('todo','doing','done','blocked','skipped','failed') AND
      (`digital_asset_workflow_event_from_status` IS NULL OR `digital_asset_workflow_event_from_status` IN ('todo','doing','done','blocked','skipped','failed'))
    )
);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_digital_asset_workflow_events_file_step_time`
ON `digital_asset_workflow_events`(`digital_asset_workflow_event_digital_asset_id`, `digital_asset_workflow_event_step_id`, `digital_asset_workflow_event_created_timestamp_ep_k`);

-- BREAK
