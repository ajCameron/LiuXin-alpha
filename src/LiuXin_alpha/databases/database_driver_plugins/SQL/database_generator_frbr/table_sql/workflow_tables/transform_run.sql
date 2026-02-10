
-- =====================================================
-- 3) TRANSFORM RUNS (what happened, with parameters and outcomes)
-- =====================================================

CREATE TABLE IF NOT EXISTS `transform_runs` (
  `transform_run_id` INTEGER PRIMARY KEY,

  `transform_run_type` TEXT NOT NULL,        -- 'ocr','convert','thumbnail','hash','dedupe','repair','extract', ...
  `transform_run_tool` TEXT NULL,            -- executable/tool name/version
  `transform_run_profile` TEXT NULL,         -- named preset ('epub->kepub', 'ocr-fast', ...)
  `transform_run_params` TEXT NULL,          -- JSON string OK here: deterministic tool params
  `transform_run_params_hash` TEXT NULL,     -- hash for caching / replay detection

  `transform_run_actor` TEXT NULL,           -- who/what initiated it
  `transform_run_status` TEXT NOT NULL DEFAULT 'started', -- 'started','succeeded','failed','aborted'
  `transform_run_error` TEXT NULL,


  -- timestamps (epoch_ms)
  `transform_run_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `transform_run_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `transform_run_source_created_datestamp_ep_k` INTEGER NULL,
  `transform_run_source_modified_datestamp_ep_k` INTEGER NULL,
  `transform_run_started_timestamp_ep_k` INTEGER NULL,
  `transform_run_finished_timestamp_ep_k` INTEGER NULL,

  `transform_run_scratch` TEXT NULL,

  CONSTRAINT `transform_run_status_check`
    CHECK (`transform_run_status` IN ('started','succeeded','failed','aborted'))

    );

CREATE INDEX IF NOT EXISTS `idx_transform_runs_type_status`
ON `transform_runs`(`transform_run_type`, `transform_run_status`);

