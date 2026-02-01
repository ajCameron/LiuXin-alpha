-- BREAK

-- =====================================================
-- FRBR CORE (WEMI)
-- =====================================================

-- -----------------------------------------------------
-- Table `works`  (FRBR Work)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `works` (
  `work_id` INTEGER PRIMARY KEY,

  -- Core identity
  `work_type` TEXT NULL,                -- 'novel', 'short_story', 'collection', 'film', 'tv_series', ...
  `work_medium` TEXT NULL,              -- 'text', 'audio', 'moving_image', 'mixed'
  `work_canonical_title` TEXT NULL,
  `work_sort_title` TEXT NULL,

  -- Original context
  `work_original_language_id` INT NULL, -- FK later (languages), kept as INT for compatibility
  `work_original_year` INT NULL,

  -- High-level classification
  `work_is_fiction` INT NULL,           -- 1 = fiction, 0 = non-fiction, NULL = unknown
  `work_audience` TEXT NULL,            -- 'adult', 'ya', 'children', 'all_ages'
  `work_completion_status` TEXT NULL,   -- 'complete', 'ongoing', 'abandoned', 'one_shot'

  -- Concept-level provenance / notes
  `work_discovery_note` TEXT NULL,

  -- Timestamps
  `work_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `work_created_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),

  `work_scratch` TEXT NULL
);
-- BREAK