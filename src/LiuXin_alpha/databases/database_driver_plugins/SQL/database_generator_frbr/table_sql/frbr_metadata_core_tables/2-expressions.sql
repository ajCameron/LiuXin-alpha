-- BREAK


-- -----------------------------------------------------
-- Table `expressions`  (FRBR Expression)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `expressions` (
  `expression_id` INTEGER PRIMARY KEY,

  -- Relation to works is via `expression_work_links` (many-to-many)

  -- Core identity
  `expression_type` TEXT NULL,               -- 'text', 'translation', 'revision', 'dub', ...
  `expression_label` TEXT NULL,              -- "French translation by X", "Director's Cut"
  `expression_year` INTEGER NULL,
  `expression_is_preferred` INTEGER NULL,        -- 1 = preferred, 0 = not, NULL = unknown

  `expression_original_date` INTEGER NULL,
  `expression_original_copyright_date` TEXT NULL,

  `expression_flags` TEXT NULL,

  -- Language & mode
  `expression_language_id` INTEGER NULL,         -- FK later (languages)
  `expression_mode` TEXT NULL,               -- 'text', 'spoken_word', 'moving_image', 'music', 'mixed'

  -- Titles (generally formed from Work title; override only when truly different)
  `expression_title_override` TEXT NULL,
  `expression_subtitle` TEXT NULL,

  -- Text-centric details
  `expression_wordcount` INTEGER NULL,
  `expression_fiction_length_category` TEXT NULL, -- 'short_story', 'novella', 'novel', ...

  -- AV-centric details
  `expression_cut_type` TEXT NULL,           -- 'theatrical', 'director_cut', 'extended', ...
  `expression_nominal_duration_seconds` INTEGER NULL,

  -- Status / provenance
  `expression_status` TEXT NULL,             -- 'complete', 'fragment', 'draft', ...
  `expression_origin_note` TEXT NULL,

    -- timestamps (epoch_ms)
  `expression_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `expression_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `expression_source_created_datestamp_ep_k` INTEGER NULL,
  `expression_source_modified_datestamp_ep_k` INTEGER NULL,

  `expression_scratch` TEXT NULL,

  CONSTRAINT `expression_language_fk`
    FOREIGN KEY (`expression_language_id`)
    REFERENCES `languages` (`language_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE

);
-- BREAK
-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_expressions_language_id`
ON `expressions` (`expression_language_id`);

-- BREAK
