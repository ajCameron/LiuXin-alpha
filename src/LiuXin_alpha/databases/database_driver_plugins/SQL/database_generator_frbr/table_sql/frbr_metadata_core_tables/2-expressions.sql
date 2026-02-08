-- BREAK


-- -----------------------------------------------------
-- Table `expressions`  (FRBR Expression)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `expressions` (
  `expression_id` INTEGER PRIMARY KEY,

  -- Relation to work
  `expression_work_id` INTEGER NOT NULL,

  -- Core identity
  `expression_type` TEXT NULL,               -- 'text', 'translation', 'revision', 'dub', ...
  `expression_label` TEXT NULL,              -- "French translation by X", "Director's Cut"
  `expression_year` INTEGER NULL,
  `expression_is_preferred` INTEGER NULL,        -- 1 = preferred, 0 = not, NULL = unknown

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

    -- timestamps (display DATETIME + epoch_ms source)
  `expression_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `expression_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `expression_scratch` TEXT NULL,

  CONSTRAINT `expression_work_fk`
    FOREIGN KEY (`expression_work_id`)
    REFERENCES `works` (`work_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE

);
-- BREAK