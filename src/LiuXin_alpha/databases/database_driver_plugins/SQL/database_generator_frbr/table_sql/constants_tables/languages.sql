-- BREAK

-- -----------------------------------------------------
-- Table `languages`
-- -----------------------------------------------------
--
-- A locked constant table seeded by the FRBR generator.
--
-- We keep `language` + `language_code` for legacy compatibility:
--   * `language`      = English display name
--   * `language_code` = canonical ISO-639-2/B code (3-letter)
--
-- For canonicalisation, we also store:
--   * ISO-639-1 (2-letter) where it exists
--   * ISO-639-2/T (terminology) where it differs
--   * BCP-47 primary language tag (base tag)
--   * A small set of common BCP-47 variants for convenience
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `languages` (
  `language_id` INTEGER PRIMARY KEY,

  -- Human name / legacy field
  `language` TEXT NULL,

  -- Canonical code (legacy field; stable, queryable)
  `language_code` TEXT NOT NULL,

  -- Code variants
  `language_iso639_1` TEXT NULL,
  `language_iso639_2_b` TEXT NOT NULL,
  `language_iso639_2_t` TEXT NULL,

  -- Tags
  `language_bcp47_primary` TEXT NOT NULL,
  `language_bcp47_variants` TEXT NULL,

  -- timestamps (epoch_ms)
  `language_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `language_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `language_source_created_datestamp_ep_k` INTEGER NULL,
  `language_source_modified_datestamp_ep_k` INTEGER NULL,

  `language_scratch` TEXT NULL,

  CONSTRAINT `language_code_unique`
    UNIQUE (`language_code`),
  CONSTRAINT `language_iso639_2_b_unique`
    UNIQUE (`language_iso639_2_b`),
  CONSTRAINT `language_bcp47_primary_unique`
    UNIQUE (`language_bcp47_primary`)
)
;

-- BREAK