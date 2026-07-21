-- BREAK

-- -----------------------------------------------------
-- Table `column_metadata`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `column_metadata` (
  `column_metadata_id` INTEGER PRIMARY KEY,
  `column_metadata_table_name` TEXT NOT NULL,
  `column_metadata_column_name` TEXT NOT NULL,
  `column_metadata_case_sensitive` INTEGER NOT NULL DEFAULT 1,
  `column_metadata_semantic_role` TEXT NOT NULL DEFAULT 'machine_value',
  `column_metadata_normalization_profile` TEXT NOT NULL DEFAULT 'none',
  `column_metadata_comparison_column` TEXT NULL,
  `column_metadata_empty_value_policy` TEXT NOT NULL DEFAULT 'null_is_missing',
  `column_metadata_merge_policy` TEXT NOT NULL DEFAULT 'replace',
  `column_metadata_validation_profile` TEXT NOT NULL DEFAULT 'none',
  `column_metadata_formatting_options_json` TEXT NOT NULL DEFAULT '{}',
  `column_metadata_display_options_json` TEXT NOT NULL DEFAULT '{}',

  `column_metadata_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `column_metadata_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `column_metadata_scratch` TEXT NULL,

  CONSTRAINT `column_metadata_case_sensitive_bool`
    CHECK (`column_metadata_case_sensitive` IN (0, 1)),
  CONSTRAINT `column_metadata_semantic_role_allowed`
    CHECK (`column_metadata_semantic_role` IN (
      'machine_value',
      'identifier',
      'relationship_key',
      'code',
      'boolean',
      'number',
      'ordering',
      'date_time',
      'locator',
      'structured_data',
      'hash',
      'normalized_key',
      'provenance',
      'scratch',
      'display_name',
      'title',
      'label',
      'sort_key',
      'taxonomy_term',
      'verbatim_text',
      'resource_name'
    )),
  CONSTRAINT `column_metadata_normalization_profile_allowed`
    CHECK (`column_metadata_normalization_profile` IN (
      'none',
      'unicode_nfc',
      'unicode_nfc_trim_casefold',
      'tag_search_term',
      'title_search_term'
    )),
  CONSTRAINT `column_metadata_empty_value_policy_allowed`
    CHECK (`column_metadata_empty_value_policy` IN (
      'null_is_missing',
      'null_or_blank_is_missing',
      'preserve'
    )),
  CONSTRAINT `column_metadata_merge_policy_allowed`
    CHECK (`column_metadata_merge_policy` IN (
      'replace',
      'set_union',
      'append',
      'preserve_existing'
    )),
  CONSTRAINT `column_metadata_validation_profile_allowed`
    CHECK (`column_metadata_validation_profile` IN (
        'none',
        'identifier',
        'code',
        'boolean',
        'number',
        'date_time',
        'locator',
        'json',
        'hash',
        'normalized_key',
        'display_text',
        'taxonomy_term',
        'verbatim_text',
        'resource_name'
      )
    ),
  CONSTRAINT `column_metadata_table_column_unique`
    UNIQUE (`column_metadata_table_name`, `column_metadata_column_name`)
);

-- BREAK
