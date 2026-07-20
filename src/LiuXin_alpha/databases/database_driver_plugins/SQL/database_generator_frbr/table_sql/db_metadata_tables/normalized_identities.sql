-- BREAK

-- Relation-level declarations for display values whose normalized form is a
-- row identity.  This is intentionally separate from column_metadata:
-- case-insensitive comparison does not imply uniqueness.
CREATE TABLE IF NOT EXISTS `normalized_identities` (
  `normalized_identity_table_name` TEXT NOT NULL,
  `normalized_identity_value_column` TEXT NOT NULL,
  `normalized_identity_key_column` TEXT NOT NULL,
  `normalized_identity_normalization_profile` TEXT NOT NULL,
  `normalized_identity_scope_columns_json` TEXT NOT NULL DEFAULT '[]',
  `normalized_identity_unique` INTEGER NOT NULL DEFAULT 1,

  CONSTRAINT `normalized_identity_declaration_pk`
    PRIMARY KEY (`normalized_identity_table_name`, `normalized_identity_value_column`),
  CONSTRAINT `normalized_identity_unique_bool`
    CHECK (`normalized_identity_unique` IN (0, 1)),
  CONSTRAINT `normalized_identity_profile_allowed`
    CHECK (`normalized_identity_normalization_profile` IN (
      'none',
      'unicode_nfc',
      'unicode_nfc_trim_casefold',
      'tag_search_term',
      'title_search_term'
    ))
);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_normalized_identities_key_column`
ON `normalized_identities` (
  `normalized_identity_table_name`,
  `normalized_identity_key_column`
);

-- BREAK
