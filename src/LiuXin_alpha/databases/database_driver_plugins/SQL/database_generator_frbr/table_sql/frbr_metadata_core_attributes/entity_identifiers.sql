
-- BREAK

-- -----------------------------------------------------
-- Table `entity_identifiers`
-- Curated/derived identifiers for any entity (Work/Expression/Manifestation/Item)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `entity_identifiers` (
  `entity_identifier_id` INTEGER PRIMARY KEY,

  `entity_identifier_entity_type` TEXT NOT NULL,  -- 'work', 'expression', 'manifestation', 'item'
  `entity_identifier_entity_id`   INT NOT NULL,
  `entity_identifier_scheme`      TEXT NOT NULL,  -- 'isbn13', 'asin', 'uuid', ...
  `entity_identifier_value`       TEXT NOT NULL,
  `entity_identifier_is_primary`  INT NULL,       -- 1 = canonical for this entity/scheme
  `entity_identifier_provenance`  TEXT NULL,      -- 'derived_from_items', 'import', 'manual'

  `entity_identifier_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `entity_identifier_created_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),

    -- timestamps
  `entity_identifier_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `entity_identifier_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),


  `entity_identifier_scratch` TEXT NULL


);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_entity_identifiers_lookup`
ON `entity_identifiers` (`entity_identifier_scheme`, `entity_identifier_value`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_entity_identifiers_entity`
ON `entity_identifiers` (`entity_identifier_entity_type`, `entity_identifier_entity_id`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_entity_identifiers_primary`
ON `entity_identifiers` (`entity_identifier_entity_type`, `entity_identifier_entity_id`, `entity_identifier_scheme`, `entity_identifier_is_primary`);

-- BREAK