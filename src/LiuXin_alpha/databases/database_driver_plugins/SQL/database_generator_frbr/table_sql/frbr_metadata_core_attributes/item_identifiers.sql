-- BREAK


-- =====================================================
-- IDENTIFIERS (Hybrid: observed on items + curated per entity)
-- =====================================================

-- -----------------------------------------------------
-- Table `item_identifiers`
-- Raw identifiers observed on specific Items
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `item_identifiers` (
  `item_identifier_id` INTEGER PRIMARY KEY,

  -- NOTE: kept nullable so DriverWrapper.get_blank_row() can insert a placeholder row.
  -- Application logic can enforce presence later.
  `item_identifier_item_id` INTEGER NULL,
  `item_identifier_scheme`  TEXT NULL,   -- 'isbn13', 'isbn10', 'asin', 'barcode', ...
  `item_identifier_value`   TEXT NULL,
  `item_identifier_source`  TEXT NULL,       -- 'scan', 'file_metadata', 'calibre', ...


      -- timestamps (epoch_ms)
  `item_identifier_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `item_identifier_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `item_identifier_source_created_datestamp_ep_k` INTEGER NULL,
  `item_identifier_source_modified_datestamp_ep_k` INTEGER NULL,

  `item_identifier_scratch` TEXT NULL,

  CONSTRAINT `item_identifier_item_fk`
    FOREIGN KEY (`item_identifier_item_id`)
    REFERENCES `items` (`item_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE

);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_item_identifiers_lookup`
ON `item_identifiers` (`item_identifier_scheme`, `item_identifier_value`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_item_identifiers_item`
ON `item_identifiers` (`item_identifier_item_id`);

-- BREAK
