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

  `item_identifier_item_id` INT NOT NULL,
  `item_identifier_scheme`  TEXT NOT NULL,   -- 'isbn13', 'isbn10', 'asin', 'barcode', ...
  `item_identifier_value`   TEXT NOT NULL,
  `item_identifier_source`  TEXT NULL,       -- 'scan', 'file_metadata', 'calibre', ...

  `item_identifier_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `item_identifier_created_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),

  `item_identifier_scratch` TEXT NULL,

  CONSTRAINT `item_identifier_item_fk`
    FOREIGN KEY (`item_identifier_item_id`)
    REFERENCES `items` (`item_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS `idx_item_identifiers_lookup`
ON `item_identifiers` (`item_identifier_scheme`, `item_identifier_value`);

CREATE INDEX IF NOT EXISTS `idx_item_identifiers_item`
ON `item_identifiers` (`item_identifier_item_id`);

-- BREAK
