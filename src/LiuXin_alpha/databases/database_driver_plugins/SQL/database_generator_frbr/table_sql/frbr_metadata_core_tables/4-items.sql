-- BREAK



-- -----------------------------------------------------
-- Table `items`  (FRBR Item / copy)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `items` (
  `item_id` INTEGER PRIMARY KEY,

  -- Relation to manifestation
  `item_manifestation_id` INT NOT NULL,

  -- Type of item
  `item_type` TEXT NULL,               -- 'digital', 'physical'

  -- Location / inventory
  `item_location` TEXT NULL,           -- shelf/box or logical location for digital
  `item_inventory_code` TEXT NULL,     -- barcode/internal code

  -- Source / provenance (per-copy)
  `item_source` TEXT NULL,             -- 'calibre', 'scan', 'web_dl', 'manual', ...
  `item_source_detail` TEXT NULL,      -- path/url/library name/seller/etc.

  -- Acquisition / lifecycle
  `item_acquired_date` TEXT NULL,      -- 'YYYY-MM-DD' or datetime
  `item_acquired_price_minor` INT NULL,
  `item_lifecycle_status` TEXT NULL,   -- 'active', 'withdrawn', 'lost', 'replaced', ...
  `item_condition` TEXT NULL,          -- 'fine', 'good', 'worn', 'damaged', ...

  -- timestamps (display DATETIME + epoch_ms source)
  item_created_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  item_created_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  item_modified_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  item_modified_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))

  `item_scratch` TEXT NULL,

  CONSTRAINT `item_manifestation_fk`
    FOREIGN KEY (`item_manifestation_id`)
    REFERENCES `manifestations` (`manifestation_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE

);

CREATE INDEX IF NOT EXISTS `idx_items_manifestation_id`
ON `items` (`item_manifestation_id`);
-- BREAK