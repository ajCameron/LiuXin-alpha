-- BREAK


-- -----------------------------------------------------
-- Table `manifestations`  (FRBR Manifestation)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `manifestations` (
  `manifestation_id` INTEGER PRIMARY KEY,

  -- Relation to expression
  `manifestation_expression_id` INT NOT NULL,

  -- Carrier / format
  `manifestation_carrier_type` TEXT NULL,      -- 'print_book', 'ebook', 'audiobook', 'bluray_disc', ...
  `manifestation_format_detail` TEXT NULL,     -- 'EPUB', 'PDF', 'A-format paperback', '4K UHD BD', ...

  -- Edition / publication info
  `manifestation_edition_statement` TEXT NULL, -- "1st ed.", "Revised ed.", ...
  `manifestation_pub_year` INT NULL,
  `manifestation_pub_date` TEXT NULL,          -- 'YYYY-MM-DD' if known

  -- Physical / technical characteristics (stable for the product)
  `manifestation_page_count` INT NULL,
  `manifestation_runtime_minutes` INT NULL,
  `manifestation_region_code` TEXT NULL,       -- 'Region 2', 'Region A', ...

  -- Status / notes
  `manifestation_status` TEXT NULL,            -- 'in_print', 'out_of_print', 'limited', ...
  `manifestation_note` TEXT NULL,

  -- timestamps (display DATETIME + epoch_ms source)
  manifestation_created_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  manifestation_created_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

    manifestation_modified_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  manifestation_modified_timestamp_ep_k INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

    `manifestation_scratch` TEXT NULL,

  CONSTRAINT `manifestation_expression_fk`
    FOREIGN KEY (`manifestation_expression_id`)
    REFERENCES `expressions` (`expression_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE

);

CREATE INDEX IF NOT EXISTS `idx_manifestations_expression_id`
ON `manifestations` (`manifestation_expression_id`);

-- BREAK
