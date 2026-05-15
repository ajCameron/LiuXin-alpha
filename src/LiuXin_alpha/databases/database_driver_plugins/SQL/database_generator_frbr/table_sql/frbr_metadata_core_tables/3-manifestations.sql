-- BREAK


-- -----------------------------------------------------
-- Table `manifestations`  (FRBR Manifestation)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `manifestations` (

  `manifestation_id` INTEGER PRIMARY KEY,

  -- Relation to expressions is via `expression_manifestation_links` (many-to-many)

  -- Title add details
  `manifestation_subtitle` TEXT NULL,

  -- Carrier / format
  `manifestation_carrier_type` TEXT NULL,      -- 'print_book', 'ebook', 'audiobook', 'bluray_disc', ...
  `manifestation_format_detail` TEXT NULL,     -- specific format/product label: 'EPUB', 'PDF', 'A-format paperback', '4K UHD BD', ...

  -- Edition / publication info
  `manifestation_edition_statement` TEXT NULL, -- "1st ed.", "Revised ed.", ...
  `manifestation_pub_year` INTEGER NULL,
  `manifestation_pub_date` TEXT NULL,          -- 'YYYY-MM-DD' if known

  -- Flags
  `manifestation_flags` TEXT NULL,

  -- Physical / technical characteristics (stable for the product)
  `manifestation_page_count` INTEGER NULL,
  `manifestation_runtime_minutes` INTEGER NULL,
  `manifestation_region_code` TEXT NULL,       -- 'Region 2', 'Region A', ...

  -- Status / notes
  `manifestation_status` TEXT NULL,            -- 'in_print', 'out_of_print', 'limited', ...
  `manifestation_note` TEXT NULL,

  -- timestamps (epoch_ms)
  `manifestation_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `manifestation_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `manifestation_source_created_datestamp_ep_k` INTEGER NULL,
  `manifestation_source_modified_datestamp_ep_k` INTEGER NULL,

  `manifestation_scratch` TEXT NULL

);

-- BREAK
-- BREAK



-- BREAK
