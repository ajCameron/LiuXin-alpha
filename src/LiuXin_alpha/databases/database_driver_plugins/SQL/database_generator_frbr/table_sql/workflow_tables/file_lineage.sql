
-- =====================================================
-- 5) FILE LINEAGE (explicit derivation edges)
-- =====================================================

CREATE TABLE IF NOT EXISTS `digital_asset_derivations` (

  `digital_asset_derivation_id` INTEGER PRIMARY KEY,

  -- NOTE: kept nullable so DriverWrapper.get_blank_row() can insert a placeholder row.
  -- Application logic can enforce presence later.
  `digital_asset_derivation_parent_digital_asset_id` INTEGER NULL,
  `digital_asset_derivation_child_digital_asset_id`  INTEGER NULL,

  `digital_asset_derivation_run_id` INTEGER NULL,     -- optional: link to the transform run that produced it
  `digital_asset_derivation_kind` TEXT NULL,          -- 'converted','ocr_text','thumbnail','repacked','repaired','extracted', ...
  `digital_asset_derivation_note` TEXT NULL,

  -- timestamps (epoch_ms)
  `digital_asset_derivation_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `digital_asset_derivation_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `digital_asset_derivation_source_created_datestamp_ep_k` INTEGER NULL,
  `digital_asset_derivation_source_modified_datestamp_ep_k` INTEGER NULL,
  `digital_asset_derivation_started_timestamp_ep_k` INTEGER NULL,
  `digital_asset_derivation_finished_timestamp_ep_k` INTEGER NULL,

  `digital_asset_derivation_scratch` TEXT NULL,

  CONSTRAINT `file_derviation_parent_file_id_fk`
    FOREIGN KEY (`digital_asset_derivation_parent_digital_asset_id`)
    REFERENCES `digital_assets`(`digital_asset_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `dad_child_fk`
    FOREIGN KEY (`digital_asset_derivation_child_digital_asset_id`)
    REFERENCES `digital_assets`(`digital_asset_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `dad_run_fk`
    FOREIGN KEY (`digital_asset_derivation_run_id`)
    REFERENCES `transform_runs`(`transform_run_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `dad_no_self`
    CHECK (`digital_asset_derivation_parent_digital_asset_id` != `digital_asset_derivation_child_digital_asset_id`)

);

CREATE UNIQUE INDEX IF NOT EXISTS `idx_digital_asset_derivations_unique`
ON `digital_asset_derivations`(`digital_asset_derivation_parent_digital_asset_id`, `digital_asset_derivation_child_digital_asset_id`);

CREATE INDEX IF NOT EXISTS `idx_digital_asset_derivations_parent_asset`
ON `digital_asset_derivations`(`digital_asset_derivation_parent_digital_asset_id`);

CREATE INDEX IF NOT EXISTS `idx_digital_asset_derivations_child_asset`
ON `digital_asset_derivations`(`digital_asset_derivation_child_digital_asset_id`);

