
-- BREAK

-- -----------------------------------------------------
-- Table `file_derivations`
-- Legacy file-row provenance edges.
-- Retained to read older catalogues and file-oriented integrations. New
-- storage work uses Digital Assets: transformations use
-- `digital_asset_derivations`, while byte-identical SquashFS members are
-- Replicas of the same Asset and therefore require no derivation edge.
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `file_derivations` (
  `file_derivation_id` INTEGER PRIMARY KEY,

  `file_derivation_parent_file_id` INTEGER NULL,
  `file_derivation_child_file_id` INTEGER NULL,

  `file_derivation_kind` TEXT NULL,
  `file_derivation_note` TEXT NULL,

  `file_derivation_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `file_derivation_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `file_derivation_started_timestamp_ep_k` INTEGER NULL,
  `file_derivation_finished_timestamp_ep_k` INTEGER NULL,

  `file_derivation_scratch` TEXT NULL,

  CONSTRAINT `file_derivation_parent_file_fk`
    FOREIGN KEY (`file_derivation_parent_file_id`)
    REFERENCES `files` (`file_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `file_derivation_child_file_fk`
    FOREIGN KEY (`file_derivation_child_file_id`)
    REFERENCES `files` (`file_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `file_derivation_no_self`
    CHECK (`file_derivation_parent_file_id` IS NULL OR `file_derivation_child_file_id` IS NULL OR `file_derivation_parent_file_id` != `file_derivation_child_file_id`)
);

-- BREAK
-- BREAK

CREATE UNIQUE INDEX IF NOT EXISTS `idx_file_derivations_unique_parent_child`
ON `file_derivations` (`file_derivation_parent_file_id`, `file_derivation_child_file_id`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_file_derivations_parent_file`
ON `file_derivations` (`file_derivation_parent_file_id`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_file_derivations_child_file`
ON `file_derivations` (`file_derivation_child_file_id`);

-- BREAK
