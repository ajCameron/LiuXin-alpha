
-- BREAK


-- -----------------------------------------------------
-- Table `folders`
-- Hierarchical folders within a store (optional; governed by store capability + triggers)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `folders` (
  `folder_id` INTEGER PRIMARY KEY,

  `folder_store_id` INTEGER NOT NULL,
  `folder_parent_id` INTEGER NULL,

  `folder_name` TEXT NOT NULL,          -- single path segment
  `folder_relpath` TEXT NULL,           -- cached relative path inside store

  `folder_policy_json` TEXT NULL,       -- optional overrides for store policy
  `folder_last_seen_timestamp_ep_k` INTEGER NULL, -- telemetry


  `folder_scratch` TEXT NULL,

  -- timestamps (epoch_ms)
  `folder_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `folder_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `folder_source_created_datestamp_ep_k` INTEGER NULL,
  `folder_source_modified_datestamp_ep_k` INTEGER NULL,

  CONSTRAINT `folder_store_fk`
    FOREIGN KEY (`folder_store_id`)
    REFERENCES `stores` (`store_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `folder_parent_fk`
    FOREIGN KEY (`folder_parent_id`)
    REFERENCES `folders` (`folder_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE

);

-- BREAK
-- BREAK


CREATE UNIQUE INDEX IF NOT EXISTS `idx_folders_unique_sibling_name`
ON `folders` (`folder_store_id`, `folder_parent_id`, `folder_name`);

-- BREAK
-- BREAK


CREATE UNIQUE INDEX IF NOT EXISTS `idx_folders_unique_relpath_per_store`
ON `folders` (`folder_store_id`, `folder_relpath`);
-- BREAK