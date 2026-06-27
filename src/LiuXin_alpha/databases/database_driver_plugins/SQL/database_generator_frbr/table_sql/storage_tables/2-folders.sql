
-- BREAK


-- -----------------------------------------------------
-- Table `folders`
-- Hierarchical folders within a store (optional; governed by store capability + triggers)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `folders` (
  `folder_id` INTEGER PRIMARY KEY,

  -- NOTE: kept nullable so DriverWrapper.get_blank_row() can insert a placeholder row.
  -- Application logic can enforce presence later.
  `folder_store_id` INTEGER NULL,
  `folder_parent_id` INTEGER NULL,

  -- NOTE: kept nullable so DriverWrapper.get_blank_row() can insert a placeholder row.
  -- Application logic can enforce presence later.
  `folder_name` TEXT NULL,          -- single path segment
  `folder_relpath` TEXT NULL,           -- cached relative path inside store

  `folder_policy_json` TEXT NULL,       -- optional overrides for store policy
  `folder_default_replication_policy_id` INTEGER NULL,
  `folder_default_backup_policy_id` INTEGER NULL,
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
    ON UPDATE CASCADE,

  CONSTRAINT `folder_default_replication_policy_fk`
    FOREIGN KEY (`folder_default_replication_policy_id`)
    REFERENCES `replication_policies` (`replication_policy_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `folder_default_backup_policy_fk`
    FOREIGN KEY (`folder_default_backup_policy_id`)
    REFERENCES `backup_policies` (`backup_policy_id`)
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
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_folders_default_replication_policy_id`
ON `folders` (`folder_default_replication_policy_id`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_folders_default_backup_policy_id`
ON `folders` (`folder_default_backup_policy_id`);
-- BREAK