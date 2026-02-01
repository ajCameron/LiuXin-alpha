
-- BREAK


-- -----------------------------------------------------
-- Table `folders`
-- Hierarchical folders within a store (optional; governed by store capability + triggers)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `folders` (
  `folder_id` INTEGER PRIMARY KEY,

  `folder_store_id` INT NOT NULL,
  `folder_parent_id` INT NULL,

  `folder_name` TEXT NOT NULL,          -- single path segment
  `folder_relpath` TEXT NULL,           -- cached relative path inside store

  `folder_policy_json` TEXT NULL,       -- optional overrides for store policy
  `folder_last_seen_at` DATETIME NULL,  -- telemetry

  `folder_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `folder_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `folder_last_modified` DATETIME DEFAULT CURRENT_TIMESTAMP,

  `folder_scratch` TEXT NULL,

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

CREATE UNIQUE INDEX IF NOT EXISTS `idx_folders_unique_sibling_name`
ON `folders` (`folder_store_id`, `folder_parent_id`, `folder_name`);

CREATE UNIQUE INDEX IF NOT EXISTS `idx_folders_unique_relpath_per_store`
ON `folders` (`folder_store_id`, `folder_relpath`);
-- BREAK