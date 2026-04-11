
-- BREAK

-- -----------------------------------------------------
-- Table `replication_policies`
-- Declarative desired-state policies for live replicas.
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `replication_policies` (
  `replication_policy_id` INTEGER PRIMARY KEY,
  `replication_policy_name` TEXT NOT NULL,
  `replication_policy_min_copies` INTEGER NOT NULL DEFAULT 1,
  `replication_policy_target_copies` INTEGER NULL,
  `replication_policy_distinct_by_json` TEXT NULL,
  `replication_policy_max_copies_per_bucket` INTEGER NOT NULL DEFAULT 1,
  `replication_policy_required_store_tags_json` TEXT NULL,
  `replication_policy_preferred_store_tags_json` TEXT NULL,
  `replication_policy_forbidden_store_tags_json` TEXT NULL,
  `replication_policy_required_capabilities_json` TEXT NULL,
  `replication_policy_forbidden_capabilities_json` TEXT NULL,
  `replication_policy_synchronous_write_copies` INTEGER NOT NULL DEFAULT 1,
  `replication_policy_auto_heal` INTEGER NOT NULL DEFAULT 1,
  `replication_policy_mode` TEXT NOT NULL DEFAULT 'active',
  `replication_policy_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `replication_policy_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `replication_policy_scratch` TEXT NULL,
  CONSTRAINT `replication_policy_name_unique` UNIQUE (`replication_policy_name`),
  CONSTRAINT `replication_policy_min_copies_check` CHECK (`replication_policy_min_copies` >= 1),
  CONSTRAINT `replication_policy_target_copies_check` CHECK (`replication_policy_target_copies` IS NULL OR `replication_policy_target_copies` >= `replication_policy_min_copies`),
  CONSTRAINT `replication_policy_max_bucket_check` CHECK (`replication_policy_max_copies_per_bucket` >= 1),
  CONSTRAINT `replication_policy_sync_write_check` CHECK (`replication_policy_synchronous_write_copies` >= 1),
  CONSTRAINT `replication_policy_auto_heal_bool` CHECK (`replication_policy_auto_heal` IN (0,1)),
  CONSTRAINT `replication_policy_mode_check` CHECK (`replication_policy_mode` IN ('active','backup','archive'))
);

CREATE INDEX IF NOT EXISTS `idx_replication_policies_name`
ON `replication_policies` (`replication_policy_name`);

-- BREAK
-- BREAK

-- -----------------------------------------------------
-- Table `backup_policies`
-- Declarative desired-state policies for backup copies.
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `backup_policies` (
  `backup_policy_id` INTEGER PRIMARY KEY,

  `backup_policy_name` TEXT NOT NULL,

  `backup_policy_min_backup_copies` INTEGER NOT NULL DEFAULT 1,
  `backup_policy_target_backup_copies` INTEGER NULL,

  `backup_policy_distinct_by_json` TEXT NULL,

  `backup_policy_max_copies_per_bucket` INTEGER NOT NULL DEFAULT 1,

  `backup_policy_required_store_tags_json` TEXT NULL,
  `backup_policy_preferred_store_tags_json` TEXT NULL,
  `backup_policy_forbidden_store_tags_json` TEXT NULL,

  `backup_policy_periodic_verification` INTEGER NOT NULL DEFAULT 1,

  `backup_policy_retention_locked` INTEGER NOT NULL DEFAULT 0,

  `backup_policy_mode` TEXT NOT NULL DEFAULT 'backup',

  `backup_policy_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `backup_policy_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

    `backup_policy_scratch` TEXT NULL,

    CONSTRAINT `backup_policy_name_unique` UNIQUE (`backup_policy_name`),
  CONSTRAINT `backup_policy_min_copies_check` CHECK (`backup_policy_min_backup_copies` >= 1),
  CONSTRAINT `backup_policy_target_copies_check` CHECK (`backup_policy_target_backup_copies` IS NULL OR `backup_policy_target_backup_copies` >= `backup_policy_min_backup_copies`),
  CONSTRAINT `backup_policy_max_bucket_check` CHECK (`backup_policy_max_copies_per_bucket` >= 1),
  CONSTRAINT `backup_policy_periodic_verification_bool` CHECK (`backup_policy_periodic_verification` IN (0,1)),
  CONSTRAINT `backup_policy_retention_locked_bool` CHECK (`backup_policy_retention_locked` IN (0,1)),
  CONSTRAINT `backup_policy_mode_check` CHECK (`backup_policy_mode` IN ('backup','archive'))
);

CREATE INDEX IF NOT EXISTS `idx_backup_policies_name`
ON `backup_policies` (`backup_policy_name`);
