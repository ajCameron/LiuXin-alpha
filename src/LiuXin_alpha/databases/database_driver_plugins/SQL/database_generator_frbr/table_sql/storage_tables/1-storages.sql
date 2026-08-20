
-- BREAK

-- =====================================================
-- STORAGE (Stores / Folders / Digital assets)
-- =====================================================

-- -----------------------------------------------------
-- Table `stores`
-- Logical storage backends (filesystem, NAS, tape, rclone, http_ro, etc.)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `stores` (
  `store_id` INTEGER PRIMARY KEY,

  -- Identity / addressing
  -- Stable public identity used by opaque storage Locations. Unlike store_id,
  -- this survives export/import and must never be repurposed for another Store.
  `store_uuid` TEXT NULL,
  `store_host_uuid` TEXT NULL,
  `store_device_uuid` TEXT NULL,
  -- NOTE: kept nullable so DriverWrapper.get_blank_row() can insert a placeholder row.
  -- Application logic can enforce presence later.
  `store_name` TEXT NULL,
  -- NOTE: kept nullable so DriverWrapper.get_blank_row() can insert a placeholder row.
  -- Application logic can enforce presence later.
  `store_kind` TEXT NULL,           -- 'filesystem', 'nas', 'tape', 'rclone', 'http_ro', ...
  `store_access_protocol` TEXT NULL,    -- 'file', 'nfs', 'smb', 'tape', 'rclone', 'http', 'https', ...
  `store_root_uri` TEXT NULL,           -- base locator (may change); file keys are relative to this

  -- Access / auth
  `store_auth_method` TEXT NULL,        -- 'none', 'env', 'keyring', 'config_file', 'embedded', ...
  `store_credentials` TEXT NULL,        -- encrypted blob or reference (ENV:/KEYRING:/PATH:...), not plaintext

  -- Storage policy
  `store_storage_mask` INTEGER NULL,    -- broad classification bitmask
  `store_policy_json` TEXT NULL,        -- fine-grained 'block'/'prefer' rules
  `store_failure_domain` TEXT NULL,     -- fault-isolation bucket for replica spread
  `store_region` TEXT NULL,             -- geographic/administrative placement bucket
  `store_tags_json` TEXT NULL,          -- lightweight label set used by policy resolution
  `store_default_replication_policy_id` INTEGER NULL,
  `store_default_backup_policy_id` INTEGER NULL,

  -- Which replica modes may legitimately live on this store.
  `store_supports_active_replica_mode` INTEGER NOT NULL DEFAULT 1,
  `store_supports_backup_replica_mode` INTEGER NOT NULL DEFAULT 1,
  `store_supports_archive_replica_mode` INTEGER NOT NULL DEFAULT 1,

  -- Broad operator-intent role for this store.
  -- This is deliberately softer than the replica-mode support flags: a store can be
  -- operationally 'backup' while still technically able to hold active files, or
  -- 'mixed' when it is used for more than one role.
  `store_operational_role` TEXT NULL,     -- 'live', 'mixed', 'backup', 'archive', 'cache'

  -- State / notes
  `store_online_status` TEXT NULL,      -- 'online', 'offline', 'retired'
  `store_location_note` TEXT NULL,

  -- Telemetry
  `store_last_seen_online_timestamp_ep_k` INTEGER NULL,
  `store_last_healthcheck_ok_timestamp_ep_k` INTEGER NULL,

  -- Capabilities
  `store_supports_folders` INTEGER NOT NULL DEFAULT 1,
  `store_supports_hierarchical_list` INTEGER NOT NULL DEFAULT 1,

  `store_supports_random_read` INTEGER NOT NULL DEFAULT 1,
  `store_supports_random_write` INTEGER NOT NULL DEFAULT 1,
  `store_supports_append` INTEGER NOT NULL DEFAULT 1,

  `store_supports_atomic_rename` INTEGER NOT NULL DEFAULT 1,
  `store_supports_atomic_overwrite` INTEGER NOT NULL DEFAULT 1,

  `store_supports_delete` INTEGER NOT NULL DEFAULT 1,
  `store_is_read_only` INTEGER NOT NULL DEFAULT 0,

  `store_is_eventually_consistent` INTEGER NOT NULL DEFAULT 0,

  `store_supports_checksums` INTEGER NOT NULL DEFAULT 0,
  `store_supports_immutable_objects` INTEGER NOT NULL DEFAULT 0,
  `store_supports_snapshots` INTEGER NOT NULL DEFAULT 0,
  `store_supports_server_side_encryption` INTEGER NOT NULL DEFAULT 0,

  `store_supports_parallel_read` INTEGER NOT NULL DEFAULT 1,
  `store_supports_parallel_write` INTEGER NOT NULL DEFAULT 1,

  `store_requires_mount` INTEGER NOT NULL DEFAULT 0,
  `store_latency_class` TEXT NULL,      -- 'hot','warm','cold','glacial'

  -- timestamps (epoch_ms)
  `store_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `store_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `store_source_created_datestamp_ep_k` INTEGER NULL,
  `store_source_modified_datestamp_ep_k` INTEGER NULL,

  `store_scratch` TEXT NULL,

  CONSTRAINT `store_default_replication_policy_fk`
    FOREIGN KEY (`store_default_replication_policy_id`)
    REFERENCES `replication_policies` (`replication_policy_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `store_default_backup_policy_fk`
    FOREIGN KEY (`store_default_backup_policy_id`)
    REFERENCES `backup_policies` (`backup_policy_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,

  CONSTRAINT `store_supports_active_replica_mode_bool`
    CHECK (`store_supports_active_replica_mode` IN (0,1)),

  CONSTRAINT `store_supports_backup_replica_mode_bool`
    CHECK (`store_supports_backup_replica_mode` IN (0,1)),

  CONSTRAINT `store_supports_archive_replica_mode_bool`
    CHECK (`store_supports_archive_replica_mode` IN (0,1)),

  CONSTRAINT `store_operational_role_check`
    CHECK (`store_operational_role` IS NULL OR `store_operational_role` IN ('live','mixed','backup','archive','cache'))

);
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_stores_default_replication_policy_id`
ON `stores` (`store_default_replication_policy_id`);

-- BREAK
-- BREAK

CREATE UNIQUE INDEX IF NOT EXISTS `idx_stores_uuid_unique`
ON `stores` (`store_uuid`)
WHERE `store_uuid` IS NOT NULL;

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_stores_default_backup_policy_id`
ON `stores` (`store_default_backup_policy_id`);

-- BREAK
-- BREAK
