
-- BREAK

-- =====================================================
-- STORAGE (Stores / Folders / Files)
-- =====================================================

-- -----------------------------------------------------
-- Table `stores`
-- Logical storage backends (filesystem, NAS, tape, rclone, http_ro, etc.)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `stores` (
  `store_id` INTEGER PRIMARY KEY,

  -- Identity / addressing
  `store_name` TEXT NOT NULL,
  `store_kind` TEXT NOT NULL,           -- 'filesystem', 'nas', 'tape', 'rclone', 'http_ro', ...
  `store_access_protocol` TEXT NULL,    -- 'file', 'nfs', 'smb', 'tape', 'rclone', 'http', 'https', ...
  `store_root_uri` TEXT NULL,           -- base locator (may change); file keys are relative to this

  -- Access / auth
  `store_auth_method` TEXT NULL,        -- 'none', 'env', 'keyring', 'config_file', 'embedded', ...
  `store_credentials` TEXT NULL,        -- encrypted blob or reference (ENV:/KEYRING:/PATH:...), not plaintext

  -- Storage policy
  `store_storage_mask` INTEGER NULL,    -- broad classification bitmask
  `store_policy_json` TEXT NULL,        -- fine-grained 'block'/'prefer' rules

  -- State / notes
  `store_online_status` TEXT NULL,      -- 'online', 'offline', 'retired'
  `store_location_note` TEXT NULL,

  -- Telemetry
  `store_last_seen_online` DATETIME NULL,
  `store_last_healthcheck_ok` DATETIME NULL,

  -- Capabilities
  `store_supports_folders` INT NOT NULL DEFAULT 1,
  `store_supports_hierarchical_list` INT NOT NULL DEFAULT 1,

  `store_supports_random_read` INT NOT NULL DEFAULT 1,
  `store_supports_random_write` INT NOT NULL DEFAULT 1,
  `store_supports_append` INT NOT NULL DEFAULT 1,

  `store_supports_atomic_rename` INT NOT NULL DEFAULT 1,
  `store_supports_atomic_overwrite` INT NOT NULL DEFAULT 1,

  `store_supports_delete` INT NOT NULL DEFAULT 1,
  `store_is_read_only` INT NOT NULL DEFAULT 0,

  `store_is_eventually_consistent` INT NOT NULL DEFAULT 0,

  `store_supports_checksums` INT NOT NULL DEFAULT 0,
  `store_supports_immutable_objects` INT NOT NULL DEFAULT 0,
  `store_supports_snapshots` INT NOT NULL DEFAULT 0,
  `store_supports_server_side_encryption` INT NOT NULL DEFAULT 0,

  `store_supports_parallel_read` INT NOT NULL DEFAULT 1,
  `store_supports_parallel_write` INT NOT NULL DEFAULT 1,

  `store_requires_mount` INT NOT NULL DEFAULT 0,
  `store_latency_class` TEXT NULL,      -- 'hot','warm','cold','glacial'

  -- Timestamps
  `store_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `store_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `store_last_modified` DATETIME DEFAULT CURRENT_TIMESTAMP,

  `store_scratch` TEXT NULL
);
-- BREAK