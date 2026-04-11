
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

  `store_scratch` TEXT NULL


);
-- BREAK