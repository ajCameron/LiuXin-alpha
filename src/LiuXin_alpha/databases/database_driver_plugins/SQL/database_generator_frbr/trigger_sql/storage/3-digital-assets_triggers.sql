
-- =====================================================
-- asset_replicas: path / store invariants
-- =====================================================

CREATE TRIGGER IF NOT EXISTS `trg_asset_replicas_storage_key_must_be_relative`
BEFORE INSERT ON `asset_replicas`
BEGIN
  SELECT CASE
    WHEN NEW.`asset_replica_storage_key` IS NULL OR LENGTH(TRIM(NEW.`asset_replica_storage_key`)) = 0
    THEN RAISE(ABORT, 'asset_replicas.asset_replica_storage_key must be a non-empty relative key')
    WHEN NEW.`asset_replica_storage_key` LIKE '%://%'
    THEN RAISE(ABORT, 'asset_replicas.asset_replica_storage_key must be relative (no URI scheme)')
    WHEN SUBSTR(NEW.`asset_replica_storage_key`, 1, 1) = '/'
      OR SUBSTR(NEW.`asset_replica_storage_key`, 1, 1) = '\'
    THEN RAISE(ABORT, 'asset_replicas.asset_replica_storage_key must be relative (must not start with "/" or "\")')
  END;
END;

CREATE TRIGGER IF NOT EXISTS `trg_asset_replicas_storage_key_must_be_relative_upd`
BEFORE UPDATE OF `asset_replica_storage_key` ON `asset_replicas`
BEGIN
  SELECT CASE
    WHEN NEW.`asset_replica_storage_key` IS NULL OR LENGTH(TRIM(NEW.`asset_replica_storage_key`)) = 0
    THEN RAISE(ABORT, 'asset_replicas.asset_replica_storage_key must be a non-empty relative key')
    WHEN NEW.`asset_replica_storage_key` LIKE '%://%'
    THEN RAISE(ABORT, 'asset_replicas.asset_replica_storage_key must be relative (no URI scheme)')
    WHEN SUBSTR(NEW.`asset_replica_storage_key`, 1, 1) = '/'
      OR SUBSTR(NEW.`asset_replica_storage_key`, 1, 1) = '\'
    THEN RAISE(ABORT, 'asset_replicas.asset_replica_storage_key must be relative (must not start with "/" or "\")')
  END;
END;


CREATE TRIGGER IF NOT EXISTS `trg_asset_replicas_folder_must_match_store`
BEFORE INSERT ON `asset_replicas`
WHEN NEW.`asset_replica_folder_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN (SELECT `folder_store_id` FROM `folders` WHERE `folder_id` = NEW.`asset_replica_folder_id`) != NEW.`asset_replica_store_id`
    THEN RAISE(ABORT, 'asset_replicas.asset_replica_folder_id refers to a folder in a different store than asset_replicas.asset_replica_store_id')
  END;
END;

CREATE TRIGGER IF NOT EXISTS `trg_asset_replicas_folder_must_match_store_upd`
BEFORE UPDATE OF `asset_replica_folder_id`, `asset_replica_store_id` ON `asset_replicas`
WHEN NEW.`asset_replica_folder_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN (SELECT `folder_store_id` FROM `folders` WHERE `folder_id` = NEW.`asset_replica_folder_id`) != NEW.`asset_replica_store_id`
    THEN RAISE(ABORT, 'asset_replicas.asset_replica_folder_id refers to a folder in a different store than asset_replicas.asset_replica_store_id')
  END;
END;


CREATE TRIGGER IF NOT EXISTS `trg_asset_replicas_mode_supported_by_store`
BEFORE INSERT ON `asset_replicas`
WHEN NEW.`asset_replica_store_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.`asset_replica_mode` = 'active'
      AND COALESCE((SELECT `store_supports_active_replica_mode` FROM `stores` WHERE `store_id` = NEW.`asset_replica_store_id`), 0) != 1
    THEN RAISE(ABORT, 'asset_replicas.asset_replica_mode=active is not supported by the target store')
    WHEN NEW.`asset_replica_mode` = 'backup'
      AND COALESCE((SELECT `store_supports_backup_replica_mode` FROM `stores` WHERE `store_id` = NEW.`asset_replica_store_id`), 0) != 1
    THEN RAISE(ABORT, 'asset_replicas.asset_replica_mode=backup is not supported by the target store')
    WHEN NEW.`asset_replica_mode` = 'archive'
      AND COALESCE((SELECT `store_supports_archive_replica_mode` FROM `stores` WHERE `store_id` = NEW.`asset_replica_store_id`), 0) != 1
    THEN RAISE(ABORT, 'asset_replicas.asset_replica_mode=archive is not supported by the target store')
  END;
END;

CREATE TRIGGER IF NOT EXISTS `trg_asset_replicas_mode_supported_by_store_upd`
BEFORE UPDATE OF `asset_replica_mode`, `asset_replica_store_id` ON `asset_replicas`
WHEN NEW.`asset_replica_store_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.`asset_replica_mode` = 'active'
      AND COALESCE((SELECT `store_supports_active_replica_mode` FROM `stores` WHERE `store_id` = NEW.`asset_replica_store_id`), 0) != 1
    THEN RAISE(ABORT, 'asset_replicas.asset_replica_mode=active is not supported by the target store')
    WHEN NEW.`asset_replica_mode` = 'backup'
      AND COALESCE((SELECT `store_supports_backup_replica_mode` FROM `stores` WHERE `store_id` = NEW.`asset_replica_store_id`), 0) != 1
    THEN RAISE(ABORT, 'asset_replicas.asset_replica_mode=backup is not supported by the target store')
    WHEN NEW.`asset_replica_mode` = 'archive'
      AND COALESCE((SELECT `store_supports_archive_replica_mode` FROM `stores` WHERE `store_id` = NEW.`asset_replica_store_id`), 0) != 1
    THEN RAISE(ABORT, 'asset_replicas.asset_replica_mode=archive is not supported by the target store')
  END;
END;
