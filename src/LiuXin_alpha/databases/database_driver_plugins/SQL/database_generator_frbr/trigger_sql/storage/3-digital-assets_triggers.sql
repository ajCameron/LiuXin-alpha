-- BREAK

-- =====================================================
-- asset_replicas: path / store / asset-kind invariants
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

-- BREAK
-- BREAK

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

-- BREAK
-- BREAK

CREATE TRIGGER IF NOT EXISTS `trg_asset_replicas_atomic_assets_only`
BEFORE INSERT ON `asset_replicas`
WHEN NEW.`asset_replica_digital_asset_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN (SELECT `digital_asset_kind` FROM `digital_assets` WHERE `digital_asset_id` = NEW.`asset_replica_digital_asset_id`) = 'composite'
    THEN RAISE(ABORT, 'asset_replicas may only reference atomic digital_assets')
  END;
END;

CREATE TRIGGER IF NOT EXISTS `trg_asset_replicas_atomic_assets_only_upd`
BEFORE UPDATE OF `asset_replica_digital_asset_id` ON `asset_replicas`
WHEN NEW.`asset_replica_digital_asset_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN (SELECT `digital_asset_kind` FROM `digital_assets` WHERE `digital_asset_id` = NEW.`asset_replica_digital_asset_id`) = 'composite'
    THEN RAISE(ABORT, 'asset_replicas may only reference atomic digital_assets')
  END;
END;

-- BREAK
-- BREAK

CREATE TRIGGER IF NOT EXISTS `trg_digital_asset_compositions_parent_must_be_composite`
BEFORE INSERT ON `digital_asset_compositions`
WHEN NEW.`digital_asset_composition_parent_asset_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN (SELECT `digital_asset_kind` FROM `digital_assets` WHERE `digital_asset_id` = NEW.`digital_asset_composition_parent_asset_id`) != 'composite'
    THEN RAISE(ABORT, 'digital_asset_compositions parent asset must be kind=composite')
  END;
END;

CREATE TRIGGER IF NOT EXISTS `trg_digital_asset_compositions_parent_must_be_composite_upd`
BEFORE UPDATE OF `digital_asset_composition_parent_asset_id` ON `digital_asset_compositions`
WHEN NEW.`digital_asset_composition_parent_asset_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN (SELECT `digital_asset_kind` FROM `digital_assets` WHERE `digital_asset_id` = NEW.`digital_asset_composition_parent_asset_id`) != 'composite'
    THEN RAISE(ABORT, 'digital_asset_compositions parent asset must be kind=composite')
  END;
END;

-- BREAK
-- BREAK

CREATE TRIGGER IF NOT EXISTS `trg_digital_asset_compositions_no_cycles`
BEFORE INSERT ON `digital_asset_compositions`
WHEN NEW.`digital_asset_composition_parent_asset_id` IS NOT NULL
 AND NEW.`digital_asset_composition_member_asset_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN EXISTS (
      WITH RECURSIVE `anc`(`id`) AS (
        SELECT NEW.`digital_asset_composition_member_asset_id`
        UNION ALL
        SELECT `dac`.`digital_asset_composition_member_asset_id`
        FROM `digital_asset_compositions` `dac`
        JOIN `anc` ON `dac`.`digital_asset_composition_parent_asset_id` = `anc`.`id`
      )
      SELECT 1 FROM `anc` WHERE `id` = NEW.`digital_asset_composition_parent_asset_id` LIMIT 1
    )
    THEN RAISE(ABORT, 'digital_asset_compositions cycle detected')
  END;
END;

CREATE TRIGGER IF NOT EXISTS `trg_digital_asset_compositions_no_cycles_upd`
BEFORE UPDATE OF `digital_asset_composition_parent_asset_id`, `digital_asset_composition_member_asset_id` ON `digital_asset_compositions`
WHEN NEW.`digital_asset_composition_parent_asset_id` IS NOT NULL
 AND NEW.`digital_asset_composition_member_asset_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN EXISTS (
      WITH RECURSIVE `anc`(`id`) AS (
        SELECT NEW.`digital_asset_composition_member_asset_id`
        UNION ALL
        SELECT `dac`.`digital_asset_composition_member_asset_id`
        FROM `digital_asset_compositions` `dac`
        JOIN `anc` ON `dac`.`digital_asset_composition_parent_asset_id` = `anc`.`id`
        WHERE `dac`.`digital_asset_composition_id` != OLD.`digital_asset_composition_id`
      )
      SELECT 1 FROM `anc` WHERE `id` = NEW.`digital_asset_composition_parent_asset_id` LIMIT 1
    )
    THEN RAISE(ABORT, 'digital_asset_compositions cycle detected')
  END;
END;
