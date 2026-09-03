-- BREAK

-- Ordered storage-component migrations, independent of the whole-database
-- build/version marker. Existing catalogues acquire this table additively.
CREATE TABLE IF NOT EXISTS `storage_schema_migrations` (
  `storage_schema_migration_id` TEXT PRIMARY KEY,
  `storage_schema_migration_version` INTEGER NOT NULL,
  `storage_schema_migration_applied_timestamp_ep_k` INTEGER NOT NULL,
  `storage_schema_migration_details_json` TEXT NULL
);

-- BREAK
