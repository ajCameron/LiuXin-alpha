-- BREAK

-- Durable bridge between external Store publication and catalogue commits.
-- The request/result envelopes are versioned by the storage repository; the
-- scalar columns make recovery work inspectable without decoding that JSON.
CREATE TABLE IF NOT EXISTS `storage_ingest_operations` (
  `storage_ingest_operation_id` INTEGER PRIMARY KEY,
  `storage_ingest_operation_uuid` TEXT NOT NULL,
  `storage_ingest_operation_state` TEXT NOT NULL DEFAULT 'started',
  `storage_ingest_operation_store_uuid` TEXT NULL,
  `storage_ingest_operation_storage_key` TEXT NULL,
  `storage_ingest_operation_digital_asset_id` INTEGER NULL,
  `storage_ingest_operation_asset_replica_id` INTEGER NULL,
  `storage_ingest_operation_last_error` TEXT NULL,
  `storage_ingest_operation_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `storage_ingest_operation_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `storage_ingest_operation_scratch` TEXT NOT NULL,

  CONSTRAINT `storage_ingest_operation_state_check`
    CHECK (`storage_ingest_operation_state` IN ('started','publishing','published','committed','failed')),
  CONSTRAINT `storage_ingest_operation_asset_fk`
    FOREIGN KEY (`storage_ingest_operation_digital_asset_id`)
    REFERENCES `digital_assets` (`digital_asset_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,
  CONSTRAINT `storage_ingest_operation_replica_fk`
    FOREIGN KEY (`storage_ingest_operation_asset_replica_id`)
    REFERENCES `asset_replicas` (`asset_replica_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE
);

-- BREAK
-- BREAK

CREATE UNIQUE INDEX IF NOT EXISTS `idx_storage_ingest_operations_uuid`
ON `storage_ingest_operations` (`storage_ingest_operation_uuid`);

-- BREAK
-- BREAK

CREATE INDEX IF NOT EXISTS `idx_storage_ingest_operations_state`
ON `storage_ingest_operations` (`storage_ingest_operation_state`);

-- BREAK
