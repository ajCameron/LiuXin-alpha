
-- BREAK

-- -----------------------------------------------------
-- Table `books_plugin_data`
-- Sidecar table to add graph structure to org agents
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS `org_agent_relations` (
  `org_agent_relation_id` INTEGER PRIMARY KEY,

  `org_agent_relation_child_agent_id`  INTEGER NOT NULL,
  `org_agent_relation_parent_agent_id` INTEGER NOT NULL,

  `org_agent_relation_type` TEXT NOT NULL,   -- 'imprint_of','subsidiary_of','owned_by','division_of', ...
  `org_agent_relation_start_date` TEXT NULL, -- ISO8601
  `org_agent_relation_end_date`   TEXT NULL, -- ISO8601
  `org_agent_relation_note` TEXT NULL,

  -- timestamps (display DATETIME + epoch_ms source)
  `org_agent_relation_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `org_agent_relation_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

  `org_agent_relation_scratch` TEXT NULL,

  CONSTRAINT `org_agent_relation_child_fk`
    FOREIGN KEY (`org_agent_relation_child_agent_id`)
    REFERENCES `agents`(`agent_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `org_agent_relation_parent_fk`
    FOREIGN KEY (`org_agent_relation_parent_agent_id`)
    REFERENCES `agents`(`agent_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `org_agent_relation_no_self`
    CHECK (`org_agent_relation_child_agent_id` != `org_agent_relation_parent_agent_id`)
);

-- BREAK
-- BREAK


CREATE UNIQUE INDEX IF NOT EXISTS `idx_org_agent_relations_unique`
ON `org_agent_relations`(
  `org_agent_relation_child_agent_id`,
  `org_agent_relation_parent_agent_id`,
  `org_agent_relation_type`,

  -- timestamps
  `org_agent_relation_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `org_agent_relation_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))

);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_org_agent_relations_parent`
ON `org_agent_relations`(`org_agent_relation_parent_agent_id`);

-- BREAK
-- BREAK


CREATE INDEX IF NOT EXISTS `idx_org_agent_relations_child`
ON `org_agent_relations`(`org_agent_relation_child_agent_id`);

-- BREAK
