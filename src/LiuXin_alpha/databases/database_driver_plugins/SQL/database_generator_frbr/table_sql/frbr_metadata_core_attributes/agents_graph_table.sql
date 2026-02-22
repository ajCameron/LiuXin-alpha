
-- BREAK

-- -----------------------------------------------------
-- Table `books_plugin_data`
-- Sidecar table to add graph structure to org agents
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS `org_agent_relations` (
  `org_agent_relation_id` INTEGER PRIMARY KEY,

  -- NOTE: kept nullable so DriverWrapper.get_blank_row() can insert a placeholder row.
  -- Application logic can enforce presence later.
  `org_agent_relation_child_agent_id`  INTEGER NULL,
  `org_agent_relation_parent_agent_id` INTEGER NULL,

  `org_agent_relation_type` TEXT NULL,   -- 'imprint_of','subsidiary_of','owned_by','division_of', ...
  `org_agent_relation_start_date` TEXT NULL, -- ISO8601
  `org_agent_relation_end_date`   TEXT NULL, -- ISO8601
  `org_agent_relation_note` TEXT NULL,

  -- timestamps (epoch_ms)
  `org_agent_relation_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `org_agent_relation_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
  `org_agent_relation_source_created_datestamp_ep_k` INTEGER NULL,
  `org_agent_relation_source_modified_datestamp_ep_k` INTEGER NULL,

  `org_agent_relation_scratch` TEXT NULL,

  CONSTRAINT `org_agent_relation_child_fk`
    FOREIGN KEY (`org_agent_relation_child_agent_id`)
    REFERENCES `org_agents`(`org_agent_agent_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `org_agent_relation_parent_fk`
    FOREIGN KEY (`org_agent_relation_parent_agent_id`)
    REFERENCES `org_agents`(`org_agent_agent_id`)
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
  `org_agent_relation_type`


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
-- BREAK


CREATE TRIGGER IF NOT EXISTS `trg_org_agent_relations_no_cycle_insert`
BEFORE INSERT ON `org_agent_relations`
BEGIN
  SELECT RAISE(ABORT, 'org_agent_relations cycle detected')
  WHERE EXISTS (
    WITH RECURSIVE `p`(`id`) AS (
      SELECT NEW.`org_agent_relation_parent_agent_id`
      UNION
      SELECT `r`.`org_agent_relation_parent_agent_id`
      FROM `org_agent_relations` AS `r`
      JOIN `p` ON `r`.`org_agent_relation_child_agent_id` = `p`.`id`
    )
    SELECT 1 FROM `p` WHERE `id` = NEW.`org_agent_relation_child_agent_id`
  );
END;

-- BREAK


CREATE TRIGGER IF NOT EXISTS `trg_org_agent_relations_no_cycle_update`
BEFORE UPDATE OF `org_agent_relation_child_agent_id`, `org_agent_relation_parent_agent_id`
ON `org_agent_relations`
BEGIN
  SELECT RAISE(ABORT, 'org_agent_relations cycle detected')
  WHERE EXISTS (
    WITH RECURSIVE `p`(`id`) AS (
      SELECT NEW.`org_agent_relation_parent_agent_id`
      UNION
      SELECT `r`.`org_agent_relation_parent_agent_id`
      FROM `org_agent_relations` AS `r`
      JOIN `p` ON `r`.`org_agent_relation_child_agent_id` = `p`.`id`
      WHERE `r`.`org_agent_relation_id` != OLD.`org_agent_relation_id`
    )
    SELECT 1 FROM `p` WHERE `id` = NEW.`org_agent_relation_child_agent_id`
  );
END;

-- BREAK
-- BREAK


CREATE TRIGGER IF NOT EXISTS `trg_org_agent_relations_no_cycle_insert`
BEFORE INSERT ON `org_agent_relations`
BEGIN
  SELECT RAISE(ABORT, 'org_agent_relations cycle detected')
  WHERE EXISTS (
    WITH RECURSIVE `p`(`id`) AS (
      SELECT NEW.`org_agent_relation_parent_agent_id`
      UNION
      SELECT `r`.`org_agent_relation_parent_agent_id`
      FROM `org_agent_relations` AS `r`
      JOIN `p` ON `r`.`org_agent_relation_child_agent_id` = `p`.`id`
    )
    SELECT 1 FROM `p` WHERE `id` = NEW.`org_agent_relation_child_agent_id`
  );
END;

-- BREAK
-- BREAK


CREATE TRIGGER IF NOT EXISTS `trg_org_agent_relations_no_cycle_update`
BEFORE UPDATE OF `org_agent_relation_child_agent_id`, `org_agent_relation_parent_agent_id`
ON `org_agent_relations`
BEGIN
  SELECT RAISE(ABORT, 'org_agent_relations cycle detected')
  WHERE EXISTS (
    WITH RECURSIVE `p`(`id`) AS (
      SELECT NEW.`org_agent_relation_parent_agent_id`
      UNION
      SELECT `r`.`org_agent_relation_parent_agent_id`
      FROM `org_agent_relations` AS `r`
      JOIN `p` ON `r`.`org_agent_relation_child_agent_id` = `p`.`id`
      WHERE `r`.`org_agent_relation_id` != OLD.`org_agent_relation_id`
    )
    SELECT 1 FROM `p` WHERE `id` = NEW.`org_agent_relation_child_agent_id`
  );
END;

-- BREAK
-- BREAK
