

-- =====================================================
-- PRIMARY UNIQUENESS + POLYMORPHIC ENTITY CHECKS (entity_identifiers)
-- =====================================================

CREATE TRIGGER IF NOT EXISTS `trg_entity_identifiers_validate_entity_ref`
BEFORE INSERT ON `entity_identifiers`
WHEN NEW.`entity_identifier_entity_type` IS NOT NULL
  OR NEW.`entity_identifier_entity_id` IS NOT NULL
  OR NEW.`entity_identifier_scheme` IS NOT NULL
  OR NEW.`entity_identifier_value` IS NOT NULL
  OR NEW.`entity_identifier_is_primary` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.`entity_identifier_entity_type` NOT IN ('work','expression','manifestation','item','agent')
    THEN RAISE(ABORT, 'entity_identifiers.entity_identifier_entity_type must be one of: work, expression, manifestation, item, agent')
  END;

  -- Catalog may store a candidate before it has an owner. Both owner fields
  -- must be NULL together; a populated reference always names an existing row.
  SELECT CASE
    WHEN (NEW.`entity_identifier_entity_type` IS NULL) != (NEW.`entity_identifier_entity_id` IS NULL)
    THEN RAISE(ABORT, 'entity_identifiers owner type and id must both be present or both absent')
  END;

  SELECT CASE
    WHEN NEW.`entity_identifier_entity_type` = 'work'
     AND NOT EXISTS (SELECT 1 FROM `works` WHERE `work_id` = NEW.`entity_identifier_entity_id`)
    THEN RAISE(ABORT, 'entity_identifiers references missing work_id')
    WHEN NEW.`entity_identifier_entity_type` = 'expression'
     AND NOT EXISTS (SELECT 1 FROM `expressions` WHERE `expression_id` = NEW.`entity_identifier_entity_id`)
    THEN RAISE(ABORT, 'entity_identifiers references missing expression_id')
    WHEN NEW.`entity_identifier_entity_type` = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM `manifestations` WHERE `manifestation_id` = NEW.`entity_identifier_entity_id`)
    THEN RAISE(ABORT, 'entity_identifiers references missing manifestation_id')
    WHEN NEW.`entity_identifier_entity_type` = 'item'
     AND NOT EXISTS (SELECT 1 FROM `items` WHERE `item_id` = NEW.`entity_identifier_entity_id`)
    THEN RAISE(ABORT, 'entity_identifiers references missing item_id')
    WHEN NEW.`entity_identifier_entity_type` = 'agent'
     AND NOT EXISTS (SELECT 1 FROM `agents` WHERE `agent_id` = NEW.`entity_identifier_entity_id`)
    THEN RAISE(ABORT, 'entity_identifiers references missing agent_id')
  END;

  SELECT CASE
    WHEN NEW.`entity_identifier_scheme` IS NULL OR LENGTH(TRIM(NEW.`entity_identifier_scheme`)) = 0
    THEN RAISE(ABORT, 'entity_identifiers.entity_identifier_scheme must be non-empty')
    WHEN NEW.`entity_identifier_value` IS NULL OR LENGTH(TRIM(NEW.`entity_identifier_value`)) = 0
    THEN RAISE(ABORT, 'entity_identifiers.entity_identifier_value must be non-empty')
  END;

  SELECT CASE
    WHEN NEW.`entity_identifier_is_primary` = 1
     AND EXISTS (
       SELECT 1 FROM `entity_identifiers`
       WHERE `entity_identifier_entity_type` = NEW.`entity_identifier_entity_type`
         AND `entity_identifier_entity_id`   = NEW.`entity_identifier_entity_id`
         AND `entity_identifier_scheme`      = NEW.`entity_identifier_scheme`
         AND `entity_identifier_is_primary`  = 1
       LIMIT 1
     )
    THEN RAISE(ABORT, 'entity_identifiers: primary already exists for this entity + scheme')
  END;
END;

CREATE TRIGGER IF NOT EXISTS `trg_entity_identifiers_validate_entity_ref_upd`
BEFORE UPDATE OF `entity_identifier_entity_type`, `entity_identifier_entity_id`, `entity_identifier_scheme`, `entity_identifier_value`, `entity_identifier_is_primary` ON `entity_identifiers`
WHEN NEW.`entity_identifier_entity_type` IS NOT NULL
  OR NEW.`entity_identifier_entity_id` IS NOT NULL
  OR NEW.`entity_identifier_scheme` IS NOT NULL
  OR NEW.`entity_identifier_value` IS NOT NULL
  OR NEW.`entity_identifier_is_primary` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.`entity_identifier_entity_type` NOT IN ('work','expression','manifestation','item','agent')
    THEN RAISE(ABORT, 'entity_identifiers.entity_identifier_entity_type must be one of: work, expression, manifestation, item, agent')
  END;

  -- Catalog may store a candidate before it has an owner. Both owner fields
  -- must be NULL together; a populated reference always names an existing row.
  SELECT CASE
    WHEN (NEW.`entity_identifier_entity_type` IS NULL) != (NEW.`entity_identifier_entity_id` IS NULL)
    THEN RAISE(ABORT, 'entity_identifiers owner type and id must both be present or both absent')
  END;

  SELECT CASE
    WHEN NEW.`entity_identifier_entity_type` = 'work'
     AND NOT EXISTS (SELECT 1 FROM `works` WHERE `work_id` = NEW.`entity_identifier_entity_id`)
    THEN RAISE(ABORT, 'entity_identifiers references missing work_id')
    WHEN NEW.`entity_identifier_entity_type` = 'expression'
     AND NOT EXISTS (SELECT 1 FROM `expressions` WHERE `expression_id` = NEW.`entity_identifier_entity_id`)
    THEN RAISE(ABORT, 'entity_identifiers references missing expression_id')
    WHEN NEW.`entity_identifier_entity_type` = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM `manifestations` WHERE `manifestation_id` = NEW.`entity_identifier_entity_id`)
    THEN RAISE(ABORT, 'entity_identifiers references missing manifestation_id')
    WHEN NEW.`entity_identifier_entity_type` = 'item'
     AND NOT EXISTS (SELECT 1 FROM `items` WHERE `item_id` = NEW.`entity_identifier_entity_id`)
    THEN RAISE(ABORT, 'entity_identifiers references missing item_id')
    WHEN NEW.`entity_identifier_entity_type` = 'agent'
     AND NOT EXISTS (SELECT 1 FROM `agents` WHERE `agent_id` = NEW.`entity_identifier_entity_id`)
    THEN RAISE(ABORT, 'entity_identifiers references missing agent_id')
  END;

  SELECT CASE
    WHEN NEW.`entity_identifier_scheme` IS NULL OR LENGTH(TRIM(NEW.`entity_identifier_scheme`)) = 0
    THEN RAISE(ABORT, 'entity_identifiers.entity_identifier_scheme must be non-empty')
    WHEN NEW.`entity_identifier_value` IS NULL OR LENGTH(TRIM(NEW.`entity_identifier_value`)) = 0
    THEN RAISE(ABORT, 'entity_identifiers.entity_identifier_value must be non-empty')
  END;

  SELECT CASE
    WHEN NEW.`entity_identifier_is_primary` = 1
     AND EXISTS (
       SELECT 1 FROM `entity_identifiers`
       WHERE `entity_identifier_entity_type` = NEW.`entity_identifier_entity_type`
         AND `entity_identifier_entity_id`   = NEW.`entity_identifier_entity_id`
         AND `entity_identifier_scheme`      = NEW.`entity_identifier_scheme`
         AND `entity_identifier_is_primary`  = 1
         AND `entity_identifier_id`         != OLD.`entity_identifier_id`
       LIMIT 1
     )
    THEN RAISE(ABORT, 'entity_identifiers: primary already exists for this entity + scheme')
  END;
END;
-- BREAK

-- Polymorphic references follow owner deletion. Referenced owner keys are stable.

CREATE TRIGGER IF NOT EXISTS `trg_work_identifiers_delete`
AFTER DELETE ON `works`
BEGIN
  DELETE FROM `entity_identifiers`
  WHERE `entity_identifier_entity_type` = 'work'
    AND `entity_identifier_entity_id` = OLD.`work_id`;
END;

CREATE TRIGGER IF NOT EXISTS `trg_work_identifiers_key_update`
BEFORE UPDATE ON `works`
WHEN NEW.`work_id` IS NOT OLD.`work_id`
 AND EXISTS (SELECT 1 FROM `entity_identifiers`
             WHERE `entity_identifier_entity_type` = 'work'
               AND `entity_identifier_entity_id` IN (OLD.`work_id`, NEW.`work_id`))
BEGIN
  SELECT RAISE(ABORT, 'Cannot change work_id while identifiers reference it');
END;

CREATE TRIGGER IF NOT EXISTS `trg_expression_identifiers_delete`
AFTER DELETE ON `expressions`
BEGIN
  DELETE FROM `entity_identifiers`
  WHERE `entity_identifier_entity_type` = 'expression'
    AND `entity_identifier_entity_id` = OLD.`expression_id`;
END;

CREATE TRIGGER IF NOT EXISTS `trg_expression_identifiers_key_update`
BEFORE UPDATE ON `expressions`
WHEN NEW.`expression_id` IS NOT OLD.`expression_id`
 AND EXISTS (SELECT 1 FROM `entity_identifiers`
             WHERE `entity_identifier_entity_type` = 'expression'
               AND `entity_identifier_entity_id` IN (OLD.`expression_id`, NEW.`expression_id`))
BEGIN
  SELECT RAISE(ABORT, 'Cannot change expression_id while identifiers reference it');
END;

CREATE TRIGGER IF NOT EXISTS `trg_manifestation_identifiers_delete`
AFTER DELETE ON `manifestations`
BEGIN
  DELETE FROM `entity_identifiers`
  WHERE `entity_identifier_entity_type` = 'manifestation'
    AND `entity_identifier_entity_id` = OLD.`manifestation_id`;
END;

CREATE TRIGGER IF NOT EXISTS `trg_manifestation_identifiers_key_update`
BEFORE UPDATE ON `manifestations`
WHEN NEW.`manifestation_id` IS NOT OLD.`manifestation_id`
 AND EXISTS (SELECT 1 FROM `entity_identifiers`
             WHERE `entity_identifier_entity_type` = 'manifestation'
               AND `entity_identifier_entity_id` IN (OLD.`manifestation_id`, NEW.`manifestation_id`))
BEGIN
  SELECT RAISE(ABORT, 'Cannot change manifestation_id while identifiers reference it');
END;

CREATE TRIGGER IF NOT EXISTS `trg_item_identifiers_delete`
AFTER DELETE ON `items`
BEGIN
  DELETE FROM `entity_identifiers`
  WHERE `entity_identifier_entity_type` = 'item'
    AND `entity_identifier_entity_id` = OLD.`item_id`;
END;

CREATE TRIGGER IF NOT EXISTS `trg_item_identifiers_key_update`
BEFORE UPDATE ON `items`
WHEN NEW.`item_id` IS NOT OLD.`item_id`
 AND EXISTS (SELECT 1 FROM `entity_identifiers`
             WHERE `entity_identifier_entity_type` = 'item'
               AND `entity_identifier_entity_id` IN (OLD.`item_id`, NEW.`item_id`))
BEGIN
  SELECT RAISE(ABORT, 'Cannot change item_id while identifiers reference it');
END;

CREATE TRIGGER IF NOT EXISTS `trg_agent_identifiers_delete`
AFTER DELETE ON `agents`
BEGIN
  DELETE FROM `entity_identifiers`
  WHERE `entity_identifier_entity_type` = 'agent'
    AND `entity_identifier_entity_id` = OLD.`agent_id`;
END;

CREATE TRIGGER IF NOT EXISTS `trg_agent_identifiers_key_update`
BEFORE UPDATE ON `agents`
WHEN NEW.`agent_id` IS NOT OLD.`agent_id`
 AND EXISTS (SELECT 1 FROM `entity_identifiers`
             WHERE `entity_identifier_entity_type` = 'agent'
               AND `entity_identifier_entity_id` IN (OLD.`agent_id`, NEW.`agent_id`))
BEGIN
  SELECT RAISE(ABORT, 'Cannot change agent_id while identifiers reference it');
END;

-- REPLACE can suppress DELETE triggers when recursive_triggers is disabled.
-- Reject replacement of an identified owner before SQLite reaches that path.
CREATE TRIGGER IF NOT EXISTS `trg_work_identifiers_replace`
BEFORE INSERT ON `works`
WHEN EXISTS (SELECT 1 FROM `works` WHERE `work_id` = NEW.`work_id`)
 AND EXISTS (SELECT 1 FROM `entity_identifiers`
             WHERE `entity_identifier_entity_type` = 'work'
               AND `entity_identifier_entity_id` = NEW.`work_id`)
BEGIN
  SELECT RAISE(ABORT, 'Cannot replace work while identifiers reference it');
END;

-- REPLACE can suppress DELETE triggers when recursive_triggers is disabled.
-- Reject replacement of an identified owner before SQLite reaches that path.
CREATE TRIGGER IF NOT EXISTS `trg_expression_identifiers_replace`
BEFORE INSERT ON `expressions`
WHEN EXISTS (SELECT 1 FROM `expressions` WHERE `expression_id` = NEW.`expression_id`)
 AND EXISTS (SELECT 1 FROM `entity_identifiers`
             WHERE `entity_identifier_entity_type` = 'expression'
               AND `entity_identifier_entity_id` = NEW.`expression_id`)
BEGIN
  SELECT RAISE(ABORT, 'Cannot replace expression while identifiers reference it');
END;

-- REPLACE can suppress DELETE triggers when recursive_triggers is disabled.
-- Reject replacement of an identified owner before SQLite reaches that path.
CREATE TRIGGER IF NOT EXISTS `trg_manifestation_identifiers_replace`
BEFORE INSERT ON `manifestations`
WHEN EXISTS (SELECT 1 FROM `manifestations` WHERE `manifestation_id` = NEW.`manifestation_id`)
 AND EXISTS (SELECT 1 FROM `entity_identifiers`
             WHERE `entity_identifier_entity_type` = 'manifestation'
               AND `entity_identifier_entity_id` = NEW.`manifestation_id`)
BEGIN
  SELECT RAISE(ABORT, 'Cannot replace manifestation while identifiers reference it');
END;

-- REPLACE can suppress DELETE triggers when recursive_triggers is disabled.
-- Reject replacement of an identified owner before SQLite reaches that path.
CREATE TRIGGER IF NOT EXISTS `trg_item_identifiers_replace`
BEFORE INSERT ON `items`
WHEN EXISTS (SELECT 1 FROM `items` WHERE `item_id` = NEW.`item_id`)
 AND EXISTS (SELECT 1 FROM `entity_identifiers`
             WHERE `entity_identifier_entity_type` = 'item'
               AND `entity_identifier_entity_id` = NEW.`item_id`)
BEGIN
  SELECT RAISE(ABORT, 'Cannot replace item while identifiers reference it');
END;

-- REPLACE can suppress DELETE triggers when recursive_triggers is disabled.
-- Reject replacement of an identified owner before SQLite reaches that path.
CREATE TRIGGER IF NOT EXISTS `trg_agent_identifiers_replace`
BEFORE INSERT ON `agents`
WHEN EXISTS (SELECT 1 FROM `agents` WHERE `agent_id` = NEW.`agent_id`)
 AND EXISTS (SELECT 1 FROM `entity_identifiers`
             WHERE `entity_identifier_entity_type` = 'agent'
               AND `entity_identifier_entity_id` = NEW.`agent_id`)
BEGIN
  SELECT RAISE(ABORT, 'Cannot replace agent while identifiers reference it');
END;
