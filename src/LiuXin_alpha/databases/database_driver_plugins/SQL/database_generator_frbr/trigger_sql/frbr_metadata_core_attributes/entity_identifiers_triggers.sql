

-- =====================================================
-- PRIMARY UNIQUENESS + POLYMORPHIC ENTITY CHECKS (entity_identifiers)
-- =====================================================

CREATE TRIGGER IF NOT EXISTS `trg_entity_identifiers_validate_entity_ref`
BEFORE INSERT ON `entity_identifiers`
BEGIN
  SELECT CASE
    WHEN NEW.`entity_identifier_entity_type` NOT IN ('work','expression','manifestation','item')
    THEN RAISE(ABORT, 'entity_identifiers.entity_identifier_entity_type must be one of: work, expression, manifestation, item')
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
BEGIN
  SELECT CASE
    WHEN NEW.`entity_identifier_entity_type` NOT IN ('work','expression','manifestation','item')
    THEN RAISE(ABORT, 'entity_identifiers.entity_identifier_entity_type must be one of: work, expression, manifestation, item')
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
