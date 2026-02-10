-- LiuXin-alpha : Tightening triggers (tier-2 invariants)
-- Notes:
--  - Adds stricter guards for hierarchy cycles, path normalization, and "primary" uniqueness
--  - Intended to be applied AFTER liuXin_triggers.sql
--  - Uses RAISE(ABORT, ...) for explicit error messages

-- BREAK


PRAGMA `foreign_keys` = ON;

-- BREAK
-- BREAK


-- =====================================================
-- HIERARCHY CYCLE PREVENTION (series / genres / subjects / folders)
-- =====================================================

-- Helper pattern:
-- Prevent parent_id being self, and prevent cycles by walking ancestry from NEW.parent_id
-- and ensuring we do not reach the current row id.

-- ----------------------
-- series: prevent cycles
-- ----------------------
CREATE TRIGGER IF NOT EXISTS `trg_series_parent_not_self`
BEFORE UPDATE OF `series_parent_id` ON `series`
WHEN NEW.`series_parent_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.`series_parent_id` = OLD.`series_id`
    THEN RAISE(ABORT, 'series.series_parent_id cannot reference itself')
  END;
END;

-- BREAK
-- BREAK


CREATE TRIGGER IF NOT EXISTS `trg_series_parent_no_cycles`
BEFORE UPDATE OF `series_parent_id` ON `series`
WHEN NEW.`series_parent_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN EXISTS (
      WITH RECURSIVE `anc`(`id`) AS (
        SELECT NEW.`series_parent_id`
        UNION ALL
        SELECT `s`.`series_parent_id`
        FROM `series` `s`
        JOIN `anc` ON `s`.`series_id` = `anc`.`id`
        WHERE `s`.`series_parent_id` IS NOT NULL
      )
      SELECT 1 FROM `anc` WHERE `id` = OLD.`series_id` LIMIT 1
    )
    THEN RAISE(ABORT, 'series hierarchy cycle detected (cannot set parent creating a loop)')
  END;
END;
-- BREAK
-- BREAK

-- Prevent self-parent (mostly moot on INSERT, but keeps symmetry)
CREATE TRIGGER IF NOT EXISTS `trg_series_parent_not_self_ins`
BEFORE INSERT ON `series`
WHEN NEW.`series_parent_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.`series_parent_id` = NEW.`series_id`
    THEN RAISE(ABORT, 'series.series_parent_id cannot reference itself')
  END;
END;

-- BREAK
-- BREAK


-- Prevent cycles on INSERT (parent chain cannot contain the new row id)
CREATE TRIGGER IF NOT EXISTS `trg_series_parent_no_cycles_ins`
BEFORE INSERT ON `series`
WHEN NEW.`series_parent_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN EXISTS (
      WITH RECURSIVE `anc`(`id`) AS (
        SELECT NEW.`series_parent_id`
        UNION ALL
        SELECT `s`.`series_parent_id`
        FROM `series` `s`
        JOIN `anc` ON `s`.`series_id` = `anc`.`id`
        WHERE `s`.`series_parent_id` IS NOT NULL
      )
      SELECT 1
      FROM `anc`
      WHERE `id` = NEW.`series_id`
      LIMIT 1
    )
    THEN RAISE(ABORT, 'series hierarchy cycle detected (cannot insert row creating a loop)')
  END;
END;

-- BREAK
-- BREAK
-- =====================================================
-- AFTER INSERT cycle + self-parent guards
-- (Correct even when ids are auto-assigned)
-- =====================================================

-- ----------------------
-- series: AFTER INSERT
-- ----------------------
CREATE TRIGGER IF NOT EXISTS `trg_series_parent_not_self_after_ins`
AFTER INSERT ON `series`
WHEN NEW.`series_parent` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.`series_parent` = NEW.`series_id`
    THEN RAISE(ABORT, 'series.series_parent cannot reference itself')
  END;
END;

-- BREAK
-- BREAK

CREATE TRIGGER IF NOT EXISTS `trg_series_parent_no_cycles_after_ins`
AFTER INSERT ON `series`
WHEN NEW.`series_parent` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN EXISTS (
      WITH RECURSIVE `anc`(`id`) AS (
        SELECT NEW.`series_parent`
        UNION ALL
        SELECT `s`.`series_parent`
        FROM `series` `s`
        JOIN `anc` ON `s`.`series_id` = `anc`.`id`
        WHERE `s`.`series_parent` IS NOT NULL
      )
      SELECT 1 FROM `anc` WHERE `id` = NEW.`series_id` LIMIT 1
    )
    THEN RAISE(ABORT, 'series hierarchy cycle detected (insert would create/confirm a loop)')
  END;
END;
-- BREAK