
-- BREAK


-- ------------------------
-- folders: prevent cycles
-- ------------------------
CREATE TRIGGER IF NOT EXISTS `trg_folders_parent_not_self`
BEFORE UPDATE OF `folder_parent_id` ON `folders`
WHEN NEW.`folder_parent_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.`folder_parent_id` = OLD.`folder_id`
    THEN RAISE(ABORT, 'folders.folder_parent_id cannot reference itself')
  END;
END;

-- BREAK
-- BREAK


CREATE TRIGGER IF NOT EXISTS `trg_folders_parent_no_cycles`
BEFORE UPDATE OF `folder_parent_id` ON `folders`
WHEN NEW.`folder_parent_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN EXISTS (
      WITH RECURSIVE `anc`(`id`) AS (
        SELECT NEW.`folder_parent_id`
        UNION ALL
        SELECT `f`.`folder_parent_id`
        FROM `folders` `f`
        JOIN `anc` ON `f`.`folder_id` = `anc`.`id`
        WHERE `f`.`folder_parent_id` IS NOT NULL
      )
      SELECT 1 FROM `anc` WHERE `id` = OLD.`folder_id` LIMIT 1
    )
    THEN RAISE(ABORT, 'folders hierarchy cycle detected (cannot set parent creating a loop)')
  END;
END;

-- BREAK
-- BREAK


-- =====================================================
-- PATH NORMALIZATION (folders.folder_relpath)
-- =====================================================
-- Rules (when relpath is provided):
--  - must be relative (no scheme, no leading slash/backslash)
--  - must not contain backslashes
--  - must not contain empty segments ("//")
--  - must not contain "." or ".." path traversal segments
--  - must not end with "/"

CREATE TRIGGER IF NOT EXISTS `trg_folders_relpath_normalized`
BEFORE INSERT ON `folders`
WHEN NEW.`folder_relpath` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN LENGTH(TRIM(NEW.`folder_relpath`)) = 0
    THEN RAISE(ABORT, 'folders.folder_relpath cannot be empty when provided')
    WHEN NEW.`folder_relpath` LIKE '%://%'
    THEN RAISE(ABORT, 'folders.folder_relpath must be relative (no URI scheme)')
    WHEN SUBSTR(NEW.`folder_relpath`, 1, 1) = '/'
      OR SUBSTR(NEW.`folder_relpath`, 1, 1) = '\\'
    THEN RAISE(ABORT, 'folders.folder_relpath must be relative (must not start with "/" or "\\")')
    WHEN INSTR(NEW.`folder_relpath`, '\\') > 0
    THEN RAISE(ABORT, 'folders.folder_relpath must use forward slashes (no "\\")')
    WHEN INSTR(NEW.`folder_relpath`, '//') > 0
    THEN RAISE(ABORT, 'folders.folder_relpath must not contain empty segments ("//")')
    WHEN NEW.`folder_relpath` = '.' OR NEW.`folder_relpath` = '..'
      OR NEW.`folder_relpath` LIKE '../%'
      OR NEW.`folder_relpath` LIKE '%/../%'
      OR NEW.`folder_relpath` LIKE '%/..'
      OR NEW.`folder_relpath` LIKE './%'
      OR NEW.`folder_relpath` LIKE '%/./%'
      OR NEW.`folder_relpath` LIKE '%/.'
    THEN RAISE(ABORT, 'folders.folder_relpath must not contain "." or ".." traversal segments')
    WHEN SUBSTR(NEW.`folder_relpath`, -1, 1) = '/'
    THEN RAISE(ABORT, 'folders.folder_relpath must not end with "/"')
  END;
END;

-- BREAK
-- BREAK


CREATE TRIGGER IF NOT EXISTS `trg_folders_relpath_normalized_upd`
BEFORE UPDATE OF `folder_relpath` ON `folders`
WHEN NEW.`folder_relpath` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN LENGTH(TRIM(NEW.`folder_relpath`)) = 0
    THEN RAISE(ABORT, 'folders.folder_relpath cannot be empty when provided')
    WHEN NEW.`folder_relpath` LIKE '%://%'
    THEN RAISE(ABORT, 'folders.folder_relpath must be relative (no URI scheme)')
    WHEN SUBSTR(NEW.`folder_relpath`, 1, 1) = '/'
      OR SUBSTR(NEW.`folder_relpath`, 1, 1) = '\\'
    THEN RAISE(ABORT, 'folders.folder_relpath must be relative (must not start with "/" or "\\")')
    WHEN INSTR(NEW.`folder_relpath`, '\\') > 0
    THEN RAISE(ABORT, 'folders.folder_relpath must use forward slashes (no "\\")')
    WHEN INSTR(NEW.`folder_relpath`, '//') > 0
    THEN RAISE(ABORT, 'folders.folder_relpath must not contain empty segments ("//")')
    WHEN NEW.`folder_relpath` = '.' OR NEW.`folder_relpath` = '..'
      OR NEW.`folder_relpath` LIKE '../%'
      OR NEW.`folder_relpath` LIKE '%/../%'
      OR NEW.`folder_relpath` LIKE '%/..'
      OR NEW.`folder_relpath` LIKE './%'
      OR NEW.`folder_relpath` LIKE '%/./%'
      OR NEW.`folder_relpath` LIKE '%/.'
    THEN RAISE(ABORT, 'folders.folder_relpath must not contain "." or ".." traversal segments')
    WHEN SUBSTR(NEW.`folder_relpath`, -1, 1) = '/'
    THEN RAISE(ABORT, 'folders.folder_relpath must not end with "/"')
  END;
END;
-- BREAK
-- BREAK


-- ----------------------
-- folders: AFTER INSERT
-- ----------------------
CREATE TRIGGER IF NOT EXISTS `trg_folders_parent_not_self_after_ins`
AFTER INSERT ON `folders`
WHEN NEW.`folder_parent_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.`folder_parent_id` = NEW.`folder_id`
    THEN RAISE(ABORT, 'folders.folder_parent_id cannot reference itself')
  END;
END;

-- BREAK
-- BREAK


CREATE TRIGGER IF NOT EXISTS `trg_folders_parent_no_cycles_after_ins`
AFTER INSERT ON `folders`
WHEN NEW.`folder_parent_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN EXISTS (
      WITH RECURSIVE `anc`(`id`) AS (
        SELECT NEW.`folder_parent_id`
        UNION ALL
        SELECT `f`.`folder_parent_id`
        FROM `folders` `f`
        JOIN `anc` ON `f`.`folder_id` = `anc`.`id`
        WHERE `f`.`folder_parent_id` IS NOT NULL
      )
      SELECT 1 FROM `anc` WHERE `id` = NEW.`folder_id` LIMIT 1
    )
    THEN RAISE(ABORT, 'folders hierarchy cycle detected (insert would create/confirm a loop)')
  END;
END;
-- BREAK