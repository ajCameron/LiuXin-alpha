

-- ----------------------
-- genres: prevent cycles
-- ----------------------
CREATE TRIGGER IF NOT EXISTS `trg_genres_parent_not_self`
BEFORE UPDATE OF `genre_parent_id` ON `genres`
WHEN NEW.`genre_parent_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.`genre_parent_id` = OLD.`genre_id`
    THEN RAISE(ABORT, 'genres.genre_parent_id cannot reference itself')
  END;
END;

-- BREAK
-- BREAK

CREATE TRIGGER IF NOT EXISTS `trg_genres_parent_no_cycles`
BEFORE UPDATE OF `genre_parent_id` ON `genres`
WHEN NEW.`genre_parent_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN EXISTS (
      WITH RECURSIVE `anc`(`id`) AS (
        SELECT NEW.`genre_parent_id`
        UNION ALL
        SELECT `g`.`genre_parent_id`
        FROM `genres` `g`
        JOIN `anc` ON `g`.`genre_id` = `anc`.`id`
        WHERE `g`.`genre_parent_id` IS NOT NULL
      )
      SELECT 1 FROM `anc` WHERE `id` = OLD.`genre_id` LIMIT 1
    )
    THEN RAISE(ABORT, 'genres hierarchy cycle detected (cannot set parent creating a loop)')
  END;
END;
-- BREAK
-- BREAK

CREATE TRIGGER IF NOT EXISTS `trg_genres_parent_not_self_ins`
BEFORE INSERT ON `genres`
WHEN NEW.`genre_parent_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.`genre_parent_id` = NEW.`genre_id`
    THEN RAISE(ABORT, 'genres.genre_parent_id cannot reference itself')
  END;
END;

-- BREAK
-- BREAK

CREATE TRIGGER IF NOT EXISTS `trg_genres_parent_no_cycles_ins`
BEFORE INSERT ON `genres`
WHEN NEW.`genre_parent_id` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN EXISTS (
      WITH RECURSIVE `anc`(`id`) AS (
        SELECT NEW.`genre_parent_id`
        UNION ALL
        SELECT `g`.`genre_parent_id`
        FROM `genres` `g`
        JOIN `anc` ON `g`.`genre_id` = `anc`.`id`
        WHERE `g`.`genre_parent_id` IS NOT NULL
      )
      SELECT 1 FROM `anc` WHERE `id` = NEW.`genre_id` LIMIT 1
    )
    THEN RAISE(ABORT, 'genres hierarchy cycle detected (cannot insert row creating a loop)')
  END;
END;

-- BREAK
-- BREAK
-- ----------------------
-- genres: AFTER INSERT
-- ----------------------
CREATE TRIGGER IF NOT EXISTS `trg_genres_parent_not_self_after_ins`
AFTER INSERT ON `genres`
WHEN NEW.`genre_parent` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.`genre_parent` = NEW.`genre_id`
    THEN RAISE(ABORT, 'genres.genre_parent cannot reference itself')
  END;
END;

-- BREAK
-- BREAK

CREATE TRIGGER IF NOT EXISTS `trg_genres_parent_no_cycles_after_ins`
AFTER INSERT ON `genres`
WHEN NEW.`genre_parent` IS NOT NULL
BEGIN
  SELECT CASE
    WHEN EXISTS (
      WITH RECURSIVE `anc`(`id`) AS (
        SELECT NEW.`genre_parent_is`
        UNION ALL
        SELECT `g`.`genre_parent_id`
        FROM `genres` `g`
        JOIN `anc` ON `g`.`genre_id` = `anc`.`id`
        WHERE `g`.`genre_parent_id` IS NOT NULL
      )
      SELECT 1 FROM `anc` WHERE `id` = NEW.`genre_id` LIMIT 1
    )
    THEN RAISE(ABORT, 'genres hierarchy cycle detected (insert would create/confirm a loop)')
  END;
END;
-- BREAK
