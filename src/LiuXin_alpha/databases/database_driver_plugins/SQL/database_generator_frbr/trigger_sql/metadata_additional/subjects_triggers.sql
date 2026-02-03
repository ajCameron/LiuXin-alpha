
-- BREAK

-- ------------------------
-- subjects: prevent cycles
-- ------------------------
CREATE TRIGGER IF NOT EXISTS trg_subjects_parent_not_self
BEFORE UPDATE OF subject_parent_id ON subjects
WHEN NEW.subject_parent_id IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.subject_parent_id = OLD.subject_id
    THEN RAISE(ABORT, 'subjects.subject_parent_id cannot reference itself')
  END;
END;

-- BREAK
-- BREAK


CREATE TRIGGER IF NOT EXISTS trg_subjects_parent_no_cycles
BEFORE UPDATE OF subject_parent_id ON subjects
WHEN NEW.subject_parent_id IS NOT NULL
BEGIN
  SELECT CASE
    WHEN EXISTS (
      WITH RECURSIVE anc(id) AS (
        SELECT NEW.subject_parent_id
        UNION ALL
        SELECT s.subject_parent_id
        FROM subjects s
        JOIN anc ON s.subject_id = anc.id
        WHERE s.subject_parent_id IS NOT NULL
      )
      SELECT 1 FROM anc WHERE id = OLD.subject_id LIMIT 1
    )
    THEN RAISE(ABORT, 'subjects hierarchy cycle detected (cannot set parent creating a loop)')
  END;
END;

-- BREAK
-- BREAK

CREATE TRIGGER IF NOT EXISTS trg_subjects_parent_not_self_ins
BEFORE INSERT ON subjects
WHEN NEW.subject_parent_id IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.subject_parent_id = NEW.subject_id
    THEN RAISE(ABORT, 'subjects.subject_parent_id cannot reference itself')
  END;
END;

-- BREAK
-- BREAK


CREATE TRIGGER IF NOT EXISTS trg_subjects_parent_no_cycles_ins
BEFORE INSERT ON subjects
WHEN NEW.subject_parent_id IS NOT NULL
BEGIN
  SELECT CASE
    WHEN EXISTS (
      WITH RECURSIVE anc(id) AS (
        SELECT NEW.subject_parent_id
        UNION ALL
        SELECT s.subject_parent_id
        FROM subjects s
        JOIN anc ON s.subject_id = anc.id
        WHERE s.subject_parent_id IS NOT NULL
      )
      SELECT 1 FROM anc WHERE id = NEW.subject_id LIMIT 1
    )
    THEN RAISE(ABORT, 'subjects hierarchy cycle detected (cannot insert row creating a loop)')
  END;
END;

-- BREAK
-- BREAK


-- ----------------------
-- subjects: AFTER INSERT
-- ----------------------
CREATE TRIGGER IF NOT EXISTS trg_subjects_parent_not_self_after_ins
AFTER INSERT ON subjects
WHEN NEW.subject_parent IS NOT NULL
BEGIN
  SELECT CASE
    WHEN NEW.subject_parent = NEW.subject_id
    THEN RAISE(ABORT, 'subjects.subject_parent cannot reference itself')
  END;
END;

-- BREAK
-- BREAK


CREATE TRIGGER IF NOT EXISTS trg_subjects_parent_no_cycles_after_ins
AFTER INSERT ON subjects
WHEN NEW.subject_parent IS NOT NULL
BEGIN
  SELECT CASE
    WHEN EXISTS (
      WITH RECURSIVE anc(id) AS (
        SELECT NEW.subject_parent
        UNION ALL
        SELECT s.subject_parent
        FROM subjects s
        JOIN anc ON s.subject_id = anc.id
        WHERE s.subject_parent IS NOT NULL
      )
      SELECT 1 FROM anc WHERE id = NEW.subject_id LIMIT 1
    )
    THEN RAISE(ABORT, 'subjects hierarchy cycle detected (insert would create/confirm a loop)')
  END;
END;
-- BREAK