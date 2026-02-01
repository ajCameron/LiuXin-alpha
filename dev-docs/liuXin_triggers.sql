-- LiuXin-alpha : Triggers (main + metadata satellites)
-- Notes:
--  - SQLite triggers using RAISE(ABORT, ...) for helpful error messages
--  - Assumes main tables + metadata tables already exist
--  - Use PRAGMA foreign_keys=ON per connection
--  - Use the -- BREAK markers as section boundaries

PRAGMA foreign_keys = ON;

-- =====================================================
-- MAIN STORAGE INVARIANTS (stores / folders / files)
-- =====================================================

-- 1) If the store doesn't support folders, file_folder_id must be NULL
CREATE TRIGGER IF NOT EXISTS trg_files_no_folder_when_store_disallows
BEFORE INSERT ON files
WHEN NEW.file_folder_id IS NOT NULL
BEGIN
  SELECT CASE
    WHEN (SELECT store_supports_folders FROM stores WHERE store_id = NEW.file_store_id) = 0
    THEN RAISE(ABORT, 'files.file_folder_id set, but the target store does not support folders')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_files_no_folder_when_store_disallows_upd
BEFORE UPDATE OF file_folder_id, file_store_id ON files
WHEN NEW.file_folder_id IS NOT NULL
BEGIN
  SELECT CASE
    WHEN (SELECT store_supports_folders FROM stores WHERE store_id = NEW.file_store_id) = 0
    THEN RAISE(ABORT, 'files.file_folder_id set, but the target store does not support folders')
  END;
END;
-- BREAK

-- 2) If file_folder_id is set, it must belong to the same store as file_store_id
CREATE TRIGGER IF NOT EXISTS trg_files_folder_must_match_store
BEFORE INSERT ON files
WHEN NEW.file_folder_id IS NOT NULL
BEGIN
  SELECT CASE
    WHEN (SELECT folder_store_id FROM folders WHERE folder_id = NEW.file_folder_id) != NEW.file_store_id
    THEN RAISE(ABORT, 'files.file_folder_id refers to a folder in a different store than files.file_store_id')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_files_folder_must_match_store_upd
BEFORE UPDATE OF file_folder_id, file_store_id ON files
WHEN NEW.file_folder_id IS NOT NULL
BEGIN
  SELECT CASE
    WHEN (SELECT folder_store_id FROM folders WHERE folder_id = NEW.file_folder_id) != NEW.file_store_id
    THEN RAISE(ABORT, 'files.file_folder_id refers to a folder in a different store than files.file_store_id')
  END;
END;
-- BREAK

-- 3) file_storage_key must be RELATIVE (no scheme, no leading slash/backslash; non-empty)
CREATE TRIGGER IF NOT EXISTS trg_files_storage_key_must_be_relative
BEFORE INSERT ON files
BEGIN
  SELECT CASE
    WHEN NEW.file_storage_key IS NULL OR LENGTH(TRIM(NEW.file_storage_key)) = 0
    THEN RAISE(ABORT, 'files.file_storage_key must be a non-empty relative key')
    WHEN NEW.file_storage_key LIKE '%://%'
    THEN RAISE(ABORT, 'files.file_storage_key must be relative (no URI scheme like "http://", "file://", etc.)')
    WHEN SUBSTR(NEW.file_storage_key, 1, 1) = '/'
      OR SUBSTR(NEW.file_storage_key, 1, 1) = '\'
    THEN RAISE(ABORT, 'files.file_storage_key must be relative (must not start with "/" or "\")')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_files_storage_key_must_be_relative_upd
BEFORE UPDATE OF file_storage_key ON files
BEGIN
  SELECT CASE
    WHEN NEW.file_storage_key IS NULL OR LENGTH(TRIM(NEW.file_storage_key)) = 0
    THEN RAISE(ABORT, 'files.file_storage_key must be a non-empty relative key')
    WHEN NEW.file_storage_key LIKE '%://%'
    THEN RAISE(ABORT, 'files.file_storage_key must be relative (no URI scheme like "http://", "file://", etc.)')
    WHEN SUBSTR(NEW.file_storage_key, 1, 1) = '/'
      OR SUBSTR(NEW.file_storage_key, 1, 1) = '\'
    THEN RAISE(ABORT, 'files.file_storage_key must be relative (must not start with "/" or "\")')
  END;
END;
-- BREAK

-- 4) Strict DB policy: disallow inserting/moving files into read-only stores
CREATE TRIGGER IF NOT EXISTS trg_files_no_insert_into_readonly_store
BEFORE INSERT ON files
BEGIN
  SELECT CASE
    WHEN (SELECT store_is_read_only FROM stores WHERE store_id = NEW.file_store_id) = 1
    THEN RAISE(ABORT, 'cannot insert files into a read-only store (stores.store_is_read_only=1)')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_files_no_update_store_into_readonly
BEFORE UPDATE OF file_store_id ON files
BEGIN
  SELECT CASE
    WHEN (SELECT store_is_read_only FROM stores WHERE store_id = NEW.file_store_id) = 1
    THEN RAISE(ABORT, 'cannot move files into a read-only store (stores.store_is_read_only=1)')
  END;
END;
-- BREAK

-- 5) Can't create a folder in a store that doesn't support folders
CREATE TRIGGER IF NOT EXISTS trg_folders_store_must_support_folders
BEFORE INSERT ON folders
BEGIN
  SELECT CASE
    WHEN (SELECT store_supports_folders FROM stores WHERE store_id = NEW.folder_store_id) = 0
    THEN RAISE(ABORT, 'cannot create folders in a store that does not support folders')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_folders_store_must_support_folders_upd
BEFORE UPDATE OF folder_store_id ON folders
BEGIN
  SELECT CASE
    WHEN (SELECT store_supports_folders FROM stores WHERE store_id = NEW.folder_store_id) = 0
    THEN RAISE(ABORT, 'cannot move a folder into a store that does not support folders')
  END;
END;
-- BREAK

-- 6) If folder_parent_id is set, parent must be in the same store
CREATE TRIGGER IF NOT EXISTS trg_folders_parent_must_match_store
BEFORE INSERT ON folders
WHEN NEW.folder_parent_id IS NOT NULL
BEGIN
  SELECT CASE
    WHEN (SELECT folder_store_id FROM folders WHERE folder_id = NEW.folder_parent_id) != NEW.folder_store_id
    THEN RAISE(ABORT, 'folders.folder_parent_id refers to a folder in a different store')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_folders_parent_must_match_store_upd
BEFORE UPDATE OF folder_parent_id, folder_store_id ON folders
WHEN NEW.folder_parent_id IS NOT NULL
BEGIN
  SELECT CASE
    WHEN (SELECT folder_store_id FROM folders WHERE folder_id = NEW.folder_parent_id) != NEW.folder_store_id
    THEN RAISE(ABORT, 'folders.folder_parent_id refers to a folder in a different store')
  END;
END;
-- BREAK

-- 7) Can't disable folders on a store that already has folders
CREATE TRIGGER IF NOT EXISTS trg_stores_cannot_disable_folders_when_folders_exist
BEFORE UPDATE OF store_supports_folders ON stores
WHEN NEW.store_supports_folders = 0 AND OLD.store_supports_folders = 1
BEGIN
  SELECT CASE
    WHEN EXISTS (SELECT 1 FROM folders WHERE folder_store_id = OLD.store_id LIMIT 1)
    THEN RAISE(ABORT, 'cannot set store_supports_folders=0 while folders exist for this store')
  END;
END;
-- BREAK

-- 8) Optional: can't disable delete while files exist (treat as WORM-ish policy)
CREATE TRIGGER IF NOT EXISTS trg_stores_cannot_disable_delete_when_files_exist
BEFORE UPDATE OF store_supports_delete ON stores
WHEN NEW.store_supports_delete = 0 AND OLD.store_supports_delete = 1
BEGIN
  SELECT CASE
    WHEN EXISTS (SELECT 1 FROM files WHERE file_store_id = OLD.store_id LIMIT 1)
    THEN RAISE(ABORT, 'cannot set store_supports_delete=0 while files exist for this store (decide policy first)')
  END;
END;
-- BREAK


-- =====================================================
-- POLYMORPHIC ENTITY POINTER GUARDS (shared patterns)
-- =====================================================
-- These triggers validate:
--   (a) entity_type is one of the allowed values
--   (b) the referenced entity_id exists in the corresponding table
-- SQLite doesn't support true polymorphic FKs, so we use EXISTS checks.
-- =====================================================

-- Allowed entity types:
--   work, expression, manifestation, item, file


-- =====================================================
-- entity_agents
-- =====================================================

CREATE TRIGGER IF NOT EXISTS trg_entity_agents_validate_entity_ref
BEFORE INSERT ON entity_agents
BEGIN
  SELECT CASE
    WHEN NEW.entity_agent_entity_type NOT IN ('work','expression','manifestation','item')
    THEN RAISE(ABORT, 'entity_agents.entity_agent_entity_type must be one of: work, expression, manifestation, item')
  END;

  SELECT CASE
    WHEN NEW.entity_agent_entity_type = 'work'
     AND NOT EXISTS (SELECT 1 FROM works WHERE work_id = NEW.entity_agent_entity_id)
    THEN RAISE(ABORT, 'entity_agents references missing work_id')
    WHEN NEW.entity_agent_entity_type = 'expression'
     AND NOT EXISTS (SELECT 1 FROM expressions WHERE expression_id = NEW.entity_agent_entity_id)
    THEN RAISE(ABORT, 'entity_agents references missing expression_id')
    WHEN NEW.entity_agent_entity_type = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM manifestations WHERE manifestation_id = NEW.entity_agent_entity_id)
    THEN RAISE(ABORT, 'entity_agents references missing manifestation_id')
    WHEN NEW.entity_agent_entity_type = 'item'
     AND NOT EXISTS (SELECT 1 FROM items WHERE item_id = NEW.entity_agent_entity_id)
    THEN RAISE(ABORT, 'entity_agents references missing item_id')
  END;

  SELECT CASE
    WHEN NEW.entity_agent_ord IS NOT NULL AND NEW.entity_agent_ord < 0
    THEN RAISE(ABORT, 'entity_agents.entity_agent_ord must be >= 0 when provided')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_entity_agents_validate_entity_ref_upd
BEFORE UPDATE OF entity_agent_entity_type, entity_agent_entity_id, entity_agent_ord ON entity_agents
BEGIN
  SELECT CASE
    WHEN NEW.entity_agent_entity_type NOT IN ('work','expression','manifestation','item')
    THEN RAISE(ABORT, 'entity_agents.entity_agent_entity_type must be one of: work, expression, manifestation, item')
  END;

  SELECT CASE
    WHEN NEW.entity_agent_entity_type = 'work'
     AND NOT EXISTS (SELECT 1 FROM works WHERE work_id = NEW.entity_agent_entity_id)
    THEN RAISE(ABORT, 'entity_agents references missing work_id')
    WHEN NEW.entity_agent_entity_type = 'expression'
     AND NOT EXISTS (SELECT 1 FROM expressions WHERE expression_id = NEW.entity_agent_entity_id)
    THEN RAISE(ABORT, 'entity_agents references missing expression_id')
    WHEN NEW.entity_agent_entity_type = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM manifestations WHERE manifestation_id = NEW.entity_agent_entity_id)
    THEN RAISE(ABORT, 'entity_agents references missing manifestation_id')
    WHEN NEW.entity_agent_entity_type = 'item'
     AND NOT EXISTS (SELECT 1 FROM items WHERE item_id = NEW.entity_agent_entity_id)
    THEN RAISE(ABORT, 'entity_agents references missing item_id')
  END;

  SELECT CASE
    WHEN NEW.entity_agent_ord IS NOT NULL AND NEW.entity_agent_ord < 0
    THEN RAISE(ABORT, 'entity_agents.entity_agent_ord must be >= 0 when provided')
  END;
END;
-- BREAK


-- =====================================================
-- entity_tags
-- =====================================================

CREATE TRIGGER IF NOT EXISTS trg_entity_tags_validate_entity_ref
BEFORE INSERT ON entity_tags
BEGIN
  SELECT CASE
    WHEN NEW.entity_tag_entity_type NOT IN ('work','expression','manifestation','item','file')
    THEN RAISE(ABORT, 'entity_tags.entity_tag_entity_type must be one of: work, expression, manifestation, item, file')
  END;

  SELECT CASE
    WHEN NEW.entity_tag_entity_type = 'work'
     AND NOT EXISTS (SELECT 1 FROM works WHERE work_id = NEW.entity_tag_entity_id)
    THEN RAISE(ABORT, 'entity_tags references missing work_id')
    WHEN NEW.entity_tag_entity_type = 'expression'
     AND NOT EXISTS (SELECT 1 FROM expressions WHERE expression_id = NEW.entity_tag_entity_id)
    THEN RAISE(ABORT, 'entity_tags references missing expression_id')
    WHEN NEW.entity_tag_entity_type = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM manifestations WHERE manifestation_id = NEW.entity_tag_entity_id)
    THEN RAISE(ABORT, 'entity_tags references missing manifestation_id')
    WHEN NEW.entity_tag_entity_type = 'item'
     AND NOT EXISTS (SELECT 1 FROM items WHERE item_id = NEW.entity_tag_entity_id)
    THEN RAISE(ABORT, 'entity_tags references missing item_id')
    WHEN NEW.entity_tag_entity_type = 'file'
     AND NOT EXISTS (SELECT 1 FROM files WHERE file_id = NEW.entity_tag_entity_id)
    THEN RAISE(ABORT, 'entity_tags references missing file_id')
  END;

  SELECT CASE
    WHEN NEW.entity_tag_ord IS NOT NULL AND NEW.entity_tag_ord < 0
    THEN RAISE(ABORT, 'entity_tags.entity_tag_ord must be >= 0 when provided')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_entity_tags_validate_entity_ref_upd
BEFORE UPDATE OF entity_tag_entity_type, entity_tag_entity_id, entity_tag_ord ON entity_tags
BEGIN
  SELECT CASE
    WHEN NEW.entity_tag_entity_type NOT IN ('work','expression','manifestation','item','file')
    THEN RAISE(ABORT, 'entity_tags.entity_tag_entity_type must be one of: work, expression, manifestation, item, file')
  END;

  SELECT CASE
    WHEN NEW.entity_tag_entity_type = 'work'
     AND NOT EXISTS (SELECT 1 FROM works WHERE work_id = NEW.entity_tag_entity_id)
    THEN RAISE(ABORT, 'entity_tags references missing work_id')
    WHEN NEW.entity_tag_entity_type = 'expression'
     AND NOT EXISTS (SELECT 1 FROM expressions WHERE expression_id = NEW.entity_tag_entity_id)
    THEN RAISE(ABORT, 'entity_tags references missing expression_id')
    WHEN NEW.entity_tag_entity_type = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM manifestations WHERE manifestation_id = NEW.entity_tag_entity_id)
    THEN RAISE(ABORT, 'entity_tags references missing manifestation_id')
    WHEN NEW.entity_tag_entity_type = 'item'
     AND NOT EXISTS (SELECT 1 FROM items WHERE item_id = NEW.entity_tag_entity_id)
    THEN RAISE(ABORT, 'entity_tags references missing item_id')
    WHEN NEW.entity_tag_entity_type = 'file'
     AND NOT EXISTS (SELECT 1 FROM files WHERE file_id = NEW.entity_tag_entity_id)
    THEN RAISE(ABORT, 'entity_tags references missing file_id')
  END;

  SELECT CASE
    WHEN NEW.entity_tag_ord IS NOT NULL AND NEW.entity_tag_ord < 0
    THEN RAISE(ABORT, 'entity_tags.entity_tag_ord must be >= 0 when provided')
  END;
END;
-- BREAK


-- =====================================================
-- synopses / notes / comments / ratings / covers
-- =====================================================

-- Helper rule: entity types for text metadata (exclude file unless specified)
-- synopses: work/expression/manifestation/item
CREATE TRIGGER IF NOT EXISTS trg_synopses_validate_entity_ref
BEFORE INSERT ON synopses
BEGIN
  SELECT CASE
    WHEN NEW.synopsis_entity_type NOT IN ('work','expression','manifestation','item')
    THEN RAISE(ABORT, 'synopses.synopsis_entity_type must be one of: work, expression, manifestation, item')
  END;

  SELECT CASE
    WHEN NEW.synopsis_entity_type = 'work'
     AND NOT EXISTS (SELECT 1 FROM works WHERE work_id = NEW.synopsis_entity_id)
    THEN RAISE(ABORT, 'synopses references missing work_id')
    WHEN NEW.synopsis_entity_type = 'expression'
     AND NOT EXISTS (SELECT 1 FROM expressions WHERE expression_id = NEW.synopsis_entity_id)
    THEN RAISE(ABORT, 'synopses references missing expression_id')
    WHEN NEW.synopsis_entity_type = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM manifestations WHERE manifestation_id = NEW.synopsis_entity_id)
    THEN RAISE(ABORT, 'synopses references missing manifestation_id')
    WHEN NEW.synopsis_entity_type = 'item'
     AND NOT EXISTS (SELECT 1 FROM items WHERE item_id = NEW.synopsis_entity_id)
    THEN RAISE(ABORT, 'synopses references missing item_id')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_synopses_validate_entity_ref_upd
BEFORE UPDATE OF synopsis_entity_type, synopsis_entity_id ON synopses
BEGIN
  SELECT CASE
    WHEN NEW.synopsis_entity_type NOT IN ('work','expression','manifestation','item')
    THEN RAISE(ABORT, 'synopses.synopsis_entity_type must be one of: work, expression, manifestation, item')
  END;

  SELECT CASE
    WHEN NEW.synopsis_entity_type = 'work'
     AND NOT EXISTS (SELECT 1 FROM works WHERE work_id = NEW.synopsis_entity_id)
    THEN RAISE(ABORT, 'synopses references missing work_id')
    WHEN NEW.synopsis_entity_type = 'expression'
     AND NOT EXISTS (SELECT 1 FROM expressions WHERE expression_id = NEW.synopsis_entity_id)
    THEN RAISE(ABORT, 'synopses references missing expression_id')
    WHEN NEW.synopsis_entity_type = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM manifestations WHERE manifestation_id = NEW.synopsis_entity_id)
    THEN RAISE(ABORT, 'synopses references missing manifestation_id')
    WHEN NEW.synopsis_entity_type = 'item'
     AND NOT EXISTS (SELECT 1 FROM items WHERE item_id = NEW.synopsis_entity_id)
    THEN RAISE(ABORT, 'synopses references missing item_id')
  END;
END;
-- BREAK

-- notes: work/expression/manifestation/item/file
CREATE TRIGGER IF NOT EXISTS trg_notes_validate_entity_ref
BEFORE INSERT ON notes
BEGIN
  SELECT CASE
    WHEN NEW.note_entity_type NOT IN ('work','expression','manifestation','item','file')
    THEN RAISE(ABORT, 'notes.note_entity_type must be one of: work, expression, manifestation, item, file')
  END;

  SELECT CASE
    WHEN NEW.note_entity_type = 'work'
     AND NOT EXISTS (SELECT 1 FROM works WHERE work_id = NEW.note_entity_id)
    THEN RAISE(ABORT, 'notes references missing work_id')
    WHEN NEW.note_entity_type = 'expression'
     AND NOT EXISTS (SELECT 1 FROM expressions WHERE expression_id = NEW.note_entity_id)
    THEN RAISE(ABORT, 'notes references missing expression_id')
    WHEN NEW.note_entity_type = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM manifestations WHERE manifestation_id = NEW.note_entity_id)
    THEN RAISE(ABORT, 'notes references missing manifestation_id')
    WHEN NEW.note_entity_type = 'item'
     AND NOT EXISTS (SELECT 1 FROM items WHERE item_id = NEW.note_entity_id)
    THEN RAISE(ABORT, 'notes references missing item_id')
    WHEN NEW.note_entity_type = 'file'
     AND NOT EXISTS (SELECT 1 FROM files WHERE file_id = NEW.note_entity_id)
    THEN RAISE(ABORT, 'notes references missing file_id')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_notes_validate_entity_ref_upd
BEFORE UPDATE OF note_entity_type, note_entity_id ON notes
BEGIN
  SELECT CASE
    WHEN NEW.note_entity_type NOT IN ('work','expression','manifestation','item','file')
    THEN RAISE(ABORT, 'notes.note_entity_type must be one of: work, expression, manifestation, item, file')
  END;

  SELECT CASE
    WHEN NEW.note_entity_type = 'work'
     AND NOT EXISTS (SELECT 1 FROM works WHERE work_id = NEW.note_entity_id)
    THEN RAISE(ABORT, 'notes references missing work_id')
    WHEN NEW.note_entity_type = 'expression'
     AND NOT EXISTS (SELECT 1 FROM expressions WHERE expression_id = NEW.note_entity_id)
    THEN RAISE(ABORT, 'notes references missing expression_id')
    WHEN NEW.note_entity_type = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM manifestations WHERE manifestation_id = NEW.note_entity_id)
    THEN RAISE(ABORT, 'notes references missing manifestation_id')
    WHEN NEW.note_entity_type = 'item'
     AND NOT EXISTS (SELECT 1 FROM items WHERE item_id = NEW.note_entity_id)
    THEN RAISE(ABORT, 'notes references missing item_id')
    WHEN NEW.note_entity_type = 'file'
     AND NOT EXISTS (SELECT 1 FROM files WHERE file_id = NEW.note_entity_id)
    THEN RAISE(ABORT, 'notes references missing file_id')
  END;
END;
-- BREAK

-- comments: work/expression/manifestation/item/file
CREATE TRIGGER IF NOT EXISTS trg_comments_validate_entity_ref
BEFORE INSERT ON comments
BEGIN
  SELECT CASE
    WHEN NEW.comment_entity_type NOT IN ('work','expression','manifestation','item','file')
    THEN RAISE(ABORT, 'comments.comment_entity_type must be one of: work, expression, manifestation, item, file')
  END;

  SELECT CASE
    WHEN NEW.comment_entity_type = 'work'
     AND NOT EXISTS (SELECT 1 FROM works WHERE work_id = NEW.comment_entity_id)
    THEN RAISE(ABORT, 'comments references missing work_id')
    WHEN NEW.comment_entity_type = 'expression'
     AND NOT EXISTS (SELECT 1 FROM expressions WHERE expression_id = NEW.comment_entity_id)
    THEN RAISE(ABORT, 'comments references missing expression_id')
    WHEN NEW.comment_entity_type = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM manifestations WHERE manifestation_id = NEW.comment_entity_id)
    THEN RAISE(ABORT, 'comments references missing manifestation_id')
    WHEN NEW.comment_entity_type = 'item'
     AND NOT EXISTS (SELECT 1 FROM items WHERE item_id = NEW.comment_entity_id)
    THEN RAISE(ABORT, 'comments references missing item_id')
    WHEN NEW.comment_entity_type = 'file'
     AND NOT EXISTS (SELECT 1 FROM files WHERE file_id = NEW.comment_entity_id)
    THEN RAISE(ABORT, 'comments references missing file_id')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_comments_validate_entity_ref_upd
BEFORE UPDATE OF comment_entity_type, comment_entity_id ON comments
BEGIN
  SELECT CASE
    WHEN NEW.comment_entity_type NOT IN ('work','expression','manifestation','item','file')
    THEN RAISE(ABORT, 'comments.comment_entity_type must be one of: work, expression, manifestation, item, file')
  END;

  SELECT CASE
    WHEN NEW.comment_entity_type = 'work'
     AND NOT EXISTS (SELECT 1 FROM works WHERE work_id = NEW.comment_entity_id)
    THEN RAISE(ABORT, 'comments references missing work_id')
    WHEN NEW.comment_entity_type = 'expression'
     AND NOT EXISTS (SELECT 1 FROM expressions WHERE expression_id = NEW.comment_entity_id)
    THEN RAISE(ABORT, 'comments references missing expression_id')
    WHEN NEW.comment_entity_type = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM manifestations WHERE manifestation_id = NEW.comment_entity_id)
    THEN RAISE(ABORT, 'comments references missing manifestation_id')
    WHEN NEW.comment_entity_type = 'item'
     AND NOT EXISTS (SELECT 1 FROM items WHERE item_id = NEW.comment_entity_id)
    THEN RAISE(ABORT, 'comments references missing item_id')
    WHEN NEW.comment_entity_type = 'file'
     AND NOT EXISTS (SELECT 1 FROM files WHERE file_id = NEW.comment_entity_id)
    THEN RAISE(ABORT, 'comments references missing file_id')
  END;
END;
-- BREAK

-- ratings: work/expression/manifestation/item
CREATE TRIGGER IF NOT EXISTS trg_ratings_validate_entity_ref_and_range
BEFORE INSERT ON ratings
BEGIN
  SELECT CASE
    WHEN NEW.rating_entity_type NOT IN ('work','expression','manifestation','item')
    THEN RAISE(ABORT, 'ratings.rating_entity_type must be one of: work, expression, manifestation, item')
  END;

  SELECT CASE
    WHEN NEW.rating_entity_type = 'work'
     AND NOT EXISTS (SELECT 1 FROM works WHERE work_id = NEW.rating_entity_id)
    THEN RAISE(ABORT, 'ratings references missing work_id')
    WHEN NEW.rating_entity_type = 'expression'
     AND NOT EXISTS (SELECT 1 FROM expressions WHERE expression_id = NEW.rating_entity_id)
    THEN RAISE(ABORT, 'ratings references missing expression_id')
    WHEN NEW.rating_entity_type = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM manifestations WHERE manifestation_id = NEW.rating_entity_id)
    THEN RAISE(ABORT, 'ratings references missing manifestation_id')
    WHEN NEW.rating_entity_type = 'item'
     AND NOT EXISTS (SELECT 1 FROM items WHERE item_id = NEW.rating_entity_id)
    THEN RAISE(ABORT, 'ratings references missing item_id')
  END;

  SELECT CASE
    WHEN NEW.rating_scale IS NOT NULL AND NEW.rating_scale <= 0
    THEN RAISE(ABORT, 'ratings.rating_scale must be > 0 when provided')
    WHEN NEW.rating_value < 0
    THEN RAISE(ABORT, 'ratings.rating_value must be >= 0')
    WHEN NEW.rating_scale IS NOT NULL AND NEW.rating_value > NEW.rating_scale
    THEN RAISE(ABORT, 'ratings.rating_value must be <= ratings.rating_scale')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_ratings_validate_entity_ref_and_range_upd
BEFORE UPDATE OF rating_entity_type, rating_entity_id, rating_value, rating_scale ON ratings
BEGIN
  SELECT CASE
    WHEN NEW.rating_entity_type NOT IN ('work','expression','manifestation','item')
    THEN RAISE(ABORT, 'ratings.rating_entity_type must be one of: work, expression, manifestation, item')
  END;

  SELECT CASE
    WHEN NEW.rating_entity_type = 'work'
     AND NOT EXISTS (SELECT 1 FROM works WHERE work_id = NEW.rating_entity_id)
    THEN RAISE(ABORT, 'ratings references missing work_id')
    WHEN NEW.rating_entity_type = 'expression'
     AND NOT EXISTS (SELECT 1 FROM expressions WHERE expression_id = NEW.rating_entity_id)
    THEN RAISE(ABORT, 'ratings references missing expression_id')
    WHEN NEW.rating_entity_type = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM manifestations WHERE manifestation_id = NEW.rating_entity_id)
    THEN RAISE(ABORT, 'ratings references missing manifestation_id')
    WHEN NEW.rating_entity_type = 'item'
     AND NOT EXISTS (SELECT 1 FROM items WHERE item_id = NEW.rating_entity_id)
    THEN RAISE(ABORT, 'ratings references missing item_id')
  END;

  SELECT CASE
    WHEN NEW.rating_scale IS NOT NULL AND NEW.rating_scale <= 0
    THEN RAISE(ABORT, 'ratings.rating_scale must be > 0 when provided')
    WHEN NEW.rating_value < 0
    THEN RAISE(ABORT, 'ratings.rating_value must be >= 0')
    WHEN NEW.rating_scale IS NOT NULL AND NEW.rating_value > NEW.rating_scale
    THEN RAISE(ABORT, 'ratings.rating_value must be <= ratings.rating_scale')
  END;
END;
-- BREAK

-- covers: work/expression/manifestation/item
-- enforce valid entity type + entity exists + only one primary cover per entity
CREATE TRIGGER IF NOT EXISTS trg_covers_validate_entity_ref
BEFORE INSERT ON covers
BEGIN
  SELECT CASE
    WHEN NEW.cover_entity_type NOT IN ('work','expression','manifestation','item')
    THEN RAISE(ABORT, 'covers.cover_entity_type must be one of: work, expression, manifestation, item')
  END;

  SELECT CASE
    WHEN NEW.cover_entity_type = 'work'
     AND NOT EXISTS (SELECT 1 FROM works WHERE work_id = NEW.cover_entity_id)
    THEN RAISE(ABORT, 'covers references missing work_id')
    WHEN NEW.cover_entity_type = 'expression'
     AND NOT EXISTS (SELECT 1 FROM expressions WHERE expression_id = NEW.cover_entity_id)
    THEN RAISE(ABORT, 'covers references missing expression_id')
    WHEN NEW.cover_entity_type = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM manifestations WHERE manifestation_id = NEW.cover_entity_id)
    THEN RAISE(ABORT, 'covers references missing manifestation_id')
    WHEN NEW.cover_entity_type = 'item'
     AND NOT EXISTS (SELECT 1 FROM items WHERE item_id = NEW.cover_entity_id)
    THEN RAISE(ABORT, 'covers references missing item_id')
  END;

  SELECT CASE
    WHEN NEW.cover_is_primary IS NOT NULL AND NEW.cover_is_primary NOT IN (0,1)
    THEN RAISE(ABORT, 'covers.cover_is_primary must be 0, 1, or NULL')
  END;

  SELECT CASE
    WHEN NEW.cover_is_primary = 1
     AND EXISTS (
       SELECT 1 FROM covers
       WHERE cover_entity_type = NEW.cover_entity_type
         AND cover_entity_id = NEW.cover_entity_id
         AND cover_is_primary = 1
       LIMIT 1
     )
    THEN RAISE(ABORT, 'covers: a primary cover already exists for this entity')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_covers_validate_entity_ref_upd
BEFORE UPDATE OF cover_entity_type, cover_entity_id, cover_is_primary ON covers
BEGIN
  SELECT CASE
    WHEN NEW.cover_entity_type NOT IN ('work','expression','manifestation','item')
    THEN RAISE(ABORT, 'covers.cover_entity_type must be one of: work, expression, manifestation, item')
  END;

  SELECT CASE
    WHEN NEW.cover_entity_type = 'work'
     AND NOT EXISTS (SELECT 1 FROM works WHERE work_id = NEW.cover_entity_id)
    THEN RAISE(ABORT, 'covers references missing work_id')
    WHEN NEW.cover_entity_type = 'expression'
     AND NOT EXISTS (SELECT 1 FROM expressions WHERE expression_id = NEW.cover_entity_id)
    THEN RAISE(ABORT, 'covers references missing expression_id')
    WHEN NEW.cover_entity_type = 'manifestation'
     AND NOT EXISTS (SELECT 1 FROM manifestations WHERE manifestation_id = NEW.cover_entity_id)
    THEN RAISE(ABORT, 'covers references missing manifestation_id')
    WHEN NEW.cover_entity_type = 'item'
     AND NOT EXISTS (SELECT 1 FROM items WHERE item_id = NEW.cover_entity_id)
    THEN RAISE(ABORT, 'covers references missing item_id')
  END;

  SELECT CASE
    WHEN NEW.cover_is_primary IS NOT NULL AND NEW.cover_is_primary NOT IN (0,1)
    THEN RAISE(ABORT, 'covers.cover_is_primary must be 0, 1, or NULL')
  END;

  -- If setting to primary, ensure no other primary exists (excluding this row)
  SELECT CASE
    WHEN NEW.cover_is_primary = 1
     AND EXISTS (
       SELECT 1 FROM covers
       WHERE cover_entity_type = NEW.cover_entity_type
         AND cover_entity_id = NEW.cover_entity_id
         AND cover_is_primary = 1
         AND cover_id != OLD.cover_id
       LIMIT 1
     )
    THEN RAISE(ABORT, 'covers: a primary cover already exists for this entity')
  END;
END;
-- BREAK


-- =====================================================
-- tags / genres / subjects basic hygiene
-- =====================================================

CREATE TRIGGER IF NOT EXISTS trg_tags_nonempty
BEFORE INSERT ON tags
BEGIN
  SELECT CASE
    WHEN NEW.tag_text IS NULL OR LENGTH(TRIM(NEW.tag_text)) = 0
    THEN RAISE(ABORT, 'tags.tag_text must be non-empty')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_tags_nonempty_upd
BEFORE UPDATE OF tag_text ON tags
BEGIN
  SELECT CASE
    WHEN NEW.tag_text IS NULL OR LENGTH(TRIM(NEW.tag_text)) = 0
    THEN RAISE(ABORT, 'tags.tag_text must be non-empty')
  END;
END;
-- BREAK


-- =====================================================
-- pictures: ensure file exists and is image-like when mime is present
-- =====================================================

CREATE TRIGGER IF NOT EXISTS trg_pictures_file_must_be_image_when_mime_known
BEFORE INSERT ON pictures
BEGIN
  SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM files WHERE file_id = NEW.picture_file_id)
    THEN RAISE(ABORT, 'pictures.picture_file_id references missing file_id')
  END;

  SELECT CASE
    WHEN EXISTS (
      SELECT 1 FROM files
      WHERE file_id = NEW.picture_file_id
        AND file_mime_type IS NOT NULL
        AND file_mime_type NOT LIKE 'image/%'
      LIMIT 1
    )
    THEN RAISE(ABORT, 'pictures.picture_file_id must reference an image/* file_mime_type when mime type is set')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_pictures_file_must_be_image_when_mime_known_upd
BEFORE UPDATE OF picture_file_id ON pictures
BEGIN
  SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM files WHERE file_id = NEW.picture_file_id)
    THEN RAISE(ABORT, 'pictures.picture_file_id references missing file_id')
  END;

  SELECT CASE
    WHEN EXISTS (
      SELECT 1 FROM files
      WHERE file_id = NEW.picture_file_id
        AND file_mime_type IS NOT NULL
        AND file_mime_type NOT LIKE 'image/%'
      LIMIT 1
    )
    THEN RAISE(ABORT, 'pictures.picture_file_id must reference an image/* file_mime_type when mime type is set')
  END;
END;
-- BREAK


-- =====================================================
-- annotations: simple anchor hygiene (FKs already enforce item/device existence)
-- =====================================================

CREATE TRIGGER IF NOT EXISTS trg_annotations_anchor_nonempty
BEFORE INSERT ON annotations
BEGIN
  SELECT CASE
    WHEN NEW.annotation_anchor_start IS NULL OR LENGTH(TRIM(NEW.annotation_anchor_start)) = 0
    THEN RAISE(ABORT, 'annotations.annotation_anchor_start must be non-empty')
    WHEN NEW.annotation_anchor_type IS NULL OR LENGTH(TRIM(NEW.annotation_anchor_type)) = 0
    THEN RAISE(ABORT, 'annotations.annotation_anchor_type must be non-empty')
  END;
END;

CREATE TRIGGER IF NOT EXISTS trg_annotations_anchor_nonempty_upd
BEFORE UPDATE OF annotation_anchor_start, annotation_anchor_type ON annotations
BEGIN
  SELECT CASE
    WHEN NEW.annotation_anchor_start IS NULL OR LENGTH(TRIM(NEW.annotation_anchor_start)) = 0
    THEN RAISE(ABORT, 'annotations.annotation_anchor_start must be non-empty')
    WHEN NEW.annotation_anchor_type IS NULL OR LENGTH(TRIM(NEW.annotation_anchor_type)) = 0
    THEN RAISE(ABORT, 'annotations.annotation_anchor_type must be non-empty')
  END;
END;
-- BREAK
