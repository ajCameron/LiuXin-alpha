-- LiuXin-alpha (FRBR + Storage) : Metadata & satellite tables
-- Notes:
--  - SQLite DDL
--  - Includes "metadata tables" + essential link tables for applying metadata to entities
--  - Avoids subtables/types tables beyond simple lookups
--  - Assumes main tables already exist: works, expressions, manifestations, items, files, stores, folders
--  - Uses the -- BREAK markers as section boundaries

PRAGMA foreign_keys = ON;

-- =====================================================
-- LANGUAGES (lookup)
-- =====================================================

CREATE TABLE IF NOT EXISTS `languages` (
  `language_id` INTEGER PRIMARY KEY,
  `language_code` TEXT NOT NULL UNIQUE,   -- e.g. 'en', 'fr', 'ja', 'de'
  `language_name` TEXT NULL,              -- e.g. 'English'
  `language_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `language_scratch` TEXT NULL
);
-- BREAK


-- =====================================================
-- AGENTS (single table; no subtables)
-- =====================================================

CREATE TABLE IF NOT EXISTS `agents` (
  `agent_id` INTEGER PRIMARY KEY,

  `agent_type` TEXT NOT NULL,             -- 'person', 'organisation', 'group', 'pseudonym'
  `agent_canonical_name` TEXT NOT NULL,
  `agent_sort_name` TEXT NULL,

  `agent_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `agent_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `agent_last_modified` DATETIME DEFAULT CURRENT_TIMESTAMP,

  `agent_scratch` TEXT NULL
);

CREATE INDEX IF NOT EXISTS `idx_agents_sort_name`
ON `agents` (`agent_sort_name`);
-- BREAK


-- =====================================================
-- ROLES (lookup)
-- =====================================================

CREATE TABLE IF NOT EXISTS `roles` (
  `role_id` INTEGER PRIMARY KEY,

  `role_code` TEXT NOT NULL UNIQUE,    -- e.g. 'author', 'illustrator', 'translator', 'publisher'
  `role_label` TEXT NOT NULL,          -- human readable
  `role_scope` TEXT NULL,              -- 'work','expression','manifestation','item' (advisory)

  `role_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `role_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `role_scratch` TEXT NULL
);
-- BREAK


-- =====================================================
-- ENTITY ↔ AGENT LINKS (metadata glue; ordered; role-based)
-- =====================================================

CREATE TABLE IF NOT EXISTS `entity_agents` (
  `entity_agent_id` INTEGER PRIMARY KEY,

  `entity_agent_entity_type` TEXT NOT NULL, -- 'work','expression','manifestation','item'
  `entity_agent_entity_id`   INT NOT NULL,

  `entity_agent_agent_id` INT NOT NULL,
  `entity_agent_role_id`  INT NOT NULL,

  `entity_agent_ord` INT NULL,              -- ordering within the same role
  `entity_agent_note` TEXT NULL,

  `entity_agent_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `entity_agent_scratch` TEXT NULL,

  CONSTRAINT `entity_agent_agent_fk`
    FOREIGN KEY (`entity_agent_agent_id`)
    REFERENCES `agents` (`agent_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `entity_agent_role_fk`
    FOREIGN KEY (`entity_agent_role_id`)
    REFERENCES `roles` (`role_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS `idx_entity_agents_entity`
ON `entity_agents` (`entity_agent_entity_type`, `entity_agent_entity_id`);

CREATE INDEX IF NOT EXISTS `idx_entity_agents_agent`
ON `entity_agents` (`entity_agent_agent_id`);

CREATE UNIQUE INDEX IF NOT EXISTS `idx_entity_agents_unique_order`
ON `entity_agents` (
  `entity_agent_entity_type`,
  `entity_agent_entity_id`,
  `entity_agent_role_id`,
  `entity_agent_agent_id`,
  `entity_agent_ord`
);
-- BREAK


-- =====================================================
-- SERIES (metadata)
-- =====================================================

CREATE TABLE IF NOT EXISTS `series` (
  `series_id` INTEGER PRIMARY KEY,

  `series_name` TEXT NOT NULL,
  `series_sort` TEXT NULL,
  `series_parent_id` INT NULL,        -- optional hierarchy

  `series_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `series_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `series_last_modified` DATETIME DEFAULT CURRENT_TIMESTAMP,

  `series_scratch` TEXT NULL,

  CONSTRAINT `series_parent_fk`
    FOREIGN KEY (`series_parent_id`)
    REFERENCES `series` (`series_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS `idx_series_parent`
ON `series` (`series_parent_id`);
-- BREAK


-- -----------------------------------------------------
-- Link: Works ↔ Series (order within series)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `work_series` (
  `work_series_id` INTEGER PRIMARY KEY,

  `work_series_work_id` INT NOT NULL,
  `work_series_series_id` INT NOT NULL,

  `work_series_position` REAL NULL,     -- supports 1,2,2.5 etc.
  `work_series_note` TEXT NULL,

  `work_series_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `work_series_scratch` TEXT NULL,

  CONSTRAINT `work_series_work_fk`
    FOREIGN KEY (`work_series_work_id`)
    REFERENCES `works` (`work_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `work_series_series_fk`
    FOREIGN KEY (`work_series_series_id`)
    REFERENCES `series` (`series_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS `idx_work_series_unique`
ON `work_series` (`work_series_work_id`, `work_series_series_id`);

CREATE INDEX IF NOT EXISTS `idx_work_series_series`
ON `work_series` (`work_series_series_id`);
-- BREAK


-- =====================================================
-- TAGS / GENRES / SUBJECTS (controlled vocab)
-- =====================================================

CREATE TABLE IF NOT EXISTS `tags` (
  `tag_id` INTEGER PRIMARY KEY,
  `tag_text` TEXT NOT NULL UNIQUE,
  `tag_kind` TEXT NULL,                 -- 'freeform','system','collection','visibility','placement',...
  `tag_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `tag_scratch` TEXT NULL
);
-- BREAK


CREATE TABLE IF NOT EXISTS `genres` (
  `genre_id` INTEGER PRIMARY KEY,
  `genre_name` TEXT NOT NULL UNIQUE,
  `genre_parent_id` INT NULL,           -- optional hierarchy
  `genre_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `genre_scratch` TEXT NULL,

  CONSTRAINT `genre_parent_fk`
    FOREIGN KEY (`genre_parent_id`)
    REFERENCES `genres` (`genre_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS `idx_genres_parent`
ON `genres` (`genre_parent_id`);
-- BREAK


CREATE TABLE IF NOT EXISTS `subjects` (
  `subject_id` INTEGER PRIMARY KEY,
  `subject_name` TEXT NOT NULL UNIQUE,
  `subject_parent_id` INT NULL,         -- optional hierarchy
  `subject_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `subject_scratch` TEXT NULL,

  CONSTRAINT `subject_parent_fk`
    FOREIGN KEY (`subject_parent_id`)
    REFERENCES `subjects` (`subject_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS `idx_subjects_parent`
ON `subjects` (`subject_parent_id`);
-- BREAK


-- -----------------------------------------------------
-- Link: Entity ↔ Tags (applies to work/expression/manifestation/item/file)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `entity_tags` (
  `entity_tag_id` INTEGER PRIMARY KEY,

  `entity_tag_entity_type` TEXT NOT NULL, -- 'work','expression','manifestation','item','file'
  `entity_tag_entity_id`   INT NOT NULL,

  `entity_tag_tag_id` INT NOT NULL,
  `entity_tag_ord` INT NULL,
  `entity_tag_note` TEXT NULL,

  `entity_tag_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `entity_tag_scratch` TEXT NULL,

  CONSTRAINT `entity_tag_tag_fk`
    FOREIGN KEY (`entity_tag_tag_id`)
    REFERENCES `tags` (`tag_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS `idx_entity_tags_entity`
ON `entity_tags` (`entity_tag_entity_type`, `entity_tag_entity_id`);

CREATE INDEX IF NOT EXISTS `idx_entity_tags_tag`
ON `entity_tags` (`entity_tag_tag_id`);

CREATE UNIQUE INDEX IF NOT EXISTS `idx_entity_tags_unique`
ON `entity_tags` (`entity_tag_entity_type`, `entity_tag_entity_id`, `entity_tag_tag_id`);
-- BREAK


-- -----------------------------------------------------
-- Link: Work ↔ Genres (genre classification is usually work-level)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `work_genres` (
  `work_genre_id` INTEGER PRIMARY KEY,

  `work_genre_work_id` INT NOT NULL,
  `work_genre_genre_id` INT NOT NULL,
  `work_genre_ord` INT NULL,

  `work_genre_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `work_genre_scratch` TEXT NULL,

  CONSTRAINT `work_genre_work_fk`
    FOREIGN KEY (`work_genre_work_id`)
    REFERENCES `works` (`work_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `work_genre_genre_fk`
    FOREIGN KEY (`work_genre_genre_id`)
    REFERENCES `genres` (`genre_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS `idx_work_genres_unique`
ON `work_genres` (`work_genre_work_id`, `work_genre_genre_id`);
-- BREAK


-- -----------------------------------------------------
-- Link: Work ↔ Subjects (subject headings are usually work-level)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `work_subjects` (
  `work_subject_id` INTEGER PRIMARY KEY,

  `work_subject_work_id` INT NOT NULL,
  `work_subject_subject_id` INT NOT NULL,
  `work_subject_ord` INT NULL,

  `work_subject_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `work_subject_scratch` TEXT NULL,

  CONSTRAINT `work_subject_work_fk`
    FOREIGN KEY (`work_subject_work_id`)
    REFERENCES `works` (`work_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `work_subject_subject_fk`
    FOREIGN KEY (`work_subject_subject_id`)
    REFERENCES `subjects` (`subject_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS `idx_work_subjects_unique`
ON `work_subjects` (`work_subject_work_id`, `work_subject_subject_id`);
-- BREAK


-- =====================================================
-- USER-FACING TEXT: SYNOPSES / NOTES / COMMENTS
-- =====================================================

CREATE TABLE IF NOT EXISTS `synopses` (
  `synopsis_id` INTEGER PRIMARY KEY,

  `synopsis_entity_type` TEXT NOT NULL, -- 'work','expression','manifestation','item'
  `synopsis_entity_id` INT NOT NULL,

  `synopsis_source` TEXT NULL,          -- 'manual','import','calibre','web'
  `synopsis_text` TEXT NOT NULL,

  `synopsis_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `synopsis_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `synopsis_last_modified` DATETIME DEFAULT CURRENT_TIMESTAMP,

  `synopsis_scratch` TEXT NULL
);

CREATE INDEX IF NOT EXISTS `idx_synopses_entity`
ON `synopses` (`synopsis_entity_type`, `synopsis_entity_id`);
-- BREAK


CREATE TABLE IF NOT EXISTS `notes` (
  `note_id` INTEGER PRIMARY KEY,

  `note_user` TEXT NULL,                -- freeform for now; user_id later
  `note_entity_type` TEXT NOT NULL,     -- 'work','expression','manifestation','item','file'
  `note_entity_id` INT NOT NULL,

  `note_title` TEXT NULL,
  `note_text` TEXT NOT NULL,
  `note_source` TEXT NULL,              -- 'manual','import',...

  `note_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `note_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `note_last_modified` DATETIME DEFAULT CURRENT_TIMESTAMP,

  `note_scratch` TEXT NULL
);

CREATE INDEX IF NOT EXISTS `idx_notes_entity`
ON `notes` (`note_entity_type`, `note_entity_id`);
-- BREAK


CREATE TABLE IF NOT EXISTS `comments` (
  `comment_id` INTEGER PRIMARY KEY,

  `comment_user` TEXT NULL,
  `comment_entity_type` TEXT NOT NULL,  -- 'work','expression','manifestation','item','file'
  `comment_entity_id` INT NOT NULL,

  `comment_text` TEXT NOT NULL,
  `comment_source` TEXT NULL,           -- 'manual','import',...

  `comment_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `comment_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `comment_last_modified` DATETIME DEFAULT CURRENT_TIMESTAMP,

  `comment_scratch` TEXT NULL
);

CREATE INDEX IF NOT EXISTS `idx_comments_entity`
ON `comments` (`comment_entity_type`, `comment_entity_id`);
-- BREAK


-- =====================================================
-- RATINGS
-- =====================================================

CREATE TABLE IF NOT EXISTS `ratings` (
  `rating_id` INTEGER PRIMARY KEY,

  `rating_user` TEXT NULL,
  `rating_entity_type` TEXT NOT NULL,  -- 'work','expression','manifestation','item'
  `rating_entity_id` INT NOT NULL,

  `rating_value` REAL NOT NULL,        -- allow 0-5, 0-10, etc. based on your UI
  `rating_scale` REAL NULL,            -- optional, e.g. 5 or 10
  `rating_source` TEXT NULL,           -- 'manual','import','goodreads','imdb',...
  `rating_note` TEXT NULL,

  `rating_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `rating_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `rating_last_modified` DATETIME DEFAULT CURRENT_TIMESTAMP,

  `rating_scratch` TEXT NULL
);

CREATE INDEX IF NOT EXISTS `idx_ratings_entity`
ON `ratings` (`rating_entity_type`, `rating_entity_id`);

CREATE INDEX IF NOT EXISTS `idx_ratings_user`
ON `ratings` (`rating_user`);
-- BREAK


-- =====================================================
-- IMAGES / COVERS
-- =====================================================

-- Pictures are files (usually images) referenced for UI
CREATE TABLE IF NOT EXISTS `pictures` (
  `picture_id` INTEGER PRIMARY KEY,

  `picture_file_id` INT NOT NULL,     -- points to files row
  `picture_kind` TEXT NULL,           -- 'cover','author_photo','still','poster',...
  `picture_width` INT NULL,
  `picture_height` INT NULL,

  `picture_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `picture_scratch` TEXT NULL,

  CONSTRAINT `picture_file_fk`
    FOREIGN KEY (`picture_file_id`)
    REFERENCES `files` (`file_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS `idx_pictures_file`
ON `pictures` (`picture_file_id`);
-- BREAK


-- Cover = a selected picture for an entity (UI primary image)
CREATE TABLE IF NOT EXISTS `covers` (
  `cover_id` INTEGER PRIMARY KEY,

  `cover_entity_type` TEXT NOT NULL,  -- 'work','expression','manifestation','item'
  `cover_entity_id` INT NOT NULL,

  `cover_picture_id` INT NOT NULL,
  `cover_is_primary` INT NULL,        -- 1 for canonical cover if multiple
  `cover_note` TEXT NULL,

  `cover_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `cover_scratch` TEXT NULL,

  CONSTRAINT `cover_picture_fk`
    FOREIGN KEY (`cover_picture_id`)
    REFERENCES `pictures` (`picture_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS `idx_covers_entity`
ON `covers` (`cover_entity_type`, `cover_entity_id`);

CREATE INDEX IF NOT EXISTS `idx_covers_picture`
ON `covers` (`cover_picture_id`);
-- BREAK


-- =====================================================
-- DEVICES + ANNOTATIONS (Kindle-style)
-- =====================================================

CREATE TABLE IF NOT EXISTS `devices` (
  `device_id` INTEGER PRIMARY KEY,

  `device_user` TEXT NOT NULL,
  `device_name` TEXT NOT NULL,          -- 'Kindle Paperwhite', ...
  `device_kind` TEXT NOT NULL,          -- 'kindle','kobo','android','ios',...
  `device_address` TEXT NULL,           -- email/serial/etc.

  `device_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `device_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `device_last_modified` DATETIME DEFAULT CURRENT_TIMESTAMP,

  `device_scratch` TEXT NULL
);
-- BREAK


CREATE TABLE IF NOT EXISTS `annotations` (
  `annotation_id` INTEGER PRIMARY KEY,

  `annotation_user` TEXT NOT NULL,
  `annotation_item_id` INT NOT NULL,
  `annotation_kind` TEXT NOT NULL,         -- 'highlight','note','bookmark','clip'

  `annotation_anchor_type` TEXT NOT NULL,  -- 'cfi','loc','percentage','timecode',...
  `annotation_anchor_start` TEXT NOT NULL,
  `annotation_anchor_end` TEXT NULL,

  `annotation_selected_text` TEXT NULL,
  `annotation_note_text` TEXT NULL,

  `annotation_source` TEXT NULL,           -- 'kindle_import','kobo','internal'
  `annotation_device_id` INT NULL,

  `annotation_created_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `annotation_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
  `annotation_last_modified` DATETIME DEFAULT CURRENT_TIMESTAMP,

  `annotation_scratch` TEXT NULL,

  CONSTRAINT `annotation_item_fk`
    FOREIGN KEY (`annotation_item_id`)
    REFERENCES `items` (`item_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,

  CONSTRAINT `annotation_device_fk`
    FOREIGN KEY (`annotation_device_id`)
    REFERENCES `devices` (`device_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS `idx_annotations_item`
ON `annotations` (`annotation_item_id`);

CREATE INDEX IF NOT EXISTS `idx_annotations_user`
ON `annotations` (`annotation_user`);
-- BREAK
