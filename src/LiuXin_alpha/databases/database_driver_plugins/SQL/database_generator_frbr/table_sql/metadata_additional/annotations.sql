CREATE TABLE annotations (
    `annotation_id`               INTEGER PRIMARY KEY,
    `annotation_user_id`          INTEGER NOT NULL,
    `annotation_item_id`          INTEGER NOT NULL,  -- which copy/file they annotated
    `annotation_kind`             TEXT NOT NULL,     -- 'highlight', 'note', 'bookmark', 'clip'

    `annotation_anchor_type`      TEXT NOT NULL,     -- 'cfi', 'percentage', 'page_offset',
                                        -- 'timecode', 'frame', 'loc', etc.
    `annotation_anchor_start`     TEXT NOT NULL,     -- encoded position
    `annotation_anchor_end`       TEXT,              -- for ranges; null for bookmarks

    `annotation_selected_text`    TEXT,              -- optional snapshot of highlight
    `annotation_note_text`        TEXT,              -- user’s note (if any)

    `annotation_source_created_datestamp_ep_k` INTEGER NULL,
    `annotation_source_modified_datestamp_ep_k` INTEGER NULL,
    `annotation_source_deleted_datestamp_ep_k` INTEGER NULL,         -- soft-delete for sync

    `annotation_source`           TEXT,              -- 'kindle_import', 'kobo', 'internal'
    `annotation_device_id`        INTEGER,           -- FK to devices, nullable
    `annotation_extra_json`       TEXT,               -- room for vendor-specific cruft,

    -- timestamps (epoch_ms)
    `annotation_created_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
    `annotation_modified_timestamp_ep_k` INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),

    `annotation_scratch` TEXT NULL

);