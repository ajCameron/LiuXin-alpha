CREATE TABLE annotations (
    annotation_id               INTEGER PRIMARY KEY,
    annotation_user_id          INTEGER NOT NULL,
    annotation_item_id          INTEGER NOT NULL,  -- which copy/file they annotated
    annotation_kind             TEXT NOT NULL,     -- 'highlight', 'note', 'bookmark', 'clip'

    annotation_anchor_type      TEXT NOT NULL,     -- 'cfi', 'percentage', 'page_offset',
                                        -- 'timecode', 'frame', 'loc', etc.
    annotation_anchor_start     TEXT NOT NULL,     -- encoded position
    annotation_anchor_end       TEXT,              -- for ranges; null for bookmarks

    annotation_selected_text    TEXT,              -- optional snapshot of highlight
    annotation_note_text        TEXT,              -- user’s note (if any)

    annotation_created_at       DATETIME DEFAULT (STRFTIME('%s', 'now')),
    annotation_updated_at       DATETIME DEFAULT (STRFTIME('%s', 'now')),
    annotation_deleted_at       DATETIME DEFAULT (STRFTIME('%s', 'now')),        -- soft-delete for sync

    annotation_source           TEXT,              -- 'kindle_import', 'kobo', 'internal'
    annotation_device_id        INTEGER,           -- FK to devices, nullable
    annotation_extra_json       TEXT               -- room for vendor-specific cruft
);