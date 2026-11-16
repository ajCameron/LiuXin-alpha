-- Base catalogue format, independent of custom-field DDL and module versions.
-- The generator records the revision only after completing its schema build.
CREATE TABLE IF NOT EXISTS `schema_revision` (
    `schema_revision_id` INTEGER PRIMARY KEY CHECK (`schema_revision_id` = 1),
    `schema_revision_revision` INTEGER NOT NULL CHECK (`schema_revision_revision` > 0)
);
