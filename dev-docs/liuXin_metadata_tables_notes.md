# LiuXin metadata tables (satellites) — notes

This adds the “metadata satellites” that sit around the core FRBR WEMI + storage model.

**Goal:** keep core entities clean and stable, while still supporting rich, searchable metadata and future Calibre compatibility.

---

## Languages
A simple lookup for language codes and names. Core entities reference `*_language_id` as INT; this table supplies the mapping.

---

## Agents + Roles + Entity agents
Instead of separate publisher/creator tables, we use a single `agents` table with an `agent_type` column.

`roles` is a lookup for contributor roles (author, illustrator, translator, publisher, etc.).

`entity_agents` is the glue table:
- attaches agents to *any* entity type (work/expression/manifestation/item)
- assigns a role
- supports ordering (`ord`) for display and canonical name formatting

This matches your earlier “roles on links + ordering + uniqueness” approach.

---

## Series + Work series
`series` supports optional hierarchy via `series_parent_id`.

`work_series` attaches works to series with `position` (REAL to allow 2.5 etc.).

---

## Tags / Genres / Subjects
We separate these because they serve different purposes:
- **tags**: flexible, user/system labels (collections, workflows, placement hints)
- **genres**: controlled classification, often hierarchical
- **subjects**: library-style subject headings, also hierarchical

Links:
- `entity_tags` applies tags to any entity type (including files)
- `work_genres` and `work_subjects` apply controlled vocabularies at the Work level

This supports your “metadata-driven store placement” goal without forcing JSON into core bibliographic tables.

---

## Synopses / Notes / Comments
These provide user-facing and imported text:
- `synopses` are display summaries (often imported)
- `notes` are richer user notes (titled, user-scoped)
- `comments` are simpler “quick comments”

All three use the same pattern: `(entity_type, entity_id)` polymorphic pointer.

---

## Ratings
`ratings` supports multiple sources (manual, imported, Goodreads/IMDB, etc.) and multiple scales.

---

## Pictures + Covers
`pictures` are references to image `files` (usually covers, posters, stills).

`covers` selects a specific picture as the primary UI image for an entity. This keeps image storage separate from “cover selection” semantics.

---

## Devices + Annotations
`devices` stores reading devices for send-to-device flows.

`annotations` stores Kindle-style highlights/notes/bookmarks anchored to Items, with flexible anchoring (CFI, locations, timecodes).

---

## What remains (next)
These tables still defer:
- work↔work relationships (adaptation_of, contains, inspired_by, etc.)
- file derivation graphs (replace `files.file_parent`)
- stricter enforcement triggers for these satellites (optional)
