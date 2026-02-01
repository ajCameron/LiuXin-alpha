# LiuXin triggers — notes

This file adds SQLite triggers that enforce invariants at the database layer. All triggers use `RAISE(ABORT, ...)` with explicit messages so violations are immediately diagnosable.

---

## 1) Storage invariants (stores / folders / files)

### Folder support
- If a store does not support folders, `files.file_folder_id` must be NULL.
- You cannot create folders in stores where `store_supports_folders = 0`.

### Folder ↔ store consistency
- If `files.file_folder_id` is set, that folder must belong to the same store as `files.file_store_id`.
- If a folder has a parent (`folders.folder_parent_id`), the parent must be in the same store.

### Relative storage keys
- `files.file_storage_key` must be non-empty.
- It must not include a URI scheme (`://`).
- It must not start with `/` or `\`.
This preserves the “storage_key is relative to store_root_uri” rule.

### Read-only stores (strict DB policy)
- Inserting files into `stores.store_is_read_only = 1` is blocked.
- Moving an existing file into a read-only store is blocked.
This treats read-only stores as catalogue-only and prevents accidental “write intent” recording inside the DB.

### Capability flips
- You cannot change a store from `supports_folders=1` to `0` if it already has folders.
- You cannot change `supports_delete=1` to `0` if it already has files (optional WORM-ish protection).

---

## 2) Polymorphic entity pointers (entity_type/entity_id)

SQLite cannot express “entity_type + entity_id” polymorphic foreign keys directly. Instead, triggers enforce:

- `entity_type` is one of the allowed values for that table
- the referenced `entity_id` exists in the corresponding table

Covered tables:
- `entity_agents`
- `entity_tags`
- `synopses`
- `notes`
- `comments`
- `ratings`
- `covers`

This catches typos and prevents orphaned metadata rows.

---

## 3) Primary cover uniqueness
For `covers`, if `cover_is_primary = 1` then:
- no other primary cover may exist for the same `(cover_entity_type, cover_entity_id)`.

This ensures UI/derivation logic has a single canonical cover when requested.

---

## 4) Basic hygiene triggers
- `tags.tag_text` must be non-empty.
- `pictures.picture_file_id` must reference an existing file; if that file has a mime type set, it must be `image/*`.
- `annotations.annotation_anchor_start` and `annotation_anchor_type` must be non-empty.

---

## Deployment note
Ensure `PRAGMA foreign_keys = ON;` is set for every SQLite connection. Many SQLite libraries require setting it per connection/session.
