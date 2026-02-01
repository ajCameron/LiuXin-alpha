# LiuXin schema notes (FRBR + Storage)

This document summarises the “main tables” agreed in this chat: the FRBR WEMI backbone plus a storage model (stores/folders/files), plus a hybrid identifiers strategy.

This intentionally excludes:
- lookup/type tables (roles, languages, etc.)
- subtables (agent_person, agent_organisation, etc.)
- link tables (entity_agents, work_relations, entity_tags, file_derivations, etc.)
- enforcement triggers (recommended, but reviewed separately)

---

## 1) FRBR WEMI: Works → Expressions → Manifestations → Items

### Work
**Work** is the abstract thing: the “platonic” creative object (book, film, TV series, etc.).

Key points:
- Works may exist as stubs (no expressions yet).
- Work holds stable, concept-level metadata: canonical title, medium/type, original language/year, broad classification.

### Expression
**Expression** is a particular realisation of the Work: translation, revision, director’s cut, audiobook performance, etc.

Key points:
- Expression is where *content-changing* properties live (wordcount, cut type, duration, translation language).
- Expression titles should usually be derived from Work title; override only when genuinely necessary.

### Expression attributes
`expression_attributes` is the structured escape hatch: extra expression facts as key/value pairs, indexed for search.
- It avoids JSON blobs in core bibliographic entities.
- When keys become important, they can later be promoted to first-class columns.

### Manifestation
**Manifestation** is an edition/release/product packaging of an Expression: hardback vs paperback, EPUB release, Blu-ray edition, etc.
- Holds carrier/format and publication details.
- Does not directly embed ISBN/ASIN fields in this “main tables only” cut; identifiers are handled by the hybrid identifiers model.

### Item
**Item** is a concrete copy of a Manifestation: your physical book, your downloaded file copy, your disc, etc.
- Stores per-copy provenance and lifecycle facts (source, acquisition date, inventory code, location, condition).

---

## 2) Identifiers: hybrid strategy

We separated identifiers into two layers:

### `item_identifiers` (observed identifiers)
Raw identifiers observed on a specific Item (copy):
- barcode on the physical object
- identifiers found in file metadata
- scraped/embedded identifiers

This preserves evidence without forcing early canonical decisions.

### `entity_identifiers` (curated identifiers)
A normalised identifier set for any entity type (work/expression/manifestation/item):
- includes `is_primary` for “canonical” identifiers
- includes `provenance` to distinguish derived vs imported vs manual identifiers

This enables fast lookups (“find manifestation by ISBN”) while keeping raw observations intact.

---

## 3) Storage model: Stores → Folders → Files

### Store
A Store is a logical backend:
- local filesystem
- NAS share
- tape / sequential archive media
- rclone remotes (e.g. Google Drive)
- read-only web mirrors (random sites)

Stores include:
- addressing (`store_root_uri`) which may change over time
- auth pointers (`store_auth_method`, `store_credentials`) — should not store plaintext secrets
- policy (`store_storage_mask`, `store_policy_json`)
- telemetry (`store_last_seen_online`, `store_last_healthcheck_ok`)
- explicit capabilities (folders, random read, atomic rename, delete support, etc.)

Capabilities exist as first-class columns to keep backend behaviour explicit and enforceable.

### Folder
Folders are an optional hierarchy within a store:
- used when a store supports folder-like namespaces
- omitted for stores where folders are meaningless (tape/packfiles) or undesired
- include `folder_relpath` as a cached relative path and optional policy overrides

### File
Files are stored binary objects, optionally tied to Items.
Key rule:
- canonical locator is `(file_store_id, file_storage_key)`
- `file_storage_key` is **relative** to `stores.store_root_uri`
  - This avoids rewriting all file rows if the store root changes.

Files also carry:
- integrity: size + hashes + last-seen + last-integrity-check
- classification: 
  - `file_class_mask` for placement/category
  - `file_visibility_mask` for privacy/visibility controls
- provenance and pipeline placeholders for later derivation graphs

---

## 4) Enforcement triggers (recommended)

In this chat we designed triggers (not included in the “main tables only” SQL file) to enforce invariants such as:
- if a store does not support folders, `file_folder_id` must be NULL
- folder and file store IDs must match
- `file_storage_key` must be non-empty and relative (no scheme, no leading slash)
- optional strict handling of read-only stores

These can be added once the table layout is approved.

---

## 5) Next steps (link tables)

To reach “archival-quality calibre compatibility”, the next layer is link tables:
- entity ↔ agents with roles + ordering
- work ↔ work relations (adaptation_of, contains, inspired_by, rewrite_of, etc.)
- tags/genres/subjects via entity_tags
- file derivation lineage (file_derivations) to replace `file_parent`

These can be added without changing the WEMI + storage foundations.
