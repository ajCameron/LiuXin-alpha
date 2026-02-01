# Tightened triggers (tier-2) — notes

This adds a stricter layer of invariants designed to prevent subtle corruption.

## What’s new

### 1) Cycle prevention in hierarchies
Applies to:
- series
- genres
- subjects
- folders

Rules:
- parent cannot be self
- parent reassignment cannot create a loop

Implemented using recursive CTE ancestry checks.

### 2) Folder relpath normalization
When folder_relpath is present, it must be a clean, relative, forward-slashed path with no traversal segments or trailing slash.

### 3) entity_identifiers strictness
Adds:
- polymorphic reference validation
- non-empty scheme/value checks
- single primary per (entity_type, entity_id, scheme)

### 4) Ordering sanity
Non-negative checks for series positions and ord fields.

### 5) Preferred expression uniqueness
Optional (but useful): at most one preferred expression per work.
