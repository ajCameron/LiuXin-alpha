# Catalog full pass

Date: 2026-07-22

## Current conclusion

Catalog is fit for its declared core semantic-persistence purpose. The facade,
repositories, matching, retrieval, projections, mutation policy, coordinated
writes, and normalized writers are concrete over the real WEMI schema. Legacy
Calibre-oriented helpers remain only as an explicitly deprecated compatibility
boundary.

The complete review, acceptance audit, durable contracts, caveats, and evidence
are in
`dev-docs/catalog-fitness-review.md`.

## Evidence

- The latest catalog tree has `391` passing tests in `825.07s`, including real
  SQLite/APSW semantic, traversal, merge, rollback, ownership, writer, search,
  field metadata, protocol, SQL-boundary, and extended matching coverage.
- Final focused semantic/search coverage: `16 passed in 89.39s`.
- Portable macro contract/harness: `23 passed, 1 skipped in 9.41s`.
- Portable macro real database coverage: `8 passed in 30.91s`.
- Focused strict mypy: `63` catalog modules, no issues.
- Every review-time concrete `NotImplementedError` path is implemented or now
  raises a domain/input error. Only intentional abstract writer hooks remain.
- Catalog contains no executable SQL and no direct commit calls.
- Full-project static checking remains a separate legacy baseline: mypy reports
  `26,426` errors across `694` files when following imports; basedpyright reports
  `2,574` errors and `2,602` warnings across configured legacy scopes.

## Implemented architecture

1. `BaseRepository` owns validated row CRUD, aliases, matching helpers, and
   schema-discovered relationship access through portable macros.
2. Work/Expression and Expression/Manifestation traverse real many-to-many
   links; Manifestation/Item uses the Item foreign key.
3. Titles write WEMI-owned columns; identifiers use entity-owned rows.
4. Matching results are deterministic and explained. Bundles choose a stable
   WEMI path and carry aggregate metadata plus link data.
5. Attachment and merge run in one nestable portable transaction. Merge keeps
   target relationship metadata on overlap and moves owned Items.
6. Shared raw-value writer build is pure; resolution, creation, combined
   cardinality validation, and replacement are atomic during apply.
7. Link ownership is explicit and independent of cardinality.
8. Search/field-metadata Python 3 failures are fixed; legacy mutation surfaces
   are deprecated and no longer commit directly.

## Important correction to earlier writer notes

The correction is now resolved. `LinkUpdate.replace_links_bulk()` and owned
one-to-one replacement remain atomic. Shared raw-value build no longer ensures
destination rows; it records unresolved references. Apply resolves/creates
destinations and performs the final link replacement inside the same portable
transaction, so the complete call is now all-or-nothing.

## Handoff notes

- Treat `Catalog` repositories and normalized writers as the only extension
  points for new catalog persistence.
- Treat `catalog_macros` and `metadata_tools` as migration-only compatibility
  packages; do not add features there.
- A bundle is one deterministic path through a many-to-many graph, not a full
  graph export.
- Logical title IDs equal WEMI entity IDs. Identifier assignment copies an
  already-owned identifier rather than moving it.
- Abstract writer `NotImplementedError` hooks are intentional; concrete catalog
  services contain none.
