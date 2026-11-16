# Native custom-field cache ownership — 2026-11-12

The user explicitly accepts cache/update coupling in the Calibre facade for now,
but wants all cache responsibilities removed from the native service. This is a
follow-up to [the package boundary change](custom-columns-native-boundary-2026-11-12.md).

## Implementation

- `CustomFieldService` now accepts only its database. Removed FieldMetadata state,
  descriptor preparation/publication and table registry maintenance. Its remaining
  subscriptions deliver committed native write receipts, with no cache dependency.
- `caches/custom_fields.py::CustomFieldCache` owns the descriptor projection.
  `write()` composes an outer native transaction, prepares before commit and
  publishes afterward. `refresh()` reads a snapshot and defers publication inside
  a managed transaction. Nested scopes follow savepoint rollback and completion
  order. Native receipt listeners can run before the separate cache callback.
- Core native commands and initial metadata construction use that coordinator.
  Direct native writes intentionally do not update a caller's FieldMetadata.
- `Database.refresh_db_metadata()` now synchronizes the wrapper's table category
  aliases as part of its existing generic post-transaction refresh. Native custom
  fields no longer patch those registries themselves.
- The Calibre facade is unchanged. No v1 storage support or forwarding aliases
  were reintroduced. Native cold imports do not load caches or FieldMetadata.
- Added 22 SQLite, APSW and live PostgreSQL regression cases to the mandatory
  manifest without removing any of its previous 2,131 requirements.

## Workspace and evidence

Work is uncommitted on `main` at `a88a5230e819c8de40518228b34e70584283f4cb`.
Preserve all earlier changes, the real index and dirty data submodule. No agents
or skills used. Evidence is in `.tmp/custom-columns-cache-ownership-2026-11-12/`.
`before/`, `baseline.json` and `state.json` capture the incoming dirty files/state.
`preservation.json` verifies HEAD/index/submodule and all eight facade files are
unchanged. Run Python with `.venv/bin/python` and `PYTHONPATH="$PWD/src"`.

## Validation

- `focused.log`: 244 passed; two Core query cases caught an unintended write-scope
  insertion into the query handler. Corrected that handler; `core-rerun.log`
  passes both. Combined evidence covers all 246 focused SQLite/APSW cases.
- `quality.log`: full quality passes, including lint, architecture, 25 cold-import
  budgets, basedpyright, mypy (473 files), and 95/93 rejected invalid contracts.
  `format-final.log`: final expanded format scope passes all 358 files.
- `collection.json`: all 2,153 required cases collect (2,264 total).
  `ratchet-preservation.json` confirms every prior requirement remains assigned.
- `callers.log`: 330 passed across Core, database facade/wrapper, transaction,
  Calibre facade and tooling contracts.
- `docs.log`: 22 documentation checks passed, using the existing disposable
  alternate index under `/tmp/liuxin-unified-doc-index-_6f701h3`.
- `postgres.log`: all 69 live PostgreSQL cases passed, including the eight new
  cache/registry contracts.
- `validation-summary.json`: 667 selected cases have passing evidence across
  focused/corrective runs, plus full quality. This was targeted validation, not a
  rerun of the entire database regression gate. Final whitespace checks passed.

Private PostgreSQL test server is **stopped** after validation: PG16, port 55439,
data `/tmp/liuxin-cc-followup-pg/data`, UTF-8 database `liuxin_followup_utf8`.
No required work remains. Changes remain uncommitted; no production data changes
or publication occurred.
