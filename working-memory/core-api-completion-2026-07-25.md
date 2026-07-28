# Core API Completion

Date: 2026-07-25

> Superseded for current interface work by the Core API `2.0`
> [whole-program audit](core-program-api-audit-2026-07-28.md) and
> [Core API contract](../dev-docs/core-api.md). This note remains the
> historical v1 foundation checkpoint.

## Outcome

Core is now the application composition root intended by the architecture. The
same `CoreClientAPI` can invoke it directly or through an HTTP RPC proxy, and
stable handlers return the same transport-shaped values on both paths.

The foundational v1 API phase was complete enough to begin interface
consolidation. The current durable contract is:
[Core API contract](../dev-docs/core-api.md).

## Implemented

- Added `CoreClientAPI`, direct and remote Core clients, envelope/correlation
  parity, typed event subscriptions, structured remote errors, and public
  `create_core(...)` / `core_client(...)` helpers.
- Made Core own composition of Library, Database, Catalog, optional Cache,
  metadata read source, jobs, events, and lifecycle.
- Added stable named schema/row/relation, Catalog, matching, Agent, WEMI,
  metadata/OPF, normalized field-write, admin, storage-file, cache, and
  read-source operations.
- Added canonical wire conversion for rows, dataclasses, bytes, temporal
  values, decimals, paths, UUIDs, mappings, sets, and sequences.
- Moved generic metadata write workflows out of the surface package, removing
  the Core-to-surface dependency.
- Preserved canonical-write receipts across cache reconciliation failures.
- Kept generic `invoke` as an explicitly transport-unstable compatibility
  escape hatch.

## Defect closed

The real cache-backed Catalog writer exposed a stale relation projection after
creating a destination value inside a transaction. Storage-cache invalidation
was accidentally calling freshness-aware accessors, which could reload and
clear a table's stale marker before the committed row became visible.

Schema-backed invalidation now only marks dependencies. Refresh remains lazy
until the transaction has closed. The original real-database regression and
the Core field-write/read acceptance path both pass.

## Verification

- Core fast/broad lane: `50 passed, 2 deselected`
- Core wire-value contract: `6 passed`
- Core application API unit/RPC lane: `10 passed, 2 deselected`
- Core real Catalog/Cache acceptance on both configured drivers:
  `2 passed in 375.67s`
- Finalized-code SQLite Core acceptance rerun: `1 passed in 184.67s`
- Original real cache writer regression: `1 passed in 48.85s`
- Cache facade/plugin/schema lanes: `104 passed, 4 skipped`
- Focused invalidation/deferred-refresh checks: `6 passed, 1 skipped`
- Strict mypy over the nine Core contract/implementation files:
  `Success: no issues found`
- Basedpyright error-level audit over the same files:
  `0 errors, 0 warnings`

The real Core acceptance covers Catalog create/update/delete, WEMI stack
creation and retrieval, metadata hydration, OPF bytes, metadata tag write-back,
match-or-create, person Agent creation, normalized field writing, cache
reconciliation receipts, cache-backed row reads, and cache-backed relation
reads.

## Consolidation handoff

Next work should migrate web, terminal, Tk, and automation surfaces to
`CoreClientAPI` and remove direct service access and generic `invoke` usage.
Long-running acquisition/import/conversion/indexing operations should submit
jobs through named Core commands. A dedicated streaming file transport can be
added later without changing the application API.
