# Driver-wrapper feature implementation

Completed **2026-11-04**, branch `maintenance-plugin-modes`. The implemented goal is
"If you could implement all of those please?": native schema facts, explicit
read ownership, bounded scans, backend capabilities and optional diagnostics,
plus legacy view-row cleanup and hierarchy cycle detection. No commit/push/PR
was requested. Preserve the large existing maintenance tree, preference files,
user TODOs and dirty `LiuXin_alpha_data` submodule.

The [feature guide](../dev-docs/databases/driver-wrapper-features-2026-11-04.md)
owns the public contracts and examples. DB10, the database index, transaction
guide and earlier wrapper review now point to it. The prior
[boundary/typing handoff](driver-wrapper-followup-2026-11-04.md) records separate
earlier evidence; its remaining-work list is superseded by the new guide.

## Implementation

- SQLite/PG native schema helpers expose nullability, SQL defaults, generated and
  identity columns, native types, ordered keys, indexes and foreign keys. PG
  nested-domain defaults/nullability are covered. SQLite PK nullability quirks,
  expression/partial uniqueness and TEMP shadowing are covered. Public specs
  retain the resolved schema. View nullability is unknown; CHECK expressions
  are not interpreted as a complete migration/validation model.
- `QueryOperationsAPI` and the wrapper query mixin add fetch helpers, scans,
  capabilities and observer registration. API methods remain abstract declarations.
  Explicit/active-macro/primary/fresh selection preserves borrowed transactions;
  cursors and owned handles close before return/yield. Raw SQL remains trusted.
- Scans share `table_pages.read_page` with portable macros. Projection, bound
  equality/NULL filters and exclusive continuation IDs use limit+1 lookahead.
  Non-integer stored SQLite keys are rejected instead of truncated. Callers own
  snapshots and durable checkpoints. `RowPage` lives in `db_types/driver_types.py`
  so API imports do not load the SQL paging implementation.
- Capability reports distinguish SQLite/PG facilities and advertise real SQLite
  bind limits. No finite PG client-side binding limit is invented.
- Optional outer-operation observations expose timings, known returned counts
  and exception class names without SQL/values. Empty hooks do no timing or event
  work. Observer errors are isolated and recursion suppressed. Raw execute does
  not time later cursor fetching; direct driver/macro calls remain outside scope.
- Legacy view-row readers share ownership-safe native inspection/conversion,
  require literal `id`, quote names and use a two-row ambiguity check. PG adapter
  execute/executemany close newly created cursors if execution fails.
- Parent chains and descendant walks reject repeated IDs. Existing API/result
  contracts and comments describe the failure and resource behavior.

## Validation status

- Final focused feature/native-schema/API/tooling: **138 passed**,
  `/tmp/liuxin-wrapper-feature-signoff.log`.
- Broader SQLite/APSW callers, schema and pagination: **230 passed**,
  `/tmp/liuxin-wrapper-feature-integrate.log`.
- Live PostgreSQL wrapper, paging and transactions: **49 passed**,
  `/tmp/liuxin-wrapper-feature-pg-final.log`; final observer/capability/resource
  refinements: **six passed**, `/tmp/liuxin-wrapper-feature-pg-check.log`.
- Full scoped quality runner passed: **303** formatting targets, lint/complexity,
  annotation coverage, import/layer/footprint, zero basedpyright diagnostics and
  **254** mypy sources. Both contract probes accept valid calls and reject all
  **47** invalid examples. `/tmp/liuxin-wrapper-features-quality-signoff.log`.
- Schema, hierarchy and view doctests passed,
  `/tmp/liuxin-wrapper-feature-doctest.log`. `git diff --check` passes.

The first full quality run caught an API import-footprint regression. Moving
`RowPage` into the existing type module removed unnecessary implementation
imports; all 25 cold-import budgets now pass without raising limits. Final lint
also prompted precise backend exception assertions and static inspection of
async callable objects, now tested on both backend families.

All **114** local links in the five edited guides resolve, including untracked
files intended for the eventual commit. All three protected preference hashes
match `/tmp/liuxin-maintenance-signoff-preferences.json`. Scratch/config/cache and
telemetry are under `/tmp`, with per-test scratch discarded. Selections overlap;
there is no full-repository or real-file import claim.

The private PostgreSQL cluster on port 55438 is stopped and removed. Its retained
server log is `/tmp/liuxin-wrapper-features-pg-server.log`. The path record under
`/tmp` is historical; do not try to reuse that deleted cluster.

## Delivery and remaining work

Changes remain uncommitted. Include the new native schema modules, schema/view
read helpers, shared paging, wrapper query API/mixin/diagnostics, capability and
observation types, three new feature/native test files, guide and this handoff
when eventually preparing a commit. Preserve the earlier untracked wrapper
review, implementation base, driver types and regression files too.

Custom-column placeholders/SQLite-specific DDL and historical explicit commit
points remain open. Real SquashFS import validation awaits physical server
access. Do not describe fetch helpers as a SQL firewall or resumable IDs as a
durable snapshot/exactly-once processing guarantee.
