# Core Interface Consolidation

Date: 2026-07-28
Status: implemented; focused acceptance green

## Outcome

LiuXin's maintained application interfaces now use `CoreClientAPI` as their
sole application boundary. The same interface code can attach to an in-process
`CoreRuntime` or an HTTP Core daemon; it does not receive a more privileged
object graph in local mode.

The migrated set is:

- native read-only and read-write web;
- Calibre-shaped web, JSON API, and OPDS;
- browse, acquisition, and image delivery;
- terminal browser and commands;
- Tk backend and desktop entry point;
- SquashFS publication CLI;
- maintained web/API/OPDS runner scripts and the surface-path benchmark.

## Shared seam

`LiuXin_alpha.surfaces.core` owns application composition and wire-shaped
surface adapters:

- `SurfaceCoreSession` opens either `--database` or `--core-endpoint`;
- `CoreSurfaceModel` provides schema, row, relation, and acquisition views over
  stable named operations;
- `CoreDatabaseView` and `CoreDriverView` preserve presentation-facing row
  ergonomics without exposing Database or driver objects;
- `enclose_legacy_database(...)` keeps older embedding/tests working while
  containing the supplied object at the composition boundary.

Caller-supplied legacy resources remain caller-owned. Short-lived compatibility
sessions do not close the process-global job manager.

## Core additions and repairs

- Added named SquashFS store/file publication jobs so the CLI no longer calls
  storage code directly.
- Moved synchronization work into Core and made terminal foreground/background
  behavior use the common jobs API.
- Made `schema.tables` return cheap summaries while `schema.table` supplies
  relation detail.
- Kept identifier-less lookup views inside Core's driver adaptation; interfaces
  receive ordinary wire records without Database row-materialization failures.
- Preserved structured acquisition-unavailable errors across direct and RPC
  delivery.

## Enforcement

`tests/surfaces/test_core_boundary_enforcement.py` AST-checks production surface
modules for owned-subsystem imports and generic `invoke` use. The only narrow
exceptions are PostgreSQL service provisioning and presentation-only Calibre
metadata/thumbnail helpers. Development fixture generators, subsystem
benchmarks, build scripts, and backend diagnostics are not application
interfaces.

`tests/surfaces/test_core_surface_acceptance.py` exercises the same runtime
through `LocalCoreClient` and `RemoteCoreClient`, covering HTML, JSON, OPDS,
read-write POST, terminal, and Tk behavior.

## Verification

- Core application/program API: `28 passed`
- Core synchronization runtime: `1 passed`
- SquashFS CLI, both configured SQLite backends: `4 passed`
- Direct/RPC cross-surface acceptance and boundary enforcement: green
- API, OPDS, acquisition, acceptance, boundary, and runner-help selection:
  `45 passed` after the delegated-wrapper expectation was corrected
- Tk, catalog, image, and read-model regression: `36 passed, 1 skipped`
- Tk focused lane: `16 passed, 1 skipped`
- Changed terminal synchronization/windowed cases: `11 passed`
- Critical read-write relation lifecycle, SQLite: `1 passed`

The full coverage suite remains the external sign-off run; it was intentionally
not duplicated during this focused migration pass.
