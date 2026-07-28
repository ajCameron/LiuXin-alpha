# Core Program API Audit

Date: 2026-07-28
Status: implemented and accepted as Core API `2.0`

## Objective

Core must be sufficient as the sole application boundary for a complete
LiuXin client. A local client and an RPC client must be able to perform the
same operations without receiving process-owned Database, Catalog, Cache,
Storage, Row, plugin, stream, or callback objects.

This is broader than the 2026-07-25 consolidation milestone. The required
surface is the application-facing union of the public subsystem APIs, reduced
only where several low-level methods are implementation details of one
application operation.

## Sources audited

- `core`, `library`, `catalog`, `caches`, `databases`, `metadata`, `storage`,
  `ingest`, `file_formats.conversion`, `jobs`, and database maintenance APIs;
- terminal, Tk, read-only web, read/write web, Calibre-compatible, OPDS, and
  acquisition surfaces;
- existing Core descriptors and direct/HTTP proxy tests.

## Inclusion rule

Include capabilities needed to:

- discover and operate a library;
- inspect, search, browse, create, edit, relate, merge, and delete catalog
  data;
- read and write metadata in the database and in files;
- manage preferences, custom fields, schema policy, and normalized identity;
- register, inspect, probe, select, and remove stores;
- read, place, copy, update, locate, and delete files;
- manage digital assets, composite assets, replicas, item links, and storage
  policies;
- ingest local and remote sources;
- inspect and run format conversion;
- plan and execute backup workflows;
- inspect and run database maintenance;
- submit, monitor, cancel, inspect results for, and read logs from long-running
  jobs;
- obtain all data needed by browse, search, OPDS, acquisition, and GUI
  interfaces without direct subsystem access.

## Deliberate exclusions

- low-level driver connections, cursors, SQL execution, trigger construction,
  schema generators, and lock-breaking internals;
- cache table/field implementation objects;
- plugin instances, configuration widgets, and GUI callbacks;
- HTTP/WSGI response construction and other presentation-only behavior;
- Python file handles, callbacks, Row objects, StoreLocation objects, and
  arbitrary callable submission.

These are implemented behind Core or remain internal. Their user-visible
effects still require Core operations.

## Implemented capability matrix

| Area | Core API `2.0` result | Local acceptance |
| --- | --- | --- |
| Lifecycle/discovery | health, description, shutdown, and machine-readable whole-program capability report | direct/RPC capability equality and complete matrix |
| Jobs/events | list/get/wait/result/log/cancel, subscriptions, and named serializable workflow submission | captured named requests plus result/log behavior |
| Database | info, summary, telemetry, driver backup/vacuum, schema/rows/relations/admin CRUD | both local SQLite drivers; maintenance availability remains driver-dependent |
| Schema policy and identity | column/link policy read/update; identity list/get/derive/resolve/audit/migrate | schema mutation and real identity resolution/audit |
| Tree semantics | root/children/lineage/walk/search/nest/delete | real recursive traversal, cycle prevention, and subtree deletion |
| Preferences | list/get/set/delete | locally composed preferences service |
| Custom fields | list/create/update/delete with stable wire records | real schema mutation on both SQLite drivers |
| Catalog | repository CRUD, WEMI, matching, metadata, field descriptions, Identifiers, Agents, hierarchy, and global search | representative semantic create/read/link/search operations |
| Metadata | database read/write and OPF export; file formats/inspect/write; online source/identify/cover jobs | real minimal-EPUB read/write; online results are plugin/network-dependent |
| Stores/files | store detail/probe/delete/default, source registration, file list/read/put/copy/delete/locate/stat | real local store, file placement, location, and byte read |
| Assets/policies | closed semantic resource registry, asset/composite/replica/item/member links, policies, workflow-owned records | real resource CRUD, composite traversal, assessment, and placement planning |
| Ingest | format discovery plus local-disk and remote-HTML named jobs | serializable named job proof; input/network requirements declared |
| Conversion | formats, options, and named conversion job | serializable named job proof; plugin/tool requirements declared |
| Backup | planning, durable workflow list/get/save/start, resumable state/output, and SquashFS job | real persistence/direct-RPC parity; execution tools declared |
| Maintenance | status, duplicates, generic plugin run, clean, merge, database backup/vacuum | representative local queries; plugin/driver support declared |
| Browse/acquisition | categories, category values, works/detail, format enumeration, resolve/read/cover | real WEMI and managed composite-to-replica traversal with byte read |

The canonical operation lists live in `CoreProgramAPI` and are returned by
`capabilities.list`. `api.describe` provides every payload descriptor.

## Availability result

Every included capability maps to a registered stable operation with both
`direct` and `rpc` call modes. `complete_program_boundary=true` means that the
contract is complete. It intentionally does not claim that external
dependencies are installed.

The capability report labels database maintenance, some source registration,
disk/remote ingest, online metadata, conversion, and backup execution as
conditional and states the dependency for each operation. These are usable
local implementations when their declared driver, path, network, plugin, or
tool requirements are met; they are not silent placeholders.

## Proof standard

Completion requires:

1. every included capability maps to a stable named Core operation;
2. `api.describe` documents every payload;
3. direct and HTTP RPC clients return identical wire values and structured
   failures;
4. representative tests cover each capability family;
5. real SQLite acceptance covers the standard local implementation;
6. the matrix identifies any capability whose underlying subsystem is itself
   only a declared placeholder, rather than silently claiming support.

All six conditions are satisfied. Focused program/API acceptance covers every
family, performs direct/RPC equality checks, and exercises both configured
SQLite implementations. The whole Core test lane and the cache/schema support
lane provide the final regression evidence recorded below.

## Verification

- Complete Core lane: `69 passed in 103.52s`
- Expanded program and jobs acceptance after final cleanup:
  `21 passed in 27.94s`
- Schema-backed cache, custom-column, and driver-schema regression:
  `149 passed, 2 skipped in 93.53s`
- Strict mypy over eight Core v2 implementation files: no issues
- Basedpyright over the same implementation: `0 errors`
  (warnings remain at dynamic legacy/plugin boundaries)
- Python bytecode compilation: clean
- `git diff --check`: clean
