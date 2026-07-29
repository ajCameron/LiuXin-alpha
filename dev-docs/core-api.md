# Core Application API

Status: complete whole-program boundary and consolidated application interfaces
API version: `2.0`
Updated: 2026-07-28

## Boundary

`CoreRuntime` is LiuXin's application composition root. A complete client can
use it without constructing or retaining a Library, Database, Catalog, Cache,
StorageManager, metadata plugin, conversion plugin, or job manager.

The supported call paths are:

```text
surface ──> LocalCoreClient ─────────────> CoreRuntime
   │
   └──────> RemoteCoreClient ──HTTP RPC──> CoreRuntime
```

Both clients execute the same named command/query handlers and return the same
wire-shaped values. Direct callers do not receive privileged Python objects.
Presentation, HTTP response construction, and GUI state remain outside Core.

Use `create_core(...)` to compose the basic local implementation. Use
`core_client(runtime=...)` or `core_client(endpoint=...)` when code should be
indifferent to local and RPC deployment:

```python
from LiuXin_alpha.core import core_client, create_core

runtime = create_core(
    database_path="/path/to/library.db",
    cache_type="schema_backed",
)
client = core_client(runtime=runtime)

result = client.query(
    "search.global",
    {"text": "earth", "limit": 50},
)
```

The equivalent RPC client is:

```python
client = core_client(endpoint="http://127.0.0.1:8765")
```

## Discovery and compatibility

`api.describe` is authoritative for every registered operation, payload,
summary, tag, and transport-stability flag.

`capabilities.list` is the program-level compatibility contract. It reports:

- `complete_program_boundary`: whether every operation in the v2 program
  capability matrix is registered;
- each capability family, its operation names, and any missing operations;
- per-operation direct/RPC call modes and local implementation status;
- operations whose successful execution depends on the selected driver,
  plugins, network, paths, or external tools.

`complete_program_boundary` describes contract coverage, not the availability
of every external dependency. Interfaces can therefore fail fast or hide an
environment-dependent action without probing internal subsystem objects.

The command and query named `invoke` remain compatibility/diagnostic escape
hatches and are marked `transport_stable=false`. New interface code must use a
stable named operation.

## Whole-program capability surface

The tables below summarize the required v2 program boundary.
`api.describe` remains the complete machine-readable list.

| Family | Stable named operations |
| --- | --- |
| Lifecycle | `health`, `api.describe`, `capabilities.list`, `shutdown` |
| Jobs | `jobs.list`, `jobs.get`, `jobs.wait`, `jobs.result`, `jobs.log.read`, `jobs.cancel` |
| Database | `database.info`, `database.summary`, `database.telemetry`, `database.backup`, `database.vacuum` |
| Schema and identity | `schema.tables`, `schema.table`, `schema.column`, `schema.link`, `schema.column.update`, `schema.identities.list`, `schema.identity.get`, `schema.identity.derive`, `schema.identity.resolve`, `schema.identities.audit`, `schema.identities.migrate` |
| Trees | `tree.root`, `tree.children`, `tree.lineage`, `tree.walk`, `tree.search`, `tree.nest`, `tree.delete` |
| Preferences | `preferences.list`, `preferences.get`, `preferences.set`, `preferences.delete` |
| Custom fields | `custom-fields.list`, `custom-fields.create`, `custom-fields.update`, `custom-fields.delete` |
| Catalog | entity CRUD, hierarchy, Identifier replacement, Agent listing/linking, field metadata, matching, bundles, WEMI creation, and semantic metadata writes under `catalog.*` |
| Search and relations | `rows.get`, `rows.query`, `relations.list`, `search.global` |
| Browse | `browse.categories`, `browse.category.items`, `browse.works`, `browse.work` |
| Acquisition | `acquisition.formats`, `acquisition.resolve`, `acquisition.read`, `acquisition.cover` |
| Metadata | database hydration/write and OPF export, file format/inspect/write, online source discovery, and identify/cover jobs under `metadata.*` |
| Stores and files | store list/get/save/probe/delete/default, file list/locate/read/put/copy/delete, location stat, source registration, refresh and synchronization under `storage.*` and `sync.*` |
| Assets and policies | resource describe/list/get/CRUD, asset detail/policy assignment, policy assessment/planning/violations under `storage.*` |
| Ingest | `ingest.formats`, `ingest.disk.start`, `ingest.remote-html.start` |
| Conversion | `conversion.formats`, `conversion.options`, `conversion.start` |
| Backup | `backup.plan`, workflow list/get/save/start, `backup.squashfs.start`, `backup.squashfs.publish-store.start`, and `backup.squashfs.publish-files.start` |
| Maintenance | `maintenance.status`, `maintenance.duplicates.find`, `maintenance.run`, `maintenance.clean`, `maintenance.merge` |

Explicit raw row and relation mutations remain available under `admin.*`.
Cache/read-source lifecycle operations remain available under `cache.*` and
`read-source.*`. They support administration and compatibility; ordinary
interfaces should prefer semantic operations.

### Storage graph

Managed storage uses a closed, schema-backed resource registry rather than an
arbitrary-table API. It covers digital assets, composite assets, replicas,
item links, composite members, replication policies, backup policies, workflow
records, workflow state, workflow output, and workflow presence.

Writable resources support semantic create/update/delete operations. Workflow
owned state/output/presence resources are read-only. Policy operations assess
current replicas, expose violations, and propose placements. Acquisition
resolution traverses legacy files and managed item-to-asset or
item-to-composite-to-member-to-replica paths.

### Browse and acquisition

Browse operations return display-neutral categories, category items, work
summaries, work detail, and related entities. Acquisition operations enumerate
formats, resolve Core-readable versus redirect delivery, return bytes in the
normal wire representation, and retrieve cover candidates. Web, OPDS, terminal,
and GUI surfaces are responsible only for presentation and delivery framing.

### Long-running work

Long-running ingest, remote discovery, online metadata, conversion,
synchronization, and backup actions are named commands that submit serializable
`JobRequest` values. Clients use the common jobs surface to wait, cancel, read
results, and read logs. No application interface needs arbitrary callable
submission or direct access to a job backend.

Persisted backup workflows retain their specification and resumable step
state, checkpoint after each step, and register outputs. SquashFS remains an
explicit workflow rather than an implicit storage side effect.

## Conditional execution

The basic local implementation registers the entire program contract. The
following executions are deliberately reported as conditional:

- database backup/vacuum require support from the selected driver;
- some storage source registration needs network access or external tools;
- disk and remote HTML ingest require a path-backed database and readable
  input; remote ingest also needs network access;
- online identify/cover results depend on configured plugins and network
  access;
- conversion requires suitable format plugins and any tools they invoke;
- persisted and SquashFS backup execution requires a path-backed database and
  workflow-specific tools such as `mksquashfs`.

These are dependency conditions, not missing Core endpoints. Failures use the
same structured Core error path in direct and RPC modes.

## Read, write, and reconciliation semantics

Core selects one structured read source. With a configured Cache, structured
rows, relations, and metadata hydration use the cache-backed source. Without
one, the same endpoints evaluate against the Database.

Semantic catalog writes go through Catalog. Normalized field writes go through
the Cache facade when configured. Custom-field/schema mutations identify
themselves as schema changes; ordinary data writes use data-only cache reload.

If a canonical write commits but cache reconciliation fails, Core raises
`cache_reconciliation_failed`. Its details retain the authoritative receipt
and set `canonical_write_committed=true`; clients must not retry such a write
blindly.

Local query and command execution is serialized by the runtime because the
composed database graph is shared by direct and RPC threads. This preserves
the same operation semantics in both call modes.

## Transport values, errors, and events

Every stable result passes through canonical Core wire conversion, including
direct calls:

- mappings have string keys, with stringification collisions rejected;
- rows and dataclasses become mappings;
- sequences become arrays and sets become deterministic arrays;
- floats must be finite;
- bytes are `{"$type": "bytes", "base64": "..."}`;
- dates, datetimes, and times carry `$type` and ISO text;
- decimals carry `$type: "decimal"` and a string value;
- paths and UUIDs become strings.

Envelope and correlation IDs survive both call modes. Direct failures use the
Core error taxonomy; RPC failures expose the corresponding HTTP status, stable
error code, and details through `RemoteProxyError`.

Subscribers receive typed `CoreEvent` values. RPC subscriptions long-poll the
daemon and return the same unsubscribe-shaped callback as local subscriptions.

## Ownership and interface consolidation

Caller-supplied resources remain caller-owned by default. Resources constructed
by `create_core(...)` are Core-owned and close during shutdown. A supplied
Cache must use Core's Database; a cache created through `cache_type=...` is
owned by Core.

`CoreRuntime.database`, `.catalog`, `.cache`, and `.read_source` exist for
composition, testing, and compatibility. They are not members of
`CoreClientAPI` and must not become interface dependencies.

The application-interface consolidation is complete. Native and Calibre-shaped
web, JSON API, OPDS, acquisition/image delivery, read-write web, terminal, Tk,
SquashFS CLI, and maintained surface runner scripts all receive one
`CoreClientAPI`. Each accepts either local composition through `--database` or
an existing daemon through `--core-endpoint` where it has a command-line entry
point. The same interface model and wire records are used in both modes.

`surfaces.core.SurfaceCoreSession` is the only application composition seam.
It owns locally created runtimes, borrows remote clients, and encloses
caller-supplied legacy Database objects for compatibility tests and older
embedding code. Those compatibility constructors do not make the Database
available to surface behavior.

Production surface modules are checked by an AST boundary test which rejects
owned-subsystem imports and generic `invoke` calls. The narrow exceptions are:

- the PostgreSQL CLI, which provisions or diagnoses the database service before
  an application Core can exist;
- pure Calibre metadata rendering helpers and thumbnail value/error types,
  which perform presentation-only work and do not retain subsystem services.

Development-only fixture generators, subsystem benchmarks, build scripts, and
backend diagnostics are outside the application-interface boundary. Maintained
application automation, including `benchmark_surface_paths.py`, goes through
Core.

Interfaces use `capabilities.list` for compatibility and dependency-aware
feature exposure, and keep presentation state, formatting, and protocol
response construction outside Core.

A streaming transport can later optimize large byte transfers without changing
the application-level acquisition or storage operations.
