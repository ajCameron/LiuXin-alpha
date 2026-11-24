# Core Application API

Status: **implemented named application API, with explicit ownership and access-control limits**.
API version: `2.0`. Source reviewed **2026-10-20**, working tree based on `fc5de7af`.

## Boundary

`CoreRuntime` is LiuXin's application composition root. A complete client can
use it without constructing or retaining a Library, Database, Catalog, Cache,
StorageManager, metadata plugin, conversion plugin, or job manager.

The supported call paths are:

```text
surface ──> CoreRuntime (implements CoreClientAPI directly)
   │
   └──────> RemoteCoreClient ──HTTP RPC──> CoreRuntime
```

Named stable operations use the same handlers and wire conversion in both modes.
`core_client(runtime=...)` returns that runtime unchanged; it does not wrap it in a
restricted local client. Python callers can still access runtime attributes, and
the generic `invoke` escape hatch is not wire-stable. Treat `CoreClientAPI` as the
interface dependency contract, not an authorization mechanism. Presentation,
HTTP response construction and GUI state remain outside Core.

Use `create_core(...)` to compose the basic local implementation. Use
`core_client(runtime=...)` or `core_client(endpoint=...)` when code should be
indifferent to local and RPC deployment:

```python
from LiuXin_alpha.core import core_client, create_core

runtime = create_core(
    database_path="/path/to/library.db",
    cache_type="schema_backed",
)
try:
    client = core_client(runtime=runtime)
    result = client.query("search.global", {"text": "earth", "limit": 50})
finally:
    runtime.shutdown()
```

The equivalent RPC client is:

```python
client = core_client(endpoint="http://127.0.0.1:8765")
```

## Configuration and reopening

A local Core captures one concrete application store and its database connection
options before bootstrap. Named dispatch, lazy services and cleanup bind that
store; another profile cannot redirect an existing graph. Worker submissions and
retries retain it, while reopened connections carry the captured target, backend
metadata and PostgreSQL schema. SQLite/APSW paths are made absolute.

Application preferences, library preferences and component files keep distinct
owners. Some installation/component singletons remain process-scoped; see the
[capture/reload inventory and worker semantics](configuration-ownership.md).
A remote client's local profile never selects the server's preference store.

## Discovery and compatibility

`api.describe` is authoritative for every registered operation, payload,
summary, tag, and transport-stability flag.

`capabilities.list` snapshots the actual command and query registrations, including
extensions added or replaced after startup. It reports:

- `inventory_complete=true`: every registration in that snapshot is represented;
- families derived from registration tags, with their operation names;
- per-route implementation support, prerequisite declarations, direct/RPC exposure,
  required remote capabilities and transport-stability policy;
- `readiness_probed=false` and per-route `readiness="not_probed"`.

Each operation has a `routes` array so a command and query with the same name keep
their separate contracts. Availability is `available` for a supported implementation
without declared prerequisites, `conditional` when prerequisites are declared, or
`unavailable` when a passive support check reports a missing implementation. An
operation with differing command/query availability reports `mixed`.
Built-in `maintenance.clean` is unavailable; an injected implementation can provide
it. A local-only extension advertises only `direct` in `call_modes`.

This replaces `complete_program_boundary`; completeness refers only to registered
routes, not completion of the whole application. Availability is not a readiness
probe or a grant of permission. Capability inspection does not construct lazy
maintenance or probe tools, drivers or networks; the ordinary dispatch preference
context still applies. Support callbacks must not open resources or start workers. HTTP roles see the same inventory and must
still satisfy each route's server-enforced `remote_capabilities`.

## Request and registration contracts

Core captures each handler, authority declaration and result policy together.
Replacing a route affects later dispatches; an in-flight command or query keeps
the registration selected when it was admitted.

After authorization and before handler execution, dispatch enforces declared
required fields and JSON primitive types. Integer IDs exclude booleans, fractions
and numeric strings; flags require booleans and numbers must be finite. Optional
means a field may be absent. Explicit `null` is accepted only when its declared
type includes `null`. Strings remain strings; required-text adapters reject blanks.
Failures carry `invalid_payload` with the parameter name. Nested domain values and
undeclared extension fields remain the receiving adapter's responsibility; this
is not a generic nested schema validator. Shared adapters also validate identity
arrays and workflow controls before their domain operations.

CLI parsing may turn textual arguments into those primitives before dispatch.
Core no longer truncates `1.9` to row ID `1`, interprets `"false"` as true, or
stringifies a missing required name. Extensions declaring payload types must use
JSON names (`string`, `integer`, `number`, `boolean`, `object`, `array`, `null`) or
`|` unions. Invalid or duplicate declarations fail before replacing a registration.

The command and query named `invoke` remain compatibility/diagnostic escape
hatches and are marked `transport_stable=false`. New interface code must use a
stable named operation. The flag controls serialization only;
`remote_capabilities` separately declares exposure. HTTP defaults to declared
reads, and generic invocation needs an explicitly authenticated trusted policy.
See [transport permissions](transport_permissions.md) for deployment roles and
the restricted client used by read-only surfaces.

## Whole-program capability surface

The tables below summarize the required v2 program boundary.
`api.describe` remains the complete machine-readable list.

| Family | Stable named operations |
| --- | --- |
| Lifecycle | `health`, `api.describe`, `capabilities.list`, `shutdown` |
| Jobs | `jobs.list`, `jobs.get`, `jobs.wait`, `jobs.result`, `jobs.log.read`, durable `jobs.events`, `jobs.cancel`, `jobs.retry` |
| Database | `database.info`, `database.summary`, `database.telemetry`, `database.backup`, `database.vacuum` |
| Schema and identity | `schema.tables`, `schema.table`, `schema.column`, `schema.link`, `schema.column.update`, `schema.identities.list`, `schema.identity.get`, `schema.identity.derive`, `schema.identity.resolve`, `schema.identities.audit`, `schema.identities.migrate` |
| Trees | `tree.root`, `tree.children`, `tree.lineage`, `tree.walk`, `tree.search`, `tree.nest`, `tree.delete` |
| Preferences | `preferences.list`, `preferences.get`, `preferences.set`, `preferences.delete` |
| Native custom fields | `custom-fields.native.*`; native definitions, values, queries and reviewed migrations |
| Calibre custom-field signatures | `custom-fields.list`, `custom-fields.create`, `custom-fields.update`, `custom-fields.delete`; explicit compatibility facade |
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

Administrative row and relation mutations remain available under `admin.*`.
Known bibliographic tables now route through Catalog validation; other tables
retain their own lower-level contracts. This does not implement admin authorization.
Cache/read-source lifecycle operations remain available under `cache.*` and
`read-source.*`. They support administration and compatibility; ordinary
interfaces should prefer semantic operations.

### Storage graph

Managed storage uses a closed, schema-backed resource registry rather than an
arbitrary-table API. It covers digital assets, composite assets, replicas,
item links, composite members, replication policies, backup policies, workflow
records, workflow state, workflow output, and workflow presence.

Writable resources retain create/update/delete adapters. Asset and Replica
resources are inspectable only; use storage-owned ingest, metadata, verification
and deletion operations for mutations. Explicit Asset policy assignment uses the
manager and returns its committed revision. Replication/backup policy-definition
CRUD also delegates to storage, preserving domain defaults, revision checks and
assigned-policy deletion guards. Composite CRUD also uses storage's complete
declaration, revision and reference-guard operations; creation requires members.
Composite member rows are inspectable only, and membership changes replace the
Composite's `members` array. See
[record mutation ownership](../storage_and_backup/record-mutations.md) for caller
migration and partial-effect semantics. Workflow-owned state/output/presence
resources are also read-only. Policy operations assess
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
results, and read logs. Those workflows retain the utility job manager. The
selected `storage.audit.start` route now uses a library-bound durable ledger,
independently owned worker services and namespaced job IDs; its status, partial
receipts and events remain available after reopening. See the
[durable audit contract](../jobs/core-storage-audit.md) for composition, delivery
identity, cancellation and explicit retry. Other audit/reconcile/
evacuation/backup entry points can still execute synchronously. Built-in
`jobs.wait` and `jobs.result` wait outside the shared database handler lock;
remote timeouts do not cancel server work. See the
[job waiting contract](../jobs/core-wait-dispatch.md).

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

The native WEMI workflow (`metadata.search`, `metadata.read_many`,
`metadata.read_for_edit`, `metadata.update`) uses `CoreServices.metadata`, bound
to Catalog and the optional native cache. Its service owns complete reads,
revision checks and reconciliation; Core does not refresh its receipts again.
An injected legacy read source still controls row/Item projection routes and
does not replace this native service. See the
[integration review](integration-review-2026-11-23.md) for the route map.

Structured cache queries do not silently switch to the Database. A table or
query shape unsupported by the selected cache raises `read_query_unavailable`
in both direct and RPC modes, with `table` and `reason` details. Unknown fields
and unexpected failures retain the normal error path. Presentation code may
show an explicit unavailable marker for optional counts, but must not turn a
failed required read into an empty result. Count-only database queries
(`rows.query` with `limit=0`) filter and count without ordering or projecting
row identities. See [read-model-failures.md](../surfaces/read-model-failures.md).

The legacy `search.global` scan also propagates required table-read errors. It
does not skip failed tables and report a successful partial count. New WEMI
consumers should use `metadata.search` for its typed request and snapshot contract.

Semantic catalog writes go through Catalog. Normalized field commands
(`catalog.field.write` and `catalog.field.write-one`) select
`CoreServices.field_writer`. With a native generic cache this is the shared
`services.metadata` composition, a `CatalogCache` bound to the configured
Catalog and cache. The generic cache does not expose metadata write methods.
Without a cache the configured Catalog owns field writes. Explicitly injected
external cache/writer services retain their write contract. Core propagates the
selected writer's failure without retrying through another persistence path or
performing a second reconciliation.

Container metadata writes create Genre, Subject and Series nodes using their
native name column. A display-path column such as `genre_full` can still be
searched, but does not substitute for the required node name on creation.
Core tree nesting and Library metadata import reserve a serialized database unit
before hierarchy reads or writes. A caller-owned outer unit must already have
that reservation; it cannot be upgraded midway through a mutation. Hierarchy
names remain nonblank, and sibling positions are zero-based insertion indexes
within the current sibling count. Leaving a wizard position blank appends.

Custom-field/schema mutations identify themselves as schema changes; ordinary
data writes use data-only cache reload.

If a canonical write commits but cache reconciliation fails, Core raises
`cache_reconciliation_failed`. Its details retain the authoritative receipt
and set `canonical_write_committed=true`; clients must not retry such a write
blindly.

Local query and command execution is serialized by default because the composed
database graph is shared by direct and RPC threads. Only explicitly independent
queries opt out; the built-in exceptions are managed job waits/results. Query
registration captures authority, execution policy and wire policy together.
Queued handlers recheck shutdown before accessing services.

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

Caller-supplied resources remain caller-owned by default. The database-backed graph opened
by `create_core(...)` is owned through `LibraryServices` and closes during
shutdown. The default utility job manager is process-shared and is not shut down
by default. Failed constructors must clean up resources not yet registered with
the owner; repeated shutdown does not retry failed closers. A supplied
Cache must use Core's Database; a cache created through `cache_type=...` is
owned by Core.

Dependency identities are checked before cache preparation. Startup failure
closes newly created caches, restores a displaced storage binding, and stops a
new durable job service before unwinding its library graph. Borrowed resources
remain open. Lazy service construction shares a lifetime guard with close;
acquisition through closed CoreServices is rejected. Database locking precedes
the service lock. Switching libraries composes a new graph rather than rebinding
retained readers and semantic services in place.

Shutdown attempts durable-job stop, service cleanup and opted-in manager shutdown
independently, reporting collected failures as an exception group. Within Core
services, storage unbind failure cannot prevent cache close or later cleanup.
An older owner only detaches its own cache binding, preserving a replacement
session's binding. Cache status-reporting failures after a write retain the same
committed receipt and error classification as reload failures.

`CoreRuntime.database`, `.catalog`, `.cache`, and `.read_source` exist for
composition, testing, and compatibility. They are not members of
`CoreClientAPI` and must not become interface dependencies.

The maintained surfaces use the common named API, while compatibility and
bootstrap exceptions remain. Native and Calibre-shaped
web, JSON API, OPDS, acquisition/image delivery, read-write web, terminal, Tk,
SquashFS CLI, and maintained surface runner scripts all receive one
`CoreClientAPI`. Each accepts either local composition through `--database` or
an existing daemon through `--core-endpoint` where it has a command-line entry
point. The same interface model and wire records are used in both modes.

`surfaces.core.SurfaceCoreSession` is the normal surface-session owner. It owns
locally created runtimes, borrows remote clients, and adapts supplied legacy
Database objects for embedding/testing. CLI one-shot operations also compose
`LibraryServices` explicitly. Synchronous local-disk/rclone/wget/native-HTML
path operations live in [`core.store_registration`](../../src/LiuXin_alpha/core/store_registration.py),
with their [import and CLI migration](../architectural_reviews/lifecycle-opening-path-review-2026-10-09.md#registration-caller-migration)
documented separately. They are Python composition helpers, not remote endpoints.
New surface behaviour should depend on the client contract; existing adapters
are not proof that all privileged Python objects are inaccessible.

Production surface modules are checked by the
[AST boundary test](../../tests/surfaces/test_core_boundary_enforcement.py), which rejects
owned-subsystem imports and generic `invoke` calls. The narrow exceptions are:

- the PostgreSQL CLI, which provisions or diagnoses the database service before
  an application Core can exist;
- terminal `new_store` and read-write web Store-creation views, which enumerate
  backend choices before submitting Core operations;
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
