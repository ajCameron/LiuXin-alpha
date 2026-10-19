# Separation of concerns

Decision date: **2026-10-11**. Implementation checked: **2026-10-19**, against
`15080c6e` on `configuration-ownership`, after PR #126.

Status: **Current ownership policy; remaining migrations are listed below.**

This guide defines where new behaviour belongs. The
[top-level structure](<02 - Top Level Structure.md>) describes the packages and
entry points. Subsystem contracts specify their APIs and guarantees. Existing
compatibility code can cross these boundaries; it does not grant permission for
new code to do so.

## Ownership at a glance

| Concern | Owner | Supporting boundary |
| --- | --- | --- |
| Application composition, configuration and resource lifetime | Core | Surfaces select inputs; `LibraryServices` opens and closes owned collaborators. |
| Workflows spanning bibliographic records and physical files | Library/application workflow | Catalog owns metadata operations; storage owns physical operations. |
| Bibliographic values, normalization and pure validation | Metadata | Catalog invokes those rules before persistence. |
| Record identity, matching, relationships and semantic mutations | Catalog | Databases enforce structural integrity and transactions. |
| Connections, SQL, schema, rows and constraints | Databases | Raw persistence is outside the Catalog semantic-validity guarantee. |
| Bytes, stores, replicas, placement and recovery | Storage | Store plugins perform backend operations; managers coordinate durable state. |
| Derived reads, invalidation and reconciliation | Caches | Writable conveniences delegate to Catalog; the read facade is Catalog-independent. |
| Application category selection and counts | Core browse queries | Surface read models adapt their results for presentation and transport. |
| Discovery, extraction and acquisition | Ingest and its domain coordinators | Candidates still need Catalog acceptance and storage handling. |
| Durable background task records and execution | Jobs | Domain workflows retain their policy; execution helpers are separate. |
| Plugin contracts, loading and application assembly | Plugins | Neutral modules and application hosts retain internal dependency boundaries. |
| Format conversion and standalone file-provider composition | File formats | It consumes neutral plugin contracts, not application assembly. |

These are ownership relationships, not a claim that every operation traverses a
single chain of facades. Core can dispatch a named Catalog operation directly or
invoke a Library workflow when several domains participate. Standalone callers
can use domain APIs with explicit resource ownership.

## Composition, lifecycle and configuration

Core's [LibraryServices](../src/LiuXin_alpha/core/library_services.py) composes
one library's database, storage, cache, Catalog and supported Library facade.
It closes resources it creates, rolls back partial startup, and leaves supplied
collaborators with their caller. Closing resources is distinct from rolling back
a database transaction or undoing a file transfer.

`library.library.Library` coordinates supplied collaborators. The retired
`library.legacy` and `library.backend` modules are not alternative opening paths.
Mixed ingest consumes an injected Library; it does not own application composition.
See [Core workflows](core/core-program-workflows.md).

Core selects the deployment and supplies the application preferences store.
`Preferences` owns the application INI; the compatibility preference/tweak views
resolve that same store. Database-local preferences, plugin component settings
and conversion options retain their own scopes. They are not competing writers
of the application mapping. Path precedence, old-file read inputs and worker
propagation are defined in [configuration ownership](core/configuration-ownership.md).

Lower-layer opening wrappers, maintenance scheduling and older startup/path
helpers remain migration work. A component must not acquire a second application
lifecycle merely because it needs a database or configuration value.

## Semantic write validity

The database is the authoritative record of what was persisted. It is not the
owner of bibliographic meaning: a stored row alone does not establish that an
identifier, language or Agent relationship is valid.

The required flow is:

```text
untrusted input
    -> metadata normalization and pure value validation
    -> Catalog operation and relationship validation
    -> repository storage adaptation
    -> database transaction and constraints
```

These responsibilities are cumulative. Catalog must establish its preconditions
rather than trusting an earlier surface or workflow. Database constraints remain
essential for types, nullability, foreign keys, uniqueness and concurrent writes.
Normalization alone does not establish semantic validity.

The supported public write routes are `catalog.mutations.writer.*`, public
repository CRUD and specialised mutations, and the advanced schema-driven
`catalog.create_writer` / `write` / `write_one` trio. Prefer a semantic operation
when one exists. Cache writer conveniences delegate to those same Catalog owners
and reconcile derived state after persistence succeeds.

The Row-oriented `add` / `ensure` / `apply` / `intralink` APIs remain supported
compatibility and are **deprecated for new code**. For Catalog-owned tables,
`catalog.row_mutations` adapts their requests and receipts to repositories and
`CatalogRelationships`. Their public workflows use the portable transaction
when the handle supplies it. Non-Catalog tables retain their low-level adapters;
that fallback does not extend Catalog's semantic guarantee to every table.

`catalog.db`, direct driver/macro/SQL calls and the private writer-host `_apply_*`
methods are outside the public semantic boundary. Prepared update objects are not
proof that public validation occurred. Use raw persistence only for explicitly
low-level schema, migration, repair or domain-storage work.

The shared preparation and relationship routes are implemented. Complete semantic
validation for every field is not: the
[write-validation contract](catalog/catalog-write-validation-contract.md) identifies
the current rules and their limits. New writes must meet the rules declared for
their route. Reads remain tolerant enough to inspect historical data; remediation
is an explicit audit or migration, never an incidental read-side rewrite.

### Normalization and identity

Pure metadata rules define stored text identity. Current declarations use NFKC,
case folding and whitespace collapse for tags, labels, genres, subjects and series,
while preserving punctuation and complete words. Genre and subject identity also
retains parent scope. Repositories and low-level row adapters derive these values
from the schema's `normalized_identities` declarations; a supplied `*_norm` value
cannot replace the declared derivation.

Matching evidence, approximate search and display sorting have different purposes.
They must not write their own competing values into the stored identity columns.
See [identity policy](catalog/catalog-write-validation-contract.md#current-identity-policy)
and [matching policy](catalog/catalog-matching-policy.md). Existing-key repair
reports collisions and does not silently merge records.

## Metadata values and persistent reads

Metadata owns passive containers, WEMI vocabulary, field meaning, codecs and pure
validators. Identifier schemes and per-entity allow-lists belong to
`metadata.identifiers`; MARC roles and per-WEMI role sets belong to
`metadata.relators`. Schema constraints may consume those rules without becoming
their semantic owner.

Containers do not open a database, hydrate themselves with `from_database`, or
persist through `write_to_database`. Use `catalog.mutations.writer.write_container`
or `write_values` to persist values. Database/cache read-source contracts and
hydrators live under `catalog.api.retrieval.hydration` and
`catalog.retrieval.hydration`; `catalog.retrieval.metadata` is the usual facade.
`metadata.facade` retains persistence-free OPF conversion.

Catalog owns bibliographic repositories, matching, relationships, merges and
persistent projections. It borrows database access; connections, SQL dialects and
transaction mechanics remain in databases. Physical placement and presentation
remain with storage and surfaces respectively.

### Caches and Catalog

Caches own rebuildable rows, relationships, indexes, lifecycle, invalidation and
reconciliation. Losing derived state must not lose authoritative library data.

`caches.read_cache.ReadCache` supplies structured reads without importing Catalog.
Catalog metadata retrieval can receive that read capability or compose it for a
cache-backed hydration request. `caches.cache.Cache` extends it with writes which
call Catalog. This split permits Catalog/cache collaboration without a dependency
cycle. Catalog mutations do not reconcile cache maps or call upward into cache
write callbacks.

Snapshot backends need explicit refresh/invalidation after external changes;
live backends retain their declared read behaviour. Direct Catalog or raw database
writes do not automatically update an independently held snapshot. Use the
[Catalog/cache contract](catalog/catalog-cache-boundary.md) for write receipts,
transaction-aware reconciliation and failure rules.

### Application catalogue reads

Core owns application read-source selection and browse category policy. In
particular, `core.browse_api` selects tags or labels once at catalogue scope for
category totals, entries and linked-work queries. Populated tags take precedence;
empty tags yield to available labels. The two families are never summed or merged.
Surface adapters preserve that decision, including absent sources and read errors.

This consolidation does not make every read one implementation. Generic Work,
search, file/image and acquisition projections retain their adapters. Transport
pages are bounded; Core still materializes and sorts entity listings before
paging, and successive reads do not promise an atomic snapshot. See
[read failures](surfaces/read-model-failures.md) and the [Core API](core/core-api.md).

## Storage and cross-domain workflows

Storage owns digital assets, stores, locations, replicas, integrity, placement,
backup execution, repair and reconciliation with durable storage records. It can
use databases for those records without taking ownership of bibliographic policy.

Its internal split is concrete:

- `StorageManager` chooses among configured stores and coordinates durable state.
- `StoreContainer` holds one configured store and optional database-facing state.
- `StorePlugin` performs physical-media operations through backend capabilities.
- `Location` represents a concrete path/key and its file operations within a store.

Keep backend operations as independent of databases as their contract allows.
The manager handles persistence and orchestration; databases do not implement
backend file transfer. See [storage design](storage_and_backup/design-patterns.md)
and the [storage API](storage_and_backup/storage_api.md).

Metadata containers contain facts and relationships, not placement policy.
Storage-facing tokens, filename stems, format candidates and placement projections
belong in `storage.api`. Storage must not import concrete metadata containers merely
to place files; a Library/application workflow can supply the needed values.

### Replication, backup and deduplication

| Operation | Owner |
| --- | --- |
| Create, verify, heal and track physical replicas | Storage |
| Execute backup, restore and recovery operations supported by a backend | Storage |
| Choose content classes, retention and broader backup policy inputs | Library/application workflow |
| Share byte-identical content or reduce physical duplication | Storage |
| Resolve bibliographic identity, matching and merge invariants | Catalog |
| Coordinate semantic deduplication, files and human conflict resolution | Library/application workflow |

Database configuration can persist policy inputs; it does not own that policy.
These ownership rules do not assert that every proposed backup medium or retention
feature is implemented. Consult the [storage status](storage_and_backup/storage_component_status.md)
for capabilities and the [recovery design](<storage_and_backup/Archive Recoverability, Redundancy, and Self-Describing Packs.md>)
for design intent.

## Ingest, jobs and execution helpers

Ingest discovers, extracts and proposes candidates, provenance and acquisition
results. Candidates become persisted bibliographic facts through Catalog-owned
mutations; physical transfers and reconciliation use storage. Mixed ingest already
uses an injected Library, while coordinators and recovery/reporting boundaries
still span ingest, storage and Core application services.

A cross-domain workflow can change both files and database rows. The current
composition does not provide a distributed transaction or automatically undo all
earlier effects after failure. State explicit recovery, cancellation and retry
behaviour in the workflow contract.

`jobs` is the durable task layer: definitions, runs, progress, heartbeats, events,
workers and scheduling belong there. `utils.jobs` provides lower-level execution
primitives. Core also has managed execution services; their relationship to durable
jobs remains consolidation work. Configuration propagation into workers is
implemented, but does not establish one job lifecycle.

A storage backup workflow remains storage domain logic when a job runs it. A job
must delegate the action instead of reimplementing Catalog validation, byte handling
or workflow policy. See [mixed ingest](storage_and_backup/mixed_ingest_operations.md)
and the [Core operation contract](core/core-api.md).

## Formats and plugins

`plugins` is the single package for shared plugin contracts, loading and application
assembly. Both `customize` and `plugin_system` are retired namespaces, with no
forwarding package. Internal ownership still matters:

- Neutral `base`, `file_types`, `configuration`, `loading` and `zipplugin` modules
  own lifecycle contracts, streams, component settings, loading and ordering.
  They cannot import concrete domain providers or application assembly.
- `file_formats` owns conversion implementations, contracts, profiles and standalone
  file-provider composition. It consumes neutral plugin leaves and cannot import
  the application plugin host or the application/persistence stack.
- `metadata.file_sources` owns metadata reader/writer implementations.
- `plugins.application` owns application startup, management and lookups. Its import
  is passive; explicit startup extends the standalone catalogue and binds the same
  initialized instances into `file_formats.plugin_runtime`.

Shared loading uses descending priority, stable registration order on ties and
installed providers first at equal priority. The composition owners still select
their providers. Separate cache, storage, database and utility registries retain
domain-specific contracts; merging the plugin packages did not unify every registry.
The [plugin composition contract](plugins/plugin-composition.md) defines startup,
refresh, failure semantics and import migration.

## Enforcement and remaining work

Package-root docstrings summarise this policy and link here. Passive package roots
are import boundaries, not service factories; import concrete facades and contracts
from their owning modules.

The [layer-direction ratchet](architectural_reviews/layer-direction-ratchet.md)
records exact backward edges and unresolved dynamic imports across the package.
The protected modern scope rejects cycles, and the
[import-footprint ratchet](architectural_reviews/import-footprint-ratchet.md) checks
fresh-process behaviour. The plugin rules also enforce internal module direction.
These checks constrain dependencies; they do not prove every runtime contract.

The current ownership work has established shared Catalog writes/identity rules,
Core category policy, application configuration ownership, and one plugin package.
Still open:

- semantic validators for uncovered fields and explicit historical-data repair;
- compatibility cache/search/field-metadata ownership and remaining read adapters;
- lifecycle wrappers, database creation/transaction safety and storage-domain policy;
- durable jobs versus Core execution, ingest coordination and domain registries;
- legacy startup/path consumers and unsupported plugin providers.

Use the [Catalog work list](catalog/catalog-roadmap.md), subsystem contracts and
[annotated architecture review](architectural_reviews/architecture-review-2026-09-26.md)
for the specific outstanding decisions. Completed caller migration does not imply
that every compatibility adapter has been removed or that all five ownership areas
are finished.
