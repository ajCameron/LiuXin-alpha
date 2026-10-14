# Separation of concerns

Decision date: **2026-10-11**

Status: **canonical ownership guide; migrations remain in progress**

This document defines where new behavior belongs. The more detailed
[top-level structure](<02 - Top Level Structure.md>) describes the intended
shape, and the dated
[Catalog write-validation contract](catalog-write-validation-contract.md)
defines the semantic write guarantee. Existing compatibility code may still
cross these boundaries; that debt is not precedent for new code.

The executable migration boundary is described in the
[layer-direction ratchet](layer-direction-ratchet.md). Existing backward edges
are exact reviewed debt, not exceptions available to new code.

## Layer map

```text
surfaces -> core -> library/workflows -> catalog -> databases
                         |                 |
                         v                 v
                      storage           metadata
                         |
                         v
                      databases
```

`caches`, `ingest`, and `jobs` support this flow without becoming alternative
owners of persisted metadata policy.

## Package-root signposts

The package docstrings in these top-level roots repeat the concise ownership
contract so it is visible before a developer explores an implementation:

- `src/LiuXin_alpha/core/__init__.py`
- `src/LiuXin_alpha/library/__init__.py`
- `src/LiuXin_alpha/catalog/__init__.py`
- `src/LiuXin_alpha/metadata/__init__.py`
- `src/LiuXin_alpha/databases/__init__.py`
- `src/LiuXin_alpha/storage/__init__.py`
- `src/LiuXin_alpha/caches/__init__.py`
- `src/LiuXin_alpha/surfaces/__init__.py`
- `src/LiuXin_alpha/ingest/__init__.py`
- `src/LiuXin_alpha/jobs/__init__.py`
- `src/LiuXin_alpha/file_formats/__init__.py`
- `src/LiuXin_alpha/plugin_system/__init__.py`

Those docstrings are entry-point summaries, not independent policy sources. If
wording drifts, this guide and the dated write-validation contract take
precedence and the package root should be corrected.

As of **2026-10-12**, package roots are also import boundaries. `metadata`,
`catalog`, and `caches` initializers do not re-export concrete facades or assemble
implementations. Import contracts and implementations from their owning modules:

- `LiuXin_alpha.metadata.facade` for persistence-free OPF conversion;
- `LiuXin_alpha.catalog.retrieval.metadata` for database/cache hydration and
  persistence-aware OPF overlays;
- `LiuXin_alpha.catalog.catalog.Catalog` for the concrete catalog;
- `LiuXin_alpha.caches.api` for contracts, `caches.cache` for the composed facade,
  and `caches.cache_plugins` for bundled backends.

This is enforced by the fresh-process
[import-footprint ratchet](import-footprint-ratchet.md). It complements static
layer checks by observing the modules Python actually executes through package
initializers.

The shortest useful description is:

- `metadata` defines what metadata values mean and how pure values are
  normalized and validated;
- `catalog` guarantees metadata-aware reads and valid public mutations;
- `databases` persist rows and enforce storage integrity;
- `storage` manages bytes, replicas, and physical placement;
- `library` coordinates workflows spanning Catalog and storage;
- `core` owns lifecycle, configuration, process services, and the application
  contract exposed to surfaces;
- `surfaces` translate external input and output; they are never a trusted
  substitute for lower-layer validation.
- `plugin_system` defines passive, format-neutral plugin lifecycle contracts;
  it never discovers providers or assembles registries;
- `file_formats` owns format implementations and reusable conversion contracts;
  `customize` remains the compatibility and application-composition surface.

## Semantic write validity

The database is the authoritative record of what was persisted, but it is not
the owner of bibliographic meaning. A row existing in a table does not by itself
prove that a title, identifier, language, or Agent relationship is semantically
valid.

Every public Catalog mutation must establish validity itself:

```text
untrusted input
    -> metadata normalization and pure value validation
    -> Catalog operation and relationship validation
    -> repository storage adaptation
    -> database transaction and constraints
```

The responsibilities are deliberately cumulative:

- `metadata` supplies reusable, persistence-free value rules;
- `catalog` invokes those rules and validates the complete requested operation;
- `databases` retain types, nullability, foreign keys, uniqueness, stable
  `CHECK` constraints, and transactional guarantees.

Application validation does not replace database constraints, particularly for
concurrent uniqueness and referential integrity. Database constraints do not
replace Catalog validation or become the home of changing bibliographic policy.
Direct driver and raw-macro calls are outside the Catalog semantic-validity
guarantee and should be limited to schema, migration, repair, and explicitly
low-level code.

The supported public write boundary is `catalog.mutations.writer.*`, public
repository CRUD and specialised mutations, plus the advanced schema-driven
`catalog.create_writer`/`write`/`write_one` trio. Library, Core, and application
operations delegate to those routes and cannot be the only validation point.
The Row-oriented `add`/`ensure`/`apply`/`intralink` surface remains supported
compatibility, is deprecated for new code, and is being routed progressively
through canonical mutations. `catalog.db`, direct persistence, and the private
writer-host `_apply_*` methods are outside the public semantic boundary.

New writes are strict. Reads remain sufficiently tolerant to inspect historical
data, and audits or migrations handle remediation explicitly rather than making
old libraries unreadable or silently rewriting them.

## Layer responsibilities

### Metadata

`metadata` owns passive value types, WEMI vocabulary, field meaning, codecs, and
pure normalization and validation. It does not own SQL, transactions, record
identity resolution, storage placement, or application workflow.

Identifier entity types, schemes, and per-entity allow-lists live in
`metadata.identifiers`; MARC relator roles and per-WEMI role sets live in
`metadata.relators`. Schema generation consumes these contracts from metadata.
They are not database types merely because a database `CHECK` constraint may
enforce them.

Metadata containers do not expose `from_database` constructors. Database/cache
read-source contracts and hydrators belong to `catalog.api.retrieval.hydration`
and `catalog.retrieval.hydration`; callers normally use
`catalog.retrieval.metadata` or the free functions in
`LiuXin_alpha.catalog.retrieval.metadata`. Row-shaped relation targets are
consumed structurally, so passive containers do not import the database `Row`
implementation.

Metadata containers do not write themselves. To persist a populated container,
call `catalog.mutations.writer.write_container(...)`; to apply public field
values, call `write_values(...)`. The absence of a container
`write_to_database` method is intentional and prevents a dependency edge from
passive metadata back into persistence.

Catalog retrieval owns every current database/cache metadata hydration facade.

### Catalog

`catalog` owns metadata-aware persistence: repositories, matching, semantic
mutation policy, WEMI relationships, merges, retrieval, and display-neutral
projections. Its public mutations validate rather than trusting surfaces,
declarations, type annotations, or earlier workflow steps.

Catalog does not own connection mechanics, SQL dialects, physical file
placement, user-interface formatting, or process lifecycle.

### Databases

`databases` owns connections, drivers, SQL, schema creation and migration,
transactions, generic rows and links, constraints, and database repair. It may
store declarative profile labels and consume passive value contracts at an
adapter boundary, but it does not define title, author, identifier, language,
matching, or merge policy.

The database remains the final structural guard and authoritative persisted
record. That is different from being the owner of semantic validation.
There is no implicit metadata-validity check on `Database`, its macros, or its
driver wrapper. Code that needs such a guarantee must enter through Catalog.

### Storage

`storage` owns digital assets, physical stores, locations, replicas, byte
integrity, placement, backup execution, repair, and reconciliation of physical
reality with durable storage state. It may use database-backed storage records
without becoming a consumer of the full bibliographic model.

Storage does not decide whether two records represent the same Work,
Expression, Manifestation, Agent, or other bibliographic entity.

### Caches

`caches` own derived, rebuildable acceleration state, structured queries, and
explicit refresh/invalidation behavior. Cache writes delegate to Catalog; a
cache must not become an alternative semantic write path or source of truth.

`caches.read_cache.ReadCache` supplies lifecycle and structured reads without a
Catalog dependency. Catalog retrieval may compose that read-only facade.
`caches.cache.Cache` subclasses it to add Catalog-mediated writes and cache
reconciliation. Keeping these owners distinct prevents read composition and
write delegation from forming a Catalog/cache import cycle.

### Library and workflows

`library` combines Catalog and storage capabilities into ingest, enrichment,
backup planning, repair, and other user-level workflows. It coordinates domains
without reimplementing their rules or bypassing Catalog for ordinary metadata
writes.

### Core

`core` owns application composition, configuration resolution, lifecycle,
background service scheduling, and the stable command/query contract exposed to
surfaces. `LibraryServices` opens and closes one library's database, storage,
cache, Catalog, and library facade. Core does not absorb metadata or storage
policy merely because it composes their owners.

### Surfaces

`surfaces` own argument parsing, transports, presentation, and conversion
between external representations and Core operations. Early validation may
improve error messages, but Catalog and database boundaries still enforce their
own contracts.

### Ingest and jobs

`ingest` extracts and proposes candidates, provenance, and acquisition results;
it does not turn those candidates into trusted database rows. Persisted metadata
goes through Catalog-owned mutations.

`jobs` owns durable task definitions, scheduling, progress, heartbeats, retries,
and execution records. A job invokes the workflow that owns the domain action;
it does not acquire that workflow's validation or storage policy.

## Metadata containers and storage placement hints

Metadata containers own metadata facts and relation structure, not physical
placement policy. Storage-facing projections such as folder tokens, filename
stems, format candidates, existing keys, and lightweight counts belong in
`storage.api`.

Consequently:

- `metadata.api` does not define storage hints;
- metadata containers do not own `storage_hints()` behavior;
- storage may expose `derive_storage_hints(metadata_like)`;
- storage does not import concrete metadata containers merely to place files;
- library may coordinate a workflow involving both owners.

## Storage-specific boundaries

### Recommended Internal Split Inside Storage

To avoid turning storage into a monolith, it should usually be split into two levels:

#### Store drivers / backend plugins

These handle the mechanics of one concrete storage backend, such as:

- local filesystem
- remote HTTP
- rclone-backed remotes
- squashfs archives
- tape-like or write-once media
- other future backends

They should ideally remain as database-agnostic as possible. Their job is to expose capabilities and perform backend-specific file operations.

#### Storage manager / storage service

This is the database-aware layer inside storage. It is responsible for:

- reading storage state and policy from the database
- deciding what storage action is required
- selecting appropriate stores
- invoking concrete store drivers
- updating durable storage state after operations complete or fail

This keeps low-level storage plugins simple while still allowing storage as a whole to participate in durable, stateful workflows.

### Anti-Responsibilities

To keep boundaries clean:

#### Database should not:

- implement physical storage operations
- contain backend-specific file transfer logic
- define bibliographic normalization, validation, matching, or merge policy
- treat a stored validation-profile label as proof that validation occurred

#### Storage should not:

- become the owner of bibliographic identity
- decide semantic merges of library entities
- absorb general library business logic unrelated to physical storage

#### Library should not:

- reimplement backend-specific storage mechanics
- bypass storage for normal file operations
- become the place where low-level byte movement logic accumulates

### Replication, Backup, and Dedupe

These concerns are related, but they do not all belong to exactly the same layer.

#### Replication

Replication is a storage responsibility.

Replication is about ensuring that the required number of physical copies of a file exist across the available stores, and that those copies remain valid and retrievable.

Storage owns:

- creating replicas
- verifying replicas
- healing missing or damaged replicas
- tracking replica placement and status in the database

Library may still influence replication policy at a higher level, for example by deciding that certain classes of content need more copies than others. But the execution and state reconciliation belong to storage.

#### Backup

Backup is primarily a storage responsibility.

Backup is about producing and maintaining recoverable physical copies according to defined retention or placement policy.

Storage owns:

- running backup copy / snapshot / export jobs
- verifying backup readability where possible
- tracking backup-related state in the database
- restoring files or stores from backup media where supported

Library may define policy inputs such as:

- what should be backed up
- how aggressively
- to which class of stores
- with what retention expectations

But the mechanics of backup are storage concerns.

#### Dedupe

Dedupe is a split responsibility and should be treated as two different problems.

##### Physical / content dedupe

Physical dedupe is a storage responsibility.

This includes cases where:

- two files are byte-identical
- one blob can be stored once and referenced multiple times
- hardlink / reflink / content-addressed strategies are possible
- physical space usage should be reduced without changing higher-level library semantics

This is about bytes, storage layout, and retrieval mechanics, so it belongs in storage.

##### Semantic / bibliographic dedupe

Semantic dedupe is split between Catalog and Library.

This includes cases where:

- two imports appear to be the same manifestation
- two records appear to represent the same expression or work
- metadata from multiple sources should be merged
- duplicate-looking library entities need human or policy-driven resolution

Catalog owns identity evidence, matching decisions, merge invariants, and the
metadata mutations. Library owns the wider workflow: deciding when to run
dedupe, coordinating files or jobs, and requesting human resolution when
Catalog reports ambiguity or conflict.

### Ownership Summary

- replication: storage
- backup execution: storage
- backup policy inputs: usually library and/or database configuration
- physical/content dedupe: storage
- semantic/bibliographic matching and merge: catalog
- semantic-dedupe workflow and cross-domain coordination: library

A good shorthand is:

- storage decides how many physical copies exist, where they live, and whether identical bytes can be shared
- Catalog decides whether metadata identities safely match or may be merged
- library decides when and why to run that work across the wider system

## Storage internal split

Storage now has its own strict internal separation of concerns:

- `StorageManager` orchestrates many configured stores
- `StoreContainer` wraps one configured store plus optional DB-facing state
- `StorePlugin` performs raw physical-media operations
- `Location` is the concrete file/location handle

This split matters because backend code is reusable outside the full managed-storage stack, while manager code is intentionally database-aware and orchestration-heavy.

A useful smell test is:

- if the code is deciding *which* store should do something, it belongs in the manager
- if the code is holding config / status for *one* store, it belongs in the container
- if the code is performing bytes-on-media work, it belongs in the plugin
- if the code is acting on one concrete path/key within one store, it belongs on `Location`

## Jobs versus utility execution helpers

The `jobs` top-level module is the durable, application-level background task layer.
It should own:
- job definitions (what should run)
- job runs (one concrete execution attempt)
- handlers for concrete job kinds
- the repository for persistence, progress, heartbeats, and events
- workers and schedulers

`utils.jobs` remains a low-level execution helper. It is useful for isolated process
execution and in-memory submission, but it is not the source of truth for durable
background tasks.

The intended split is:
- `jobs.*` = durable task orchestration
- `utils.jobs.*` = execution primitive that jobs may reuse internally

A storage backup workflow is not itself a job. A backup workflow is domain logic.
A job is how the application asks for that workflow to be run in the background,
tracked, retried, and scheduled.
