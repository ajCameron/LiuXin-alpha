# Storage: current model and entry points

Reviewed: **2026-10-20**, against the working-tree storage contracts and
implementations based on `fc5de7af`.

Status: **Current overview; remaining policy and compatibility boundaries are explicit.**
This replaces the mixed historical overview previously at this path. Use the
[storage index](storage_and_backup/README.md) for task routes and the
[storage API](storage_and_backup/storage_api.md) for detailed calls.

## The identities to keep separate

| Concept | Meaning | Owning boundary |
| --- | --- | --- |
| Item | An owned/observed bibliographic copy. | Catalog; linked to payloads through storage workflows. |
| Digital Asset | One atomic payload's managed identity and metadata. | Storage manager and its persistence adapters. |
| Composite | A logical assembly of ordered atomic members, such as audiobook chapters. | Storage manager; required and optional members remain distinct. |
| Replica | A managed claim about one Asset at one Location, with mode and observations. | Storage manager. |
| Store | One configured endpoint with stable UUID, capabilities and operational state. | Configured Store facade. |
| Location | Immutable Store UUID plus opaque Store-owned key. | Plain value; the Store interprets its key. |

One Asset can have several Replicas. A Composite has member assets rather than
its own directly replicated multipart bytes. Exporting it as a ZIP can produce
a new atomic Asset if a caller explicitly ingests and retains that result.
Item attachment roles such as `primary_payload` and `cover` describe the relation,
not an intrinsic identity of the bytes.

The [schema overview](<03 - Database Schema.md#digital-assets-and-physical-copies>) maps
these concepts to tables. In particular, Composites use
`composite_digital_assets` and ordered member links; the older
`digital_asset_compositions` name is not the current schema.

## Runtime responsibilities

```text
Core / Library workflow
    -> StorageManagerAPI      identity, policy, routing, durable operations
        -> StoreAPI           one configured endpoint, Location-based operations
            -> StorageDriverAPI   raw byte access and backend-native addresses
```

The normal concrete application entry point is
[`storage.durable_manager.StorageManager`](../src/LiuXin_alpha/storage/durable_manager.py).
It binds the shared orchestration core to a database metadata repository,
unit-of-work adapters, Store bootstrap and an ingest journal. With a supported
database, metadata is database-authoritative; an attached Core cache accelerates
eligible reads without becoming a second authority.

The similarly named `storage.storage_manager` package contains repository-neutral
composition, mixins, persistence adapters and `TransientStorageManager`.
`storage.store_manager` is a forwarding compatibility module. New imports should
use the concrete owner, and importing the passive `storage` root does not create
a manager.

`StoreAPI` is the configured facade. `StorageDriverAPI` owns physical primitives
without bibliography, database transactions or cross-Store policy. Drivers can
be reused by acquisition and repair code when only byte access is needed.
Optional driver protocols and `StoreCapabilities` describe operations such as
range reads, inventory, create/replace, conditional deletion and publication.
Do not infer those guarantees merely from a backend name or a writable flag.

[`StoreContainer`](../src/LiuXin_alpha/storage/store_container.py) is a separate
adapter pairing a borrowed live Store with its configuration, optional database
row and cached status. It is not the raw driver and does not replace `StoreAPI`
as the byte-operation boundary.

### Locations and handles

[`Location`](../src/LiuXin_alpha/storage/api/models.py) contains `store_ref` and
`key`; it does not retain a driver, cache a stat result or implement filesystem
operations. The key is opaque outside its Store. A Store UUID is not a database
row ID, display name or backend root URI.

The manager or Store performs reads and writes. A short-lived `BoundLocation`
delegates convenience operations to a live router; persist the Location value,
not the bound handle. A missing object, an unavailable Store and an unknown
Store configuration are distinct failures. An offline endpoint is not evidence
that its files were deleted.

## A bounded first example

This example uses a memory Store and deliberately transient manager metadata;
it demonstrates the API without writing a catalogue or claiming durability:

```python
from LiuXin_alpha.storage.durable_manager import StorageManager
from LiuXin_alpha.storage.stores import MemoryStore

with StorageManager(stores=(MemoryStore(name="scratch"),)) as manager:
    assert manager.metadata_is_durable is False
    asset = manager.store_bytes(
        b"example payload",
        name="example.txt",
        replica_mode="transient",
    )
    assert manager.read_asset(asset, replica_mode="transient") == b"example payload"
```

Application code should normally receive the manager composed for its library.
The class name alone does not establish durability: construction without a
supported storage catalogue retains transient metadata. An incomplete catalogue
that claims storage support is rejected rather than silently treated as memory.

Construction with supplied Stores and bootstrap from database rows are separate
steps. Follow [starting a manager](storage_and_backup/storage_api.md#starting-a-manager)
and [Core lifecycle](core/core-program-workflows.md) for database-backed use.
The manager borrows its database; Core owns the complete graph it creates.
Close owned streams, managers and database sessions explicitly. See the
[runnable examples](../examples/storage/README.md) for larger workflows.

## Ingest, publication and recovery

Convenience calls such as `store_file`, `store_bytes` and `store_composite`
delegate to the same manager contracts as declaration-shaped operations. Storage
owns technical metadata, placement and Replica evidence; Catalog still owns the
Work/Expression/Manifestation identity a Library workflow associates with them.

Backend write sessions separate staging, validation and publication according
to their advertised guarantees. Database commit and physical publication cannot
generally share one atomic transaction. Durable ingest uses a journal and
operation identity to reconcile interrupted work. Reusing an operation UUID
requires the same complete request, not just the same byte digest.

Reconciliation also separates plans from application and carries inventory
completeness. An incomplete enumeration or unavailable check cannot establish a
clean Store. Recovery must check observed bytes and current metadata rather than
blindly replay a stale plan. See
[cross-boundary recovery](storage_and_backup/storage_api.md#cross-boundary-recovery).

## Replication, backup and retention

Replica modes include `active`, `backup`, `archive`, `cache`, `transient` and
`unmanaged`. Store suitability, observations and policy determine which copies
can satisfy a requirement. A transient work file must not silently count as a
durable backup, and one accessible copy is not proof of independent failure domains.

Store policy references act as placement-time defaults: on first Replica
creation, selected defaults are captured on the Asset. Later Replicas do not
continually redefine its policy. Do not infer a runtime folder → Store → global
resolution chain simply because the schema has default-policy columns.

Policies distinguish required retained copies, exact recreation and explicitly
accepted loss. A derivation edge alone does not establish a reproducible recipe.
Current SQLite policy minima permit zero, and the durable repository retains
complete policy values in versioned envelopes alongside projected scalar fields.
Existing databases still need compatible schema/envelopes and explicit migrations.

Policy assessment and planning are not universal deletion guards. In particular,
the current `remove_replica()` path can delete bytes before recording the metadata
change and does not itself enforce a last-copy/retention-policy check. Raw Store
and driver deletion cannot infer library policy either. Hardening destructive
boundaries remains work in the
[annotated architecture review](architectural_reviews/architecture-review-2026-09-26.md).

## Backup artifacts and backend coverage

A backup workflow stages designated sources, publishes an artifact and records
progress/output state. It is distinct from the Store that holds bytes and the
job service that schedules execution. Sealed artifacts can be registered as
read-only Stores, with source coverage recorded through `backup_presence_links`.
Publication, registration and coverage recording have separate failure boundaries.

Use the [backend registry](../src/LiuXin_alpha/storage/utils/backend_registry.py)
and [backend behaviour](storage_and_backup/backend-behavior.md) to select an
implementation. The registry covers local, memory, remote and archive endpoints;
some need external tools, optional libraries or supplied clients. Registration
does not certify availability, capacity, writable support or every recovery path.
The [component status](storage_and_backup/storage_component_status.md) records
dated coverage and the [live runbook](storage_and_backup/live_storage_ci.md)
describes environment-dependent checks.

Remote inventory is also distinct from downloading and verifying content.
The retained [reconciliation helpers](../src/LiuXin_alpha/storage/reconcile/)
and `Library.register_*_store` routes include legacy `files` registration.
Those rows are not interchangeable with a complete modern Asset/Replica graph.
Use [mixed ingest](storage_and_backup/mixed_ingest_operations.md) for acquisition
budgets and resumption, and [terminal composition](surfaces/terminal/terminal-composition.md)
for the surface owner. Keep backend flags and defaults with those owners instead
of duplicating a command reference here.

## Checks and remaining work

Useful starting checks cover [Location values and bound handles](../tests/storage/api2/test_location_api2.py),
[manager composition](../tests/storage/api/test_storage_manager_composition.py),
[durable reload/recovery](../tests/storage/api/test_storage_manager_database_reload.py)
and [policy assignment](../tests/storage/api/test_storage_policy_manager_api.py).
Run the backend contracts relevant to the changed medium as well.

Remaining cross-cutting work includes destructive-operation policy enforcement,
resource and cache-lease coordination, legacy registration migration, and shared
jobs/ingest lifecycle. Broader self-describing archives and recovery rehearsals
remain proposals or bounded workflows with their own status; see
[ideas](ideas.md) and the [recovery design](<storage_and_backup/Archive Recoverability, Redundancy, and Self-Describing Packs.md>).
No single backend test run establishes an archival guarantee for the whole system.
