# Storage API architecture

This document describes the replacement storage API in
`LiuXin_alpha.storage.api`. The old `06 - Storage.md` model is deprecated and
is retained only as salvage material.

## The three boundaries

```text
StorageManagerAPI
  policy, assets, replicas, routing, topology, reconciliation
                         │ Location
                         ▼
StoreAPI / DriverBackedStoreAPI
  one configured LiuXin Store, Store UUID, read-only configuration
                         │ private translation
                         ▼
StorageDriverAPI + optional protocols
  one raw endpoint, bytes, object addresses, native metadata
```

Code outside storage normally uses `StorageManagerAPI`. A `StoreAPI` is one
configured LiuXin Store identified by a stable Store UUID. Its database row is
an implementation detail. A `StorageDriverAPI` is lower level and deliberately
Store-neutral: it can
also power an import source, a temporary workspace, or another byte-oriented
facility without inventing a Store row.

The raw driver must not know about digital assets, bibliographic records,
Replicas and their states, desired copy counts, placement or deduplication
policy, repair jobs, pack planning, or database transactions spanning Stores.

## The reusable driver core

`StorageDriverAPI[DriverObjectAddressT]` composes three small facades:

- `StorageDriverObjectAddressAPI`: checked addresses, persisted-value parsing,
  explicit external URI conversion, and a credential-free endpoint URI;
- `ReadableStorageDriverAPI`: `capabilities`, `stat`, and `open_read`, plus safe
  read-only conveniences; and
- `StorageDriverLifecycleAPI`: `startup`, `probe`, `status`, and `close`.

Only these operations are mandatory. A readable HTTP object, immutable archive,
or single-object import source therefore need not implement fake mutation,
enumeration, allocation, or path methods.

`open_read` returns a context-managed binary stream. It need not be seekable,
and closing it must release all backend resources. Negative ranges are invalid.
A non-default `offset` or `length` is either honoured or rejected with
`StorageUnsupportedOperation`; a driver must not silently return the full
object. A limited stream returns at most `length` bytes.

Raw `DriverObjectInfo.size` is optional. `None` means the endpoint cannot report
an authoritative length before reading—for example, a chunked HTTP response or
generated object. Driver utilities stream such objects without inventing an
expected size and count the bytes they actually receive. The configured Store
boundary remains stricter: `FileInfo.size` is mandatory, so
`DriverBackedStoreAPI` rejects an unsized result rather than admitting an
incomplete Replica description.

All driver-produced addresses are scoped: `address_space_uuid` is mandatory
and identifies the configured Store, import source, or temporary workspace.
`stat(address)`, write-session commit, and native copy/move results must report
exactly the checked address requested by the caller. Reusable utilities and
`DriverBackedStoreAPI` enforce this boundary before trusting returned metadata.
All `modified_at` and status `checked_at` timestamps are timezone-aware.

`DriverObjectHints` carries a suggested filename, media type, and backend-native
string metadata. Both `DriverObjectInfo` and `DriverInventoryEntry` contain the same
hints value, so a known non-enumerable object can expose HTTP-style response
hints just as an inventory entry can.

## Optional driver protocols

Optional mechanics are structural, independently detectable protocols:

| Protocol | Operation | Capability evidence |
| --- | --- | --- |
| `EnumerableStorageDriverAPI` | `iter_inventory` | `enumeration != UNAVAILABLE` |
| `WritableStorageDriverAPI` | `begin_write` | `create` and/or `replace` |
| `DeletableStorageDriverAPI` | `delete` | `delete`; protected deletion additionally requires `conditional_delete` |
| `ObjectAddressAllocatorStorageDriverAPI` | `allocate_object_address` | `object_address_allocation` |
| `HierarchicalStorageDriverAPI` | `join_object_address` | `hierarchical_object_addresses` |
| `NativeCopyStorageDriverAPI` | `native_copy` | `native_copy` |
| `NativeMoveStorageDriverAPI` | `native_move` | `native_move` |
| `NativeDigestStorageDriverAPI` | `native_compute_digest` | `native_digest` |

Callers check both the capability flag and the protocol. A contradictory driver
fails explicitly rather than falling through to a missing method. Native copy
and move apply only within one driver instance. Cross-Store topology decisions
remain `Location`-based manager decisions.

### Inventory

`iter_inventory()` returns objects, not virtual directories. Its
`EnumerationCompleteness` is `COMPLETE`, `PARTIAL`, or `UNAVAILABLE`; listing
failure must never masquerade as a complete empty result.
Addresses are checked and unique within one iteration. Enumeration is not
assumed to be a point-in-time snapshot unless the concrete driver documents
that stronger guarantee.

A `DriverInventoryEntry` may include metadata already available cheaply from the
backend listing: shared `DriverObjectHints`, size, modified time, digest, and
version. These are discovery hints, not
bibliographic assertions. This avoids an obligatory `stat()` request for every
remote listing item and gives importers a useful filename without teaching the
driver about import policy.

Prefix filtering is independent of listing itself. A driver sets
`capabilities.prefix_enumeration` only when it can honour a non-`None` prefix.
Otherwise an unfiltered inventory remains valid, while a prefix request raises
`StorageUnsupportedOperation` rather than being silently ignored.

## Transactional writes

`begin_write()` is the write primitive; a generic `open(mode=...)` is not. It
returns a `DriverWriteSessionAPI` with the following contract:

- `CREATE_ONLY` is the safe default; replacement is explicit;
- the final address remains absent or unchanged before `commit()`;
- `commit()` checks the expected size and digest before publication;
- successful commit makes the complete file readable at the final address;
- failed commit does not leave a successful-looking partial object;
- context exit without commit aborts the write; and
- `abort()` and cleanup are idempotent.

A session is single-use: after commit or abort, additional writes and commits
raise `StorageError`, while `abort()` remains safe. Committed metadata must
describe exactly the address passed to `begin_write()`.

Non-empty backend-native write metadata requires `write_metadata`. It must be
preserved in the committed object's hints or rejected with
`StorageUnsupportedOperation`; silently discarding it is invalid.

Atomic visibility is separately declared by `atomic_publish`. A backend that
cannot provide it reports false rather than pretending. Content-addressed or
immutable drivers may expose only `CREATE_ONLY`; identical content at an
existing digest address may be an idempotent success, while different content
is an integrity error.

`put_object()` and `write_object_bytes()` are policy-free utilities built on
the staged-write protocol. They are not additional primitives and live in
`LiuXin_alpha.storage.utils.driver`, outside the contract package.

## Conditional deletion and safe moves

`delete(..., missing_ok=True)` suppresses only genuine absence. Passing an
`if_version` token asks the backend to delete exactly the object version
previously returned by `stat()`. This stronger operation is advertised
separately as `conditional_delete`: a backend may support ordinary deletion
without having a safe compare-and-delete primitive.

If conditional deletion is not supported, an `if_version` request raises
`StorageUnsupportedOperation` (or its Store-facing alias). If it is supported
but the token is stale, the backend raises `StoragePreconditionFailed`. Neither
case is reduced to `False`, `None`, or unconditional deletion.

Generic move fallbacks require both `conditional_delete` and a non-`None`
source version. They check these requirements before publishing the
destination, then perform verified copy followed by conditional source
deletion. If either guarantee is unavailable, callers must use a genuinely
safe native move or model the operation as copy plus a separately coordinated
deletion. This rule applies consistently at raw-driver, Store, and manager
facades.

## Addresses, Locations, and URIs

`DriverObjectAddress` is the opaque value understood by one raw driver. Its
`address_space_uuid` identifies the configured endpoint that interprets it.
That UUID can be a Store UUID, an import-source UUID, or an ephemeral workspace
UUID. The standard `ScopedDriverObjectAddressChecker` checks both the concrete
address subtype and this UUID, preventing values from leaking between two
instances of the same backend.

The generic address subtype prevents an FTP address being passed to a
filesystem driver at type-check time. The injected checker provides the
instance-level protection that generics cannot.

Persisted driver-relative values enter through `parse_object_address()`.
External identifiers enter separately through `object_address_from_uri()`, and
an optional credential-free external representation comes from `object_uri()`.
URI parsing and rendering are separately advertised as
`external_uri_parsing` and `external_uri_rendering`.
`root_uri` and returned object URIs must never contain passwords, access tokens,
or equivalent secrets. This separation prevents an endpoint URL or credentials
from silently becoming a stored key.

At the configured Store boundary, the address becomes a global `Location`:

```text
Location(store_ref=<Store UUID>, key=<opaque persisted driver address>)
```

All public Store and manager operations use `Location`. `DriverBackedStoreAPI`
privately translates it and requires a branded driver address space to equal
the Store UUID. Database row IDs and human-readable Store names are lookup
inputs to `LocationFactory`; they are not fields on `Location`.

## Reuse by importing and other facilities

Raw drivers are suitable for file importing because the core contains no Store
or asset policy. A typical importer can:

1. enumerate `DriverInventoryEntry` values when the source supports inventory;
2. inspect through `open_read` or `read_bytes`;
3. call `storage.utils.driver.materialize_object()` only for a legacy reader
   requiring a local path;
4. transfer bytes with `storage.utils.driver.transfer_between_drivers()`; and
5. let the ingest/manager layer create assets, choose destinations, and record
   Replicas.

`materialize_object()` verifies a known authoritative size and any available
digest, counts unsized streams without treating the missing length as failure,
yields a temporary `Path`, then deletes it on context exit. It uses an explicit
filename first, then inventory hints, then `stat()` hints. The temporary path is
not an object address and must not be persisted.

`transfer_between_drivers()` supports different concrete address types and
address spaces. It uses a same-instance native copy only when advertised;
otherwise it performs read → staged write → available expected-size/digest
verification → commit. An unknown source size is passed as `None` rather than
guessed. Backend-native metadata is not copied implicitly; callers may pass
explicitly translated `destination_metadata`. `move_between_drivers()` adds
conditional deletion of the source after
a successful transfer and refuses its fallback before copying unless the source
advertises `conditional_delete` and supplies a version token.

A discovery crawler remains a distinct abstraction: it discovers candidate
URLs and crawl state, while a storage driver authoritatively addresses and reads
objects from one configured endpoint.

## Sync, async, and concurrency

The driver contract is synchronous. Existing async-native backends and
sync-native callers can use `AsyncNativeSyncFacade`, `SyncNativeAsyncFacade`,
and their stream/iterator adapters in `LiuXin_alpha.utils.sync_async`. This
keeps event-loop and worker-thread bridging out of every driver interface.

`DriverCapabilities.concurrency` is conservative by default. It declares
instance thread safety, overlapping reads, overlapping writes, and an optional
recommended parallel-read count. Importers and transfer schedulers must not
infer concurrency safety merely because a backend happens to be remote.
Concurrent read/write claims require `thread_safe`; a parallel-read
recommendation greater than one requires `concurrent_reads`.

Driver construction configures an endpoint but need not connect it.
`startup()` is idempotent, entering a driver context starts it, and `close()` is
idempotent. `probe()` reports ordinary offline state with
`DriverStatus(available=False)` while configuration, authentication,
permission, and unexpected backend failures remain typed exceptions.

## Everyday file operations

The same four familiar write names are available at every storage layer:

| Layer | Simple result | Address input | Metadata meaning |
| --- | --- | --- | --- |
| `StorageManagerAPI` | `DigitalAssetRecord` | manager-selected Store | library metadata projected to placement hints |
| `StoreAPI` | `FileInfo` | optional Location or opaque key string | library metadata projected to placement hints |
| `StorageDriverAPI` | `DriverObjectInfo` | optional typed address or persisted string | backend-native string metadata |

Each layer provides `store()`, `store_bytes()`, `store_stream()`, and
`store_file()`. The generic `store()` dispatches ordinary bytes-like values,
binary streams, and local paths. Omitting the destination asks that layer's
allocator to choose one; when allocation is unsupported, callers supply the
opaque key or address as a string without constructing a value object:

```python
stored = store.store_bytes(
    payload,
    name="book.epub",
    metadata=item_metadata,
)

imported = driver.store_file(
    "/incoming/book.epub",
    object_address="staging/book.epub",
    metadata={"content-type": "application/epub+zip"},
)
```

Manager operations call Replica placement state `replica_mode`; Store and
driver writes call publication behavior `write_mode`. This avoids giving the
same `mode` name two unrelated meanings:

```python
archive_asset = manager.store_bytes(payload, replica_mode="archive")
replaced = store.store_bytes(
    new_payload,
    location="objects/42",
    write_mode="replace",
)
```

The former `mode=` keyword remains a compatibility alias. Supplying both names
is rejected so caller intent cannot be ambiguous.

These conveniences do not add weaker write semantics. They resolve or
allocate the destination, then delegate to the same staged `put()` or
`begin_write()` path with size, digest, create/replace mode, native metadata,
and placement hints intact. `StoreAPI` never exposes a driver address, and raw
drivers never receive WEMI metadata or manager policy. Exact `Location`,
`DriverObjectAddress`, write-session, and utility-based calls remain available
for uncommon control and cross-endpoint transfers.

Simple retrieval uses `open_file()` for an owned, read-only binary stream and
`read_file()` for in-memory bytes. `get_file()` remains a familiar read-only
alias for `open_file()`. Neither method accepts a write mode: staged writes go
through `store*()` or the exact `begin_write()` session and become visible only
when that session commits. Identifiers remain honest at each boundary:

```python
with manager.open_file(42) as source:            # Digital Asset ID
    payload = source.read()

payload = manager.read_file(sha256_hex)          # registered digest lookup
payload = store.read_file(stored)                 # returned FileInfo
payload = driver.read_file(imported)              # returned DriverObjectInfo
```

Only `StorageManagerAPI` owns a digest-to-Asset index, so only it interprets a
bare hash as a reverse lookup (SHA-256 by default, or an explicit `Digest`). A
Store or driver accepts a hash string only when that string is already its
opaque key/address, as with a content-addressed backend. Generic lower layers
do not enumerate and hash every object or pretend to have an index.

Store and driver results round-trip through the whole everyday object
lifecycle. The same methods also accept their opaque string key/address or
exact typed value:

```python
stored = store.store_bytes(payload, name="book.epub")
current = store.stat_file(stored)
assert store.file_exists(stored)
store.delete_file(stored, if_version=current.version)

imported = driver.store_bytes(
    payload,
    object_address="imports/book.epub",
)
assert driver.file_exists(imported)
driver.delete_file(imported, missing_ok=True)
```

`open_file()` and its `get_file()` alias return an owned, read-only binary
stream and should be used as a context manager. `read_file()` reads the
selected range fully into memory.

## Utilities are not contracts

Free-standing operations live under `LiuXin_alpha.storage.utils`, not
`LiuXin_alpha.storage.api`:

- `storage.utils.store` contains configured-Store conveniences such as
  `try_stat`, `read_bytes`, `put`, `copy`, and `compute_digest`;
- `storage.utils.driver` contains raw-driver streaming, transfer, inventory-view,
  and temporary-materialisation operations; and
- `storage.utils.workflow` contains workflow implementation helpers such as
  archive-path normalization.

The `storage.utils` package lazily exposes these names for convenient imports.
API facade methods may delegate to utilities, but the API modules themselves
contain contracts, models, typed errors, and facade adapters rather than a
collection of free operations. General sync/async adaptation remains in
`LiuXin_alpha.utils.sync_async` because it is useful outside storage as well.

Backup workflows apply the same naming distinctions. A
`BackupWorkflowDeclaration` is durable intent, a `BackupWorkflowCheckpoint` is
the latest resumable or terminal execution state, each
`BackupSourceStagingReport` describes one completed staging attempt, and a
`BackupWorkflowResult` is the terminal operational return. Registering the
sealed artifact produces a `BackupArtifactRegistration`; that value describes
the registration and is not the artifact's bytes.

## Error model

Raw drivers raise one typed `StorageError` family:

- `StorageNotFound` — genuine absence;
- `StorageAlreadyExists` — create-only collision;
- `StorageInvalidAddress` — malformed/wrong-space address, URI, or range;
- `StorageReadOnly` and `StorageNoSpace` — mutation constraints;
- `StoragePreconditionFailed` — stale version/race protection;
- `StorageIntegrityError` — expected bytes did not arrive;
- `StorageUnavailable`, `StorageTimeout`, `StorageAuthenticationFailed`, and
  `StoragePermissionDenied` — operational failures; and
- `StorageUnsupportedOperation` — the backend fundamentally lacks an operation.

`try_stat()` suppresses only `StorageNotFound`. `exists()` is derived from it,
so network, authentication, and permission failures remain visible. Store-facing
`Store*` exception names are aliases of the same hierarchy; the Store adapter
does not erase or flatten driver failures.

Concrete drivers translate backend-native exceptions at their boundary. A
raised failure message identifies the backend, attempted operation,
credential-free target, and a concise reason; the original exception remains
available through Python exception chaining. Targets and backend details must
not reproduce URL user information, query values, authorization material, or
secret-like assignments. Filesystem and SQLite errors additionally classify
common absence, collision, permissions, read-only, capacity, timeout, and
corruption conditions into the corresponding typed errors.

Health probes flatten only ordinary unavailability and timeouts into
`DriverStatus(available=False)`. Invalid configuration, missing configured
containers, authentication failure, permission denial, integrity failure, and
unexpected backend faults remain typed exceptions with the same contextual
message, so operators can distinguish “temporarily offline” from “needs
intervention.”

## Store and manager responsibilities

`DriverBackedStoreAPI` translates driver capabilities/status into
`StoreCapabilities` and `StoreStatus`, applies configured read-only state, and
adapts `DriverWriteSessionAPI` commits into routed `FileInfo` values.
It also translates advertised driver-native copy, move, and digest operations;
it never advertises those accelerators while silently selecting a streaming
fallback.

`stat_digest_authoritative` says that digests already returned by `stat()` are
authoritative for the described version; a driver with this flag false must
leave `DriverObjectInfo.digest` empty. `native_digest` independently says
the backend implements `native_compute_digest()`. These facts must not be
conflated.

`StorageManagerAPI` owns cross-Store routing and policy. `StoreConfiguration`
can declare stable host and device UUIDs, allowing the manager to answer whether
two Locations share a host/device before choosing a transfer path. Missing
topology data yields `UNKNOWN`, not an invented physical distinction.
Manager-wide status enumeration returns `StoreStatusObservation` values so
each dynamic `StoreStatus` remains paired with the configured Store UUID that
was observed. Enumeration includes configured Stores that have no live facade,
reporting an unavailable status for them. `StoreConfigurationNotFound` means a
Store UUID is not configured; `StoreUnavailable` means it is configured but
cannot currently serve requests. Neither is `StoreNotFound`, which is reserved
for missing bytes at a concrete Location.

Store-level default policy IDs are placement-time defaults. When a new Digital
Asset receives its first Replica, the selected Store defaults are captured on
the Asset record. Adding later Replicas does not change its effective policy,
so resolution cannot depend on Replica creation or iteration order.

The executable reference implementation is
`LiuXin_alpha.storage.storage_manager.InMemoryStorageManager`. It implements
the complete manager facade and routes bytes to injected `StoreAPI` instances,
while deliberately retaining asset, Replica, policy, Composite, provenance,
Item-link, ingest-idempotency, and reconciliation state only in memory. It is
therefore suitable for contract tests, prototypes, and review of orchestration
semantics—not as a durable production catalogue. A production implementation
can replace those in-process repositories without changing the public manager
contract. Store construction is injected as a factory; already-created Stores
can be registered explicitly with `attach_store()`.

### Starting a manager

Application code normally uses `StorageManager`, which supplies the standard
Store factory on top of the reference orchestration implementation. The
smallest useful startup constructs a Store directly and lets the manager own
its runtime lifetime:

```python
from pathlib import Path

from LiuXin_alpha.storage.store_manager import StorageManager
from LiuXin_alpha.storage.stores import FilesystemStore

primary = FilesystemStore(Path("/srv/liuxin/primary"), name="primary")

with StorageManager(stores=[primary]) as manager:
    asset = manager.store_bytes(
        b"book bytes",
        original_name="book.epub",
        media_type="application/epub+zip",
    )
    assert manager.read_file(asset.digital_asset_id) == b"book bytes"
```

When Store configuration already exists independently of a live facade, use
the manager's default factory. This is also the construction path used by
database bootstrap:

```python
from uuid import uuid4

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.store_manager import StorageManager

configuration = api.StoreConfiguration(
    store_uuid=uuid4(),
    store_name="primary",
    store_kind="filesystem",
    store_root_uri="file:///srv/liuxin/primary",
)

with StorageManager() as manager:
    manager.create_store(configuration)
    asset = manager.store_file("/incoming/book.epub")
```

For database-backed Store discovery, the database remains responsible for the
durable `stores` rows and the manager reports every loaded, skipped, or failed
configuration:

```python
manager, report = StorageManager.from_database(database)
with manager:
    if not report.ok:
        for issue in report.issues:
            print("Store bootstrap issue:", issue.reason)

    # Re-read the stores table after an administrator changes it.
    report = manager.reload_stores()
```

A database-bound `reload_stores()` treats the current `stores` table as
authoritative. It constructs and starts replacements before swapping them in,
adds newly discovered Stores, and unloads Stores whose rows were removed or
marked offline/retired. If a replacement cannot be constructed or started,
the previous live facade remains in service and the failure is included in the
report. A removed Store configuration that is still referenced by an in-memory
Replica is retained as an unavailable identity until that claim is retired.

Pass `replace_existing=False` for an additive refresh: new and currently
unavailable configurations are loaded, while existing live Stores are not
rebuilt and removed rows are not unloaded.

These manager implementations persist bytes in their Stores but retain the
Asset, Replica, Composite, Item-link, and derivation catalogue in memory. A
process restart therefore needs a durable manager implementation of the
persistence SPI; merely reconstructing the same filesystem Store does not
reconstruct those records.

Runnable forms of the first example and a larger two-Store workflow live in
`examples/storage/storage_manager_manual_roundtrip_example.py` and
`examples/storage/storage_manager_workflows_example.py`.

### Everyday manager convenience surface

`StorageManagerAPI` includes a concrete `StorageConvenienceAPI` mixin for
ordinary application code. It owns no state and creates no second set of
storage semantics: each operation normalizes familiar inputs and delegates to
the explicit manager contract. Manager implementations therefore inherit the
whole surface without implementing more abstract methods.

The main entry point accepts bytes, a binary stream, or a local path:

```python
book = manager.store(
    payload,
    name="Book",
    media_type="application/epub+zip",
    original_name="book.epub",
    metadata=item_metadata,
    item=42,
)

cover = manager.store_file(
    "/incoming/cover.jpg",
    item=42,
    role="cover",
)

with manager.open_asset(book) as source:
    header = source.read(16)

archive_copy = manager.replicate_asset(
    book,
    to=archive_store,
    replica_mode="archive",
)
package = manager.create_composite(
    {"book.epub": book, "cover.jpg": cover},
    name="book package",
)
manager.link(42, package, role="package")

imported_package = manager.store_composite(
    {
        "book.epub": "/incoming/book.epub",
        "images/cover.jpg": cover_bytes,
    },
    name="imported book package",
)
manager.export_composite_to_directory(imported_package, "/exports/book")
with manager.open_composite_zip(imported_package) as package_zip:
    deliver(package_zip)
```

Convenience arguments accept the IDs, records, configurations, and Store
facades that callers naturally already have. `store()`, `store_bytes()`,
`store_stream()`, and `store_file()` return a `DigitalAssetRecord`, which is
the useful result for most application work. Callers needing the operation
UUID, exact Replica record, deduplication flags, or verification report use
the corresponding `ingest_bytes()`, `ingest_stream()`, or `ingest_file()`
method instead. `ingest_file()` also pins the observed local size and supplies
the basename as `original_name` when the caller did not provide one.

The convenience `metadata=` argument is deliberately richer than the Asset's
flat `name`, `media_type`, `original_name`, and `attributes` fields. It accepts
a WEMI metadata container, a plain mapping, or an existing
`WorkStorageHints`, `ExpressionStorageHints`, `ManifestationStorageHints`, or
`ItemStorageHints` value. `derive_storage_hints()` projects metadata containers
on the storage side, so the metadata package does not depend on storage:

```python
book = manager.store_bytes(
    payload,
    metadata={
        "title": "Permutation City",
        "primary_agents": ["Greg Egan"],
        "file_formats": ["EPUB"],
    },
)
```

These values are advisory placement hints, not Digital Asset identity and not
backend-native object metadata. The explicit ingest methods call the argument
`placement_hints=` to make that distinction visible. The operation UUID binds
the supplied hints as part of retry identity.

A Store advertises support with `StoreCapabilities.placement_hints`. Supporting
Stores receive the same hint value in both `allocate_location()` and
`begin_write()`, allowing a backend to choose a human layout and update a rich
index transactionally. Stores without that capability continue to use their
ordinary allocation and write behavior; hints are safely ignored. Placement
hints are not automatically retained on the `DigitalAssetRecord`, because a
later metadata edit must not change content identity or rewrite old Replicas.
Instead, each `ReplicaRecord` retains the placement-hint snapshot requested for
that copy. Replication reuses the source Replica's snapshot by default and
accepts an explicit override when the destination needs different
organization. Retaining a hint even when the first Store ignores it allows a
later rich destination to benefit from the same metadata.

The same rule covers less frequent setup and provenance:

```python
live = manager.define_replication_policy("durable", copies=2)
backup = manager.define_backup_policy(
    "offsite", copies=1, require_tags={"offsite"},
)
archive = manager.add_store(
    "archive",
    "filesystem",
    "file:///srv/archive",
    replication=live,
    backup=backup,
)

known = manager.declare_asset(
    size=4,
    digests={"sha256": "..."},
    name="manifest object",
)
derivation = manager.record_derivation(
    cover,
    {"source": book},
    kind="extract",
)
```

Exact reproduction recipes, detailed Composite membership, uncommon policy
constraints, transactional ingest results, and backend-specific Store settings
remain available through the rich value-based methods. The convenience layer
is intentionally an easy route into that contract, not a replacement domain
model.

### Domain objects and public records are distinct

The manager boundary distinguishes three things that must not be collapsed:

| Concept | Meaning | Public representation |
| --- | --- | --- |
| Digital Asset | One expected byte sequence | Conceptual domain object |
| Digital Asset record | Manager-maintained facts about that sequence | `DigitalAssetRecord` |
| Replica | One concrete stored copy | Conceptual/physical object |
| Replica record | Manager claim about a copy at a Location | `ReplicaRecord` |
| Content | The readable bytes obtained from that copy | `BinaryIO` |

`DigitalAssetRecord` and `ReplicaRecord` are immutable public values. They are
not ORM objects, database rows, live storage handles, or containers for bytes.
The former holds expected size, digests, and descriptive metadata. The latter
links the asset identity to a `Location`, operational mode, placement-hint
snapshot, and latest `ReplicaObservation`. Opening that Location supplies the
actual content.

Creation inputs are distinct values: `DigitalAssetDeclaration`,
`ReplicaDeclaration`, and `CompositeDigitalAssetDeclaration`. A new value is
therefore not modelled as a record whose database ID happens to be `None`.
`DigitalAssetID`, `ReplicaID`, Item and policy identifiers are nominal
`NewType` values so static analysis can reject cross-entity ID mistakes.

The same vocabulary is used throughout this API: a `Reference` points to
another object, an `Observation` is physical evidence, a `Resolution` records a
selected route, an `Assessment` interprets current facts and policy, a `Plan`
describes unapplied work, and a `Report` describes work or inspection already
performed. `Configuration`, `Status`, and `Registration` retain their ordinary
meanings. These suffixes describe public semantics, not persistence technology.

Manager implementations receive persistence through narrow ports:

```text
StorageManagerAPI
  public domain operations
             │
             ▼
StorageUnitOfWorkAPI
  DigitalAssetRepositoryAPI
  ReplicaRepositoryAPI
  CompositeDigitalAssetRepositoryAPI
             │ private translation
             ▼
database rows / ORM / document records
```

Repository adapters translate persistence representations into public records.
Row-shaped protocols and metadata containers do not cross the
manager API. A metadata unit of work covers only durable manager state; it does
not pretend that an external Store write participates in the database
transaction.

### Asset and Replica operations

The public manager uses domain names only for domain operations:

- `declare_digital_asset(declaration)` registers a known byte identity without
  asserting that any copy is present;
- `ingest_stream()` and `ingest_bytes()` identify bytes, publish a copy, and
  return a `DigitalAssetIngestResult` containing a `DigitalAssetRecord` and
  `ReplicaRecord`;
- `replicate_digital_asset()` returns the new `ReplicaRecord`;
- `resolve_digital_asset()` returns a `DigitalAssetResolution`, pairing the
  expected record with the selected readable-copy record;
- `forget_digital_asset()` forgets domain knowledge and does not imply byte
  deletion; and
- `remove_replica()` explicitly coordinates physical and domain-state removal.

`STAGED` describes publication work that is not yet committed for serving. A
STAGED Replica may be selected for verification or reconciliation work, but it
is never returned as readable content or counted as a readable policy copy.

Manager lookup names state when they return records—for example,
`get_digital_asset_record()` and `iter_replica_records()`. Equivalent database
CRUD mechanics stay inside repository adapters.
Manager-level absence also has manager-level errors: `DigitalAssetNotFound`,
`ReplicaNotFound`, `NoReadableReplica`, `CompositeDigitalAssetNotFound`, and
`CompositeDigitalAssetIncomplete` are distinct from `StoreNotFound` at one concrete
Location.

Item associations are symmetrical at the public boundary:
`ItemDigitalAssetLinkAPI` links Item roles to atomic or Composite Assets and
removes those links, while `resolve_item_digital_asset()` reads them.

`verify_digital_asset()` normally stops after finding one healthy Replica.
Supplying `replica_ids=` instead verifies exactly that ordered subset by
default. `stop_after_first_healthy=` selects either policy explicitly;
`all_replicas=` remains as the compatibility spelling for callers using the
older boolean surface.

### Persistence SPI

Repository and transaction contracts live under
`LiuXin_alpha.storage.api.persistence_api`. They are implementation-facing
ports for durable manager adapters, not another application API. The package
contains repositories for Assets, Replicas, Composites, and derivations plus a
unit-of-work factory. The old `storage_manager_api.repositories_api` module
reexports those same protocols for compatibility.

A durable Replica repository must round-trip the complete
`ReplicaDeclaration`/`ReplicaRecord`, including its placement-hint snapshot.
The reference `InMemoryStorageManager` does so in memory. The existing legacy
FRBR storage tables are not presented as the persistence implementation of
this new SPI; an adapter and its schema must preserve the same fields before
it can claim that contract.

### Cross-boundary recovery

Publication and repository commit cannot generally be one atomic transaction.
Ingest therefore accepts an optional operation UUID and returns it in
`DigitalAssetIngestResult`. Implementations use that identity with a staged Replica state
to resume, compensate, or reconcile failure between Store publication and
metadata commit. Retrying the same logical operation should use the same UUID.
That UUID binds the complete request—not only byte identity—including metadata,
Item role, placement preference, Replica mode, and verification intent. Reuse
for a different request fails with `StoragePreconditionFailed`.

Reconciliation is likewise split into `plan_reconciliation()` and
`apply_reconciliation()`. Inventory completeness is carried as
`EnumerationCompleteness`; partial enumeration or unavailable Replica checks
can never produce a conclusive clean result. A plan carries identity and an
optional repository revision so stale application can fail explicitly.

Composite resolution returns `CompositeDigitalAssetMemberResolution` values instead of a
bare tuple of Locations. Logical names, paths, roles, titles, ordering, and the
selected member records therefore survive resolution.

Composite membership remains flat and atomic at this boundary. A
`CompositeDigitalAssetMembership.logical_path` can express useful hierarchy, and callers
may construct a tree-shaped presentation from those paths. Persisted recursive
Composite membership is deferred until a concrete use case justifies graph
cycles, nested policy, and ownership semantics.

Composite availability counts required members. A missing or unreadable
optional member may be omitted from resolution without making the Composite
unreadable.

The convenience surface can ingest a mapping of logical paths to bytes,
streams, or local paths with `store_composite()`. It can materialize resolved
members beneath a local directory with traversal and collision checks, or
return a seekable transient ZIP stream. A ZIP that should itself be retained
is ingested as a new atomic Asset and linked to the Composite with an explicit
derivation; export does not silently create a new managed identity.

### Derivation and exact recreation

A derived result remains an ordinary atomic Digital Asset; there is no
`DerivedDigitalAsset` subtype. Derivation describes provenance between Assets,
while the Item-to-Asset link independently describes a contextual role such as
`cover`, `thumbnail`, or `derived_output`.

`DigitalAssetDerivationRecord` records one result, ordered atomic or Composite
provenance sources, a semantic `DigitalAssetDerivationKind`, and an optional
`ReproductionRecipe`.
One record represents one transformation edge. Multi-stage work is expressed
by making each intermediate output an ordinary managed Asset and using it as
the next edge's input, for example:

```text
HTML Asset -- html-to-epub recipe --> EPUB Asset
EPUB Asset -- epub-to-mobi recipe --> MOBI Asset
```

This retains the intermediate byte identity, permits branching and caching,
and allows each step to make its own reproducibility claim. An optional
`workflow_id` groups edges produced by one pipeline execution without making
that operational grouping the provenance relationship itself. A single recipe
may still contain an internally multi-stage command when its intermediates are
deliberately ephemeral and never become managed Assets.

Its sources are `DigitalAssetDerivationSourceReference` values. An exact recipe
pins `ReproductionRecipeInputReference` and
`ReproductionRecipeArtifactReference` values; those values
identify inputs and tooling but are not the input bytes or executable
artefacts themselves.
For URI-only executor or dependency references, a manager needs an injected
`ReproductionRecipeArtifactResolverAPI` that verifies availability of bytes
matching the pinned digest. A URI string by itself is not evidence that the
artefact remains recoverable.
The recipe deliberately distinguishes three claims:

- `EXACT`: a complete recipe is expected to reproduce the registered bytes;
- `BEST_EFFORT`: the process can be rerun but byte identity is not promised;
- `NOT_REPRODUCIBLE`: the edge records provenance only.

An exact complete recipe pins:

- the ordered atomic input Asset IDs, sizes, digests, roles, and logical paths;
- an executor artefact by name and digest, retrievable through either its own
  managed Digital Asset ID or a URI whose bytes are checked against that digest;
- dependency artefacts by digest and a managed Asset ID or verified URI;
- a versioned argv-style replay command;
- canonical relative input, working-directory, and result paths for an isolated
  recipe workspace;
- canonical JSON parameters and execution-environment description;
- expected output size when known and at least one output digest; and
- optional human instructions, operator, timestamp, and notes.

When a provenance source is a Composite Asset, the recipe still enumerates the
exact atomic member inputs consumed by that run. Later Composite membership
changes therefore cannot silently alter replay. Environment JSON must include
any platform, locale, timezone, random seed, clock, codec, or hardware detail
on which deterministic output depends. It must not contain secret values;
recipes should name credential slots or immutable non-secret artefacts instead.

`record_digital_asset_derivation()` validates referenced identities, rejects
cycles, and checks an exact recipe's expected output size and digests against
the registered result Asset. A `StorageUnitOfWorkAPI` exposes a
`DigitalAssetDerivationRepositoryAPI`, so creation of provenance can commit
with the manager's other metadata. External Store publication remains outside
that database transaction.

The manager exposes the resulting DAG directly. `iter_derivation_ancestors()`
walks from a result toward its transitive inputs;
`iter_derivation_descendants()` walks from an input toward every transitive
result; and `get_derivation_graph()` returns the node inventory and edge
records in either or both directions. Traversal is breadth-first, can be
bounded by depth, can retain only exact recipes, and can be restricted to a
single workflow execution. Alternatives and branches remain present in these
provenance views.

`plan_digital_asset_recreation()` is the deliberately selective operation. It
uses current Replica and pinned-artefact availability, recursively considers
exact derivations for missing inputs and managed tools, and chooses a viable
route requiring the fewest replay steps. Its `DigitalAssetRecreationPlan`
returns derivation records in executable topological order, identifies the
selected root derivation and other viable alternatives, and reports readable
leaf Assets, unavailable Assets, and warnings. Planning is read-only; executing
recipes and publishing their verified output is a separate workflow concern.

The existing `transform_runs`, `transform_run_inputs`,
`transform_run_outputs`, and `digital_asset_derivations` schema is a useful
legacy starting point, but it cannot yet persist this contract losslessly. A
schema migration must preserve canonical environment data, replay commands,
executor/dependency digests and Asset IDs, recipe completeness and
reproducibility claims, ordered inputs with roles and paths, and Composite
provenance sources. Until that migration exists, adapters must reject exact
recipe persistence rather than dropping fields or downgrading the claim
silently.

### Durability of recreatable derivatives

Replication and backup policy copy minima are non-negative. This permits a
reproducible derivative to request zero long-lived live copies and zero backup
copies, but zero is never inferred merely because an Asset has a derivation
edge.

`ReplicationPolicy.loss_action` makes the consequence explicit:

- `REQUIRE_COPY` is the default for originals and other retained Assets;
- `RECREATE` permits loss of stored result bytes only through a complete exact
  recipe whose pinned inputs and executor artefacts remain recoverable; and
- `ACCEPT_LOSS` deliberately permits irrecoverable loss.

`retention_priority` on replication and backup policies lets capacity planning
evict lower-valued surplus copies before originals or irreplaceable Assets.
Zero-copy backup policy cannot be retention locked. Policy assignment and
assessment validate recreation transitively: a chain of disposable Assets
must terminate in retained sources and must not form a cycle. Updating a
persisted policy performs the same validation against every affected Asset and
accepts an optional revision precondition; an unsafe or stale update leaves the
previous policy intact.

`DigitalAssetStorageAssessment` separately reports current readability, backup
state, exact recreatability, recoverability, and irrecoverability.
`DigitalAssetReplicationPlan` may select the exact derivation to execute;
`DigitalAssetReplicationPlan` and `DigitalAssetBackupPlan` may identify surplus
Replicas to remove.
Neither plan performs the transformation or deletion itself.

The current SQL schemas constrain replication and backup minima to at least
one. They require a migration to permit zero and to persist `loss_action` and
`retention_priority`; until then, persistence adapters must reject these newer
policies rather than store misleading partial policy.

`DigitalAssetStorageAssessment` deliberately separates current readability,
replication satisfaction, backup satisfaction, at-risk state, and complete
unavailability. A readable Asset may still violate durability policy.

Replicas, asset identity, desired copy counts, deduplication and failure-domain
policy, sealing/parity, repair/reconciliation, and durable workflow state all
remain above both the Store and raw driver boundaries.
