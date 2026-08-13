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
configured LiuXin Store and is identified by a database record and stable Store
UUID. A `StorageDriverAPI` is lower level and deliberately Store-neutral: it can
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

Raw `DriverFileInfo.size` is optional. `None` means the endpoint cannot report
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
string metadata. Both `DriverFileInfo` and `DriverObjectEntry` contain the same
hints value, so a known non-enumerable object can expose HTTP-style response
hints just as an inventory entry can.

## Optional driver protocols

Optional mechanics are structural, independently detectable protocols:

| Protocol | Operation | Capability evidence |
| --- | --- | --- |
| `EnumerableStorageDriverAPI` | `iter_object_entries` | `enumeration != UNAVAILABLE` |
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

`iter_object_entries()` returns files, not virtual directories. Its
`EnumerationCompleteness` is `COMPLETE`, `PARTIAL`, or `UNAVAILABLE`; listing
failure must never masquerade as a complete empty result.
Addresses are checked and unique within one iteration. Enumeration is not
assumed to be a point-in-time snapshot unless the concrete driver documents
that stronger guarantee.

A `DriverObjectEntry` may include metadata already available cheaply from the
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
returns a `DriverWriteSession` with the following contract:

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

1. enumerate `DriverObjectEntry` values when the source supports inventory;
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

## Store and manager responsibilities

`DriverBackedStoreAPI` translates driver capabilities/status into
`StoreCapabilities` and `StoreStatus`, applies configured read-only state, and
adapts `DriverWriteSession` commits into routed `FileInfo` values.
It also translates advertised driver-native copy, move, and digest operations;
it never advertises those accelerators while silently selecting a streaming
fallback.

`stat_digest_authoritative` says that digests already returned by `stat()` are
authoritative for the described version; a driver with this flag false must
leave `DriverFileInfo.digest` empty. `native_digest` independently says
the backend implements `native_compute_digest()`. These facts must not be
conflated.

`StorageManagerAPI` owns cross-Store routing and policy. Store specifications
can declare stable host and device UUIDs, allowing the manager to answer whether
two Locations share a host/device before choosing a transfer path. Missing
topology data yields `UNKNOWN`, not an invented physical distinction.

### Domain values are not persistence records

The manager boundary distinguishes three things that must not be collapsed:

| Concept | Meaning | Public representation |
| --- | --- | --- |
| Digital Asset | Identity of one expected byte sequence | `DigitalAsset` |
| Replica | Claim about one concrete copy at a Location | `Replica` |
| Content | The byte stream obtained from that copy | `BinaryIO` |

`DigitalAsset` and `Replica` are immutable domain snapshots. They are not ORM
objects, database rows, live storage handles, or containers for the bytes.
`DigitalAsset` holds expected size and digests plus byte-object metadata.
`Replica` links that identity to a `Location`, operational mode, and latest
physical observation. Opening the Replica's Location through the manager or
Store supplies the actual content stream.

Creation inputs are distinct values: `DigitalAssetSpec`, `ReplicaSpec`, and
`CompositeDigitalAssetSpec`. A new value is therefore not modelled as an entity
whose database ID happens to be `None`. `DigitalAssetID`, `ReplicaID`, Item and
policy identifiers are nominal `NewType` values so static analysis can reject
cross-entity ID mistakes.

Manager implementations receive persistence through narrow ports:

```text
StorageManagerAPI
  public domain operations
             │
             ▼
StorageUnitOfWorkAPI
  DigitalAssetRepositoryAPI
  ReplicaRepositoryAPI
  CompositeAssetRepositoryAPI
             │ private translation
             ▼
database rows / ORM / document records
```

Repository adapters translate persistence representations into domain
snapshots. Row-shaped protocols and metadata containers do not cross the
manager API. A metadata unit of work covers only durable manager state; it does
not pretend that an external Store write participates in the database
transaction.

### Asset and Replica operations

The public manager uses domain names only for domain operations:

- `declare_digital_asset(spec)` registers a known byte identity without
  asserting that any copy is present;
- `ingest_stream()` and `ingest_bytes()` identify bytes, publish a copy, and
  return an `IngestResult` containing a `DigitalAsset` and `Replica`;
- `replicate_digital_asset()` returns the newly created `Replica`;
- `resolve_digital_asset()` returns a `ResolvedAsset`, pairing the expected
  identity with the selected readable copy;
- `forget_digital_asset()` forgets domain knowledge and does not imply byte
  deletion; and
- `remove_replica()` explicitly coordinates physical and domain-state removal.

Public `create_*_record`, `update_*_record`, and `delete_*_metadata` operations
do not exist. Equivalent persistence mechanics stay inside repository adapters.
Manager-level absence also has manager-level errors: `DigitalAssetNotFound`,
`ReplicaNotFound`, `NoReadableReplica`, `CompositeAssetNotFound`, and
`CompositeIncomplete` are distinct from `StoreNotFound` at one concrete
Location.

### Cross-boundary recovery

Publication and repository commit cannot generally be one atomic transaction.
Ingest therefore accepts an optional operation UUID and returns it in
`IngestResult`. Implementations use that identity with a staged Replica state
to resume, compensate, or reconcile failure between Store publication and
metadata commit. Retrying the same logical operation should use the same UUID.

Reconciliation is likewise split into `plan_reconciliation()` and
`apply_reconciliation()`. Inventory completeness is carried as
`EnumerationCompleteness`; partial enumeration or unavailable Replica checks
can never produce a conclusive clean result. A plan carries identity and an
optional repository revision so stale application can fail explicitly.

Composite resolution returns `ResolvedCompositeMember` values instead of a
bare tuple of Locations. Logical names, paths, roles, titles, ordering, and the
selected member Asset/Replica therefore survive resolution.

`DigitalAssetStorageHealth` deliberately separates current readability,
replication satisfaction, backup satisfaction, at-risk state, and complete
unavailability. A readable Asset may still violate durability policy.

Replicas, asset identity, desired copy counts, deduplication and failure-domain
policy, sealing/parity, repair/reconciliation, and durable workflow state all
remain above both the Store and raw driver boundaries.
