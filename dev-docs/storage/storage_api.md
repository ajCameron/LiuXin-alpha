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

### Store characteristics and limitations

`StoreCapabilities` remains the compact answer to whether a Store can perform
an operation. `StorageCharacteristics` separately describes constraints and
costs callers need before choosing where to perform it: publication and
temporary-space granularity, recommended write usage, maximum object and path
sizes, container-normalization behaviour, and stable limitation codes with
operator-facing explanations. Unknown values remain explicitly unknown rather
than being treated as unlimited or inexpensive.

Drivers may implement the optional `StorageDriverCharacteristicsAPI`; the
driver-backed Store bridge exposes that profile through
`StoreCharacteristicsAPI`. `StorageManagerAPI.characteristics(store_ref)` is
the simple universal lookup, with an unknown-safe profile for older Stores.
Manager writes reject declared sizes above a known maximum before consuming the
source stream. Automatic replication and backup placement applies the same
limit and reserves `ARCHIVAL_SNAPSHOT` writers for archive-mode plans.

Every built-in backend now advertises a catalogue-time profile, and every
built-in concrete driver exposes the configured profile used by its Store.
Ordinary filesystem, SQLite, writable rclone, and S3 Stores publish per object;
HTTP, FTP, read-only rclone, SquashFS, and ISO readers advertise no write path;
SquashFS builders advertise mutable staging followed by an explicit seal; and
writable ISO advertises a whole-Store rebuild. Encrypted Stores project the
inner Store's publication model while adding ciphertext staging overhead and
inner-limit warnings. S3-compatible and rclone drivers retain unknown size and
atomicity values where those facts depend on the selected service, paired with
stable limitation codes explaining why no stronger claim is safe.

Dynamic, per-instance evidence remains in `StoreStatus`. Status warnings are
promoted to attributable `store_warning` operational issues, while static
limitations do not by themselves make an otherwise usable Store unhealthy.

### ISO images

`IsoStorageDriver` and the registered `iso_readonly` Store expose ordinary
ISO 9660 images without mounting them or requiring a shell utility. Namespace
selection is explicit and deterministic: standard Rock Ridge is preferred,
then an available UDF namespace on an ISO/UDF bridge image, then the highest
available Joliet supplementary volume, then the primary ISO 9660 volume. UDF
selection uses the optional `pycdlib` dependency; without it, a hybrid image
remains readable through its direct ISO/Joliet namespace. The selected
namespace is reported in Store status and object hints.

Inventory parses bounded directory and SUSP continuation records. Reads stream
directly across recorded extents, including multi-extent files, and support
conditional range reads pinned to the containing image identity. Rock Ridge
byte names use surrogate escapes so an old image with incorrectly encoded
directory entries remains addressable on POSIX rather than being silently
renamed. A symbolic link, other non-regular entry, ambiguous topology, or
unsafe name rejects the selected read-only namespace; it is never followed or
silently omitted from an ingest inventory.

UDF reads stage the selected member in a bounded private temporary file before
returning a full or ranged reader. `enable_udf`, member and total logical-byte
ceilings, the logical/image expansion-ratio ceiling, and the path-byte ceiling
are durable backend options. The current optional parser requires an ISO/UDF bridge,
so UDF-only images remain explicitly unsupported. zisofs-compressed members are
also rejected rather than being returned as compressed bytes.

`WritableIsoStorageDriver` and the registered `iso_writable` Store provide the
same read surface plus create, replace, upsert, conditional delete, and address
allocation. ISO filesystems do not have an in-place transactional mutation
primitive, so each commit streams every retained member and the new payload
into a complete sibling image. LiuXin parses and verifies that candidate before
using an atomic filesystem replacement to publish it. The old image remains
untouched if staging, copying, layout, validation, or publication fails; no
extracted mirror or in-memory payload cache is retained.

Writable images contain a conservative primary ISO 9660 namespace, Rock Ridge
byte names, and—when all names fit the format—a Joliet supplementary namespace.
Opening an existing primary, Joliet, or Rock Ridge image for writing preserves
its regular-file keys and bytes, then publishes this hybrid form on the first
mutation. Before doing so, the parser audits skipped symbolic links and other
non-regular entries, unpreserved SUSP/Rock Ridge fields, boot and partition
descriptors, unrecognised supplementary descriptors, and hybrid-UDF markers.
Detected loss makes the Store dynamically non-writable and blocks every rebuild
by default. `allow_lossy_rebuild=True` is the explicit durable opt-in for a
normalizing conversion; the detected reasons remain visible as Store warnings.

`volume_id`, `include_joliet`, `deterministic`, `allow_lossy_rebuild`, allocation
prefix, and reader safety limits are durable backend options. Current writes
enforce their member ceiling while bytes are staged, preflight the complete
logical-byte total before building, reject individual members of 4 GiB or
larger, and reject Rock Ridge components longer than 255 encoded bytes. The registry and configured Store
advertise these limits, whole-image publication, store-copy staging, container
rewriting, and archival-snapshot usage structurally. Whole-image rebuilding
makes this backend appropriate for occasional archive mutation; high-volume
ingest should target an ordinary writable Store before producing an ISO
snapshot.

### ZIP, TAR, RAR, and 7z archives

ZIP and TAR follow the same container-Store model as ISO. Registered
`zip_readonly` and `tar_readonly` Stores provide complete regular-file
inventory, exact full and ranged reads, and conditional reads whose version
token identifies the containing archive. `zip_writable` and `tar_writable`
add create, replace, upsert, allocation, and conditional deletion by streaming
a complete sibling archive, validating it, and atomically replacing the old
file. They do not keep an extracted directory tree or accumulate member
payloads in memory.

Every archive member name is treated as an opaque relative POSIX key. Absolute
paths, dot components, empty components, backslashes, NULs, over-deep paths,
duplicate keys, and entries beyond configured inventory or size bounds fail
with typed storage errors. Unicode is not normalized, so NFC and NFD names
remain distinct. TAR uses UTF-8 with surrogate escapes, preserving legacy
non-UTF-8 POSIX byte names where Python's TAR format support can represent
them. Only regular files are exposed. A link, device, other special entry,
duplicate record, file/directory collision, or file used as another member's
parent rejects a ZIP or TAR archive rather than producing an ambiguous partial
projection. Nothing is extracted into a caller-selected host path.

ZIP central-directory records are counted with an allocation-free preflight
before `zipfile` constructs its inventory. The declared count must agree with
the records actually present, local and central member names must agree, local
headers may not be shared or overlap, and path validation happens before any
member is opened. The durable ZIP policy independently bounds inventory count,
central-directory bytes, per-member expanded bytes, total expanded bytes, path
depth, and per-member compression ratio. Defaults cap the central directory at
128 MiB, each member at 4 GiB, total declared expansion at 64 GiB, and
expansion ratio at 200:1; operators may lower these for ebook-only ingest or
explicitly raise them for a known large archive. Invalid UTF-8 metadata becomes
a contextual integrity error rather than leaking a codec exception.

ZIP writes support stored, Deflate, BZIP2, and LZMA methods. Encrypted members,
unknown compression methods, symbolic links, special files, and multi-disk ZIP
sets are unsupported. A write session enforces the member limit while staging,
and a rebuild plan is rejected before I/O if its keys collide or its expanded
size exceeds policy. Candidate validation applies the same read-side limits,
create-only publication never replaces an existing archive, and a whole-file
rebuild checks the source archive identity immediately before atomic replace.
TAR reads auto-detect
uncompressed, gzip, bzip2, and xz archives; the writable Store publishes PAX
TAR in the explicitly configured compression. Ranged reads from compressed TAR
may have to decompress from an earlier stream position. Its parser bounds the
decompressed TAR stream and individual metadata records before `tarfile` can
allocate them. The durable policy caps all entries, member bytes, total logical
bytes, compression ratio, depth, aggregate metadata, and a single PAX/GNU
metadata record. Defaults match ZIP's 4 GiB member, 64 GiB total, and 200:1
ratio ceilings, with 128 MiB of aggregate parser metadata.

As with writable ISO, mutation is a normalizing conversion. ZIP comments and
ordinary member metadata, and TAR ownership, permissions, sparse maps,
and unusual PAX metadata cannot all be preserved by the regular-file Store
model. Inspection therefore marks the Store non-writable and every mutation
fails closed when such material is present. Unsafe ZIP entries and ambiguous
topology reject the archive outright; `allow_lossy_rebuild` does not turn them
into conversion candidates. The durable
`allow_lossy_rebuild=True` option is the explicit conversion opt-in; warnings
continue to advertise what will be discarded. These whole-archive writers are
classified for archival snapshots, not high-volume ingest targets.

`rar_readonly` indexes RAR 3/4/5 archives in process and reads stored members
without an external command. The vendored parser remains a dependency-free
RAR 3/4 fallback; RAR 5 requires the maintained optional `rarfile` dependency.
Compressed members require a configured or discoverable `unrar`/`rar`
executable. Its stdout and stderr are bounded, the subprocess is timed out,
stdout is written to a private temporary file, and the
declared size and available CRC-32 or BLAKE2sp digest are verified before any
requested range is returned. Password-protected and multi-volume archives are
rejected explicitly, and links or other redirections reject inventory. The
durable read policy bounds all entries, member and total logical bytes,
per-member and aggregate compression ratio, path bytes/depth, and extraction
time. Defaults are 4 GiB per member, 64 GiB total, and 200:1.

`sevenzip_readonly` exposes a bounded regular-file projection of a 7z archive
through optional `py7zr` support. It preserves opaque Unicode member names,
provides complete inventory and conditional ranges, and verifies each selected
member's declared size and CRC in a private temporary file. Solid archives are
supported but advertise their per-member decompression amplification. Encrypted
and multi-volume archives remain explicit limitations. There is no mutable 7z
Store. Like RAR and SquashFS packs, a completed 7z image can be catalogued as a
sealed archival artifact rather than pretending the container supports cheap
object commits. Before reads, the durable policy bounds parser/header bytes,
all entries, member and total logical bytes, available per-member and aggregate
compression ratios, path bytes, and depth; the default size and ratio ceilings
are the same 4 GiB/64 GiB/200:1 envelope.

RAR intentionally has no mutable whole-archive Store. The optional `rar_build`
backend instead provides a build-once lifecycle: ordinary Store writes collect
committed files in a durable filesystem staging directory, and the explicit
`seal()` transition creates one RAR 4, non-solid archive. Sealing requires an
operator-installed and appropriately licensed `rar` creator. LiuXin neither
downloads nor bundles that proprietary tool. A candidate is built beside the
destination, tested with `rar t`, checked against the staged key/size/CRC
manifest using LiuXin's independent reader, and then published create-only.
The destination is never adopted, overwritten, or modified; successful
publication permanently locks the builder and returns a `rar_readonly` facade.
Failed builds leave the durable staging tree available for diagnosis or retry.
Staging writes are member-bounded, and sealing first rejects links, special
files, topology conflicts, excessive paths, entry counts, member sizes, and
total bytes. Creator output and runtime are bounded. LiuXin then opens the
candidate with the fully configured hostile RAR reader and verifies every
member before create-only publication.

The SquashFS reader follows the same fail-closed boundary. `unsquashfs` listing
output, all entries, topology, paths, member/total logical bytes, and archive
expansion ratio are bounded. Every selected member is extracted through a
timed subprocess into size-bounded private staging, with bounded diagnostics
and archive-identity checks before and after. `squashfs_build` preflights its
durable staging tree without following links, bounds creator execution and
output, then independently inventories and hashes every candidate member before
publication. A successful builder remains sealed and immutable.

These limits apply to one container. Recursive ingest must additionally own a
cumulative budget for nesting depth, members, expanded bytes, wall time,
temporary space, and ancestry/cycles; it must not multiply each container's
allowance at every level.

All archive backends are registered plugins. Their paths, Store identity,
safety limits, compression choices, rebuild/build policy, durable RAR staging,
and optional tools round-trip through `StoreConfiguration`, so normal database
loading and reload recreates the same Store rather than requiring application
wiring. Installing the `archives` project extra supplies `py7zr`, `pycdlib`,
and maintained `rarfile` support; importing the storage package itself does not
require those dependencies.

### Unicode path conformance

Concrete Store tests share `tests.storage.contracts.unicode_paths`. The common
contract requires exact opaque-key identity through locate, inventory, stat,
full reads, ranged reads, and—where advertised—external URI round trips. Its
case set keeps NFC and NFD spellings distinct and exercises case-sensitive
scripts, bidi and format controls, astral characters, emoji sequences,
variation selectors, noncharacters, private-use characters, significant
spacing, URL punctuation, and combining-mark storms.

Every registered backend family, including both ISO modes and every ZIP, TAR,
RAR, and 7z mode, is exercised through this contract. Media that
can contain non-Unicode POSIX byte names also has a supplementary
surrogateescape case; URL backends separately retain opaque percent-encoded
octets. A backend may reject an unpaired surrogate supplied through a Unicode
API, but it must do so with a typed invalid-address error before backend I/O.

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

### Sealed containers as derived artifacts

The physical lifecycle and the catalogue lifecycle are deliberately separate.
SquashFS and RAR build Stores stage members and seal an immutable image. A
read-only archive Store exposes the members of that image. Neither layer
decides what the image means in the Digital Asset graph.

`SealedArtifactWorkflow` supplies that missing workflow boundary. It accepts a
managed `Location` or local artifact path plus a mapping of archive paths to
ordinary Asset IDs or records. A managed output is adopted in place; a local
output is streamed into a selected writable Store. The workflow then records:

- the image bytes as an ordinary atomic `DigitalAssetRecord`;
- an archive-mode Replica for those bytes;
- a `PACKAGE` derivation from the ordered member Assets;
- exact input sizes, digests, and logical archive paths;
- a digest-pinned executor and dependencies;
- canonical build settings, environment, replay command, and output identity;
  and
- a namespaced `backup:<id>` workflow reference when the build came from
  durable backup intent.

The simple surface is:

```python
from LiuXin_alpha.storage import SealedArtifactWorkflow

sealed = SealedArtifactWorkflow(manager)
tool = sealed.pin_local_executor("mksquashfs", version="4.6.1")
registration = sealed.record_artifact(
    output_location,
    {"books/book.epub": source_asset},
    artifact_format="squashfs",
    executor=tool,
    command=("mksquashfs", ".", "artifact.squashfs", "-noappend"),
    parameters={"compression": "zstd"},
    reproducibility="exact",
)
```

Completed `SquashfsBackupWorkflow` results have a specific adapter:

```python
result = backup.run_to_completion()
registration = sealed.record_backup_result(result, executor=tool)
```

Managed backup source Locations automatically retain their Asset and Replica
IDs in `BackupSourceDeclaration`. Store-backup planning does the same whenever
the inventory Location is already a registered Replica. A local or legacy
source without a catalogue identity must be supplied explicitly through
`source_assets`; the workflow refuses to invent provenance.

The SquashFS adapter claims exact reproducibility only when the physical build
used its deterministic settings. RAR output is recorded as best effort because
the supported creator does not currently promise byte-identical output. The
generic recorder supports 7z, ISO/UDF, ZIP, TAR, and other sealed images when a
caller can provide the actual pinned command and settings.

The container image Asset is distinct from the read-only Store configured over
its members. Member bytes in that Store are Replicas/presence of their existing
Assets, not derivations. `BackupArtifactRegistry` remains responsible for
registering that Store and its protected presence links.

### Asset-backed and nested Stores

`StoreBackingReference` now makes the image-Asset/Store-view relationship
durable rather than asking generic code to infer it from matching URIs. It
contains the backing `DigitalAssetID`, an optional preferred `ReplicaID`, and
an optional local materialization Store UUID. The Asset is authoritative; the
preferred Replica is replaceable routing evidence, not Store identity.

`manager.add_backed_store()` is the ordinary construction surface:

```python
outer = manager.add_backed_store(
    "outer pack",
    "zip_readonly",
    outer_asset_id,
    source_replica_id=outer_replica_id,
)
inner = manager.add_backed_store(
    "inner pack",
    "zip_readonly",
    inner_asset_id,
    source_replica_id=inner_replica_id,
    materialization_store_ref=local_cache_store_uuid,
)
```

When no UUID is supplied, the manager derives Store-view identity from the
Asset's preferred content digest and size plus backend kind and view options.
It never derives public Store identity from a database-local Asset ID or from
the preferred physical Replica.

The first form can use a local source Replica in place. The second copies a
container member through normal manager publication into a Store supporting
`ReplicaMode.CACHE`, then supplies that local file to the archive driver. The
copy is a normal durable Replica record and is reused after restart; it is not
an unmanaged temporary file. Store bootstrap orders the source Store,
materialization Store, outer view, and inner view by their declared
dependencies.

No automatic cache eviction currently removes these materializations. Before
such eviction is enabled, live Asset-backed Stores will need explicit cache
pin/lease ownership so an open driver cannot lose its container path.

Backed Stores are deliberately read-only. Mutating a container behind the
catalogue would invalidate the backing Asset's digest and every member claim.
A rebuilt or resealed archive is instead a new Asset produced by the sealed
artifact/derivation workflow. Current file-container support covers the
read-only SquashFS, ISO, ZIP, TAR, RAR, and 7z backend families.

`materialize_digital_asset()` also accepts `source_replica_id=` and ordered
`source_modes=`. This permits exact ARCHIVE or UNMANAGED sources without
misclassifying them as ACTIVE. Its default source search remains ACTIVE-only
for compatibility.

This is the recursive Store foundation, not an unbounded recursive scanner.
The ZIP driver now enforces a complete per-container expansion policy, so each
nested ZIP is bounded when opened. Automatic nested-container discovery still
belongs in the future mixed-format coordinator, which must additionally impose
cumulative cross-container depth, member-count, expanded-byte, compression
ratio, time, temporary-space, and ancestry/cycle limits. Per-ZIP limits are not
a substitute for that global budget.

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

The application implementation is `LiuXin_alpha.storage.store_manager.StorageManager`.
Its manager state is database-authoritative and exposed to the orchestration
core through repository views. When Core owns a LiuXin `Cache`, the same cache
serves eligible storage reads and receives explicit invalidation after storage
writes; uncached helper/workflow tables remain direct repository reads. The
manager does not retain a second private catalogue snapshot.

The repository-neutral core is a composition rather than a monolith. Files in
`storage/storage_manager/mixins` follow the same order and responsibility split
as `StorageManagerAPI`: Store administration and routing first; Asset ingest,
retrieval, and Replica lifecycle next; higher-level links, Composites,
derivations, and policy after that; reconciliation and operational status last.
Private state and support mixins hold only genuinely cross-cutting mechanics.
`storage/storage_manager/manager.py` is the small composition and compatibility
root.

`TransientStorageManager` implements the complete facade for focused contract
tests and deliberately disposable one-shot work. The former
`InMemoryStorageManager` spelling is a compatibility alias. The production
manager does not inherit from it. See `storage_component_status.md` for the
runtime composition and remaining production checklist.

### Starting a manager

Application code normally uses `StorageManager`, which supplies the standard
Store factory on top of the repository-neutral orchestration implementation. The
smallest production-shaped startup opens the database-backed manager and lets
it own each Store's runtime lifetime:

```python
from LiuXin_alpha.databases.database import Database

with Database(
    metadata={"database_path": "/srv/liuxin/catalog.sqlite"},
    create=False,
    storage_startup_on_add=True,
) as database:
    manager = database.storage
    assert manager is not None
    if not tuple(manager.iter_store_configurations()):
        manager.add_filesystem_store("primary", "/srv/liuxin/primary")
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

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.store_manager import StorageManager

configuration = api.StoreConfiguration(
    store_uuid=uuid4(),
    store_name="primary",
    store_kind="filesystem",
    store_root_uri="file:///srv/liuxin/primary",
)

with Database(
    metadata={"database_path": "/srv/liuxin/catalog.sqlite"},
    create=False,
    enable_storage_manager=False,
) as database, StorageManager(db=database) as manager:
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
report. A removed Store configuration that is still referenced by a catalogued
Replica is retained as an unavailable identity until that claim is retired.

Pass `replace_existing=False` for an additive refresh: new and currently
unavailable configurations are loaded, while existing live Stores are not
rebuilt and removed rows are not unloaded.

The application manager persists both Store configuration and manager-owned
domain records. Reconstructing a filesystem Store alone is intentionally not a
catalogue recovery mechanism; reopening the LiuXin database restores those
records and runs pending-ingest recovery.

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

### Existing SquashFS drive ingestion

`SquashfsDriveIngestWorkflow` is the first deliberately narrow mess-ingestion
surface. Given a local directory, it:

1. registers or reuses the directory as a read-only unmanaged filesystem
   Store;
2. walks it without following symlinks and recognizes SquashFS images by a
   common suffix or the `hsqs` on-disk magic;
3. registers every readable image as its own immutable
   `squashfs_readonly` Store backed by the image's Digital Asset;
4. adopts the image file as an `UNMANAGED` Replica and each regular member as
   an `ARCHIVE` Replica; and
5. returns bounded per-drive/per-archive counts and contextual issues while
   optionally publishing progress events.

Adoption calculates SHA-256 identities but does not copy or extract the drive's
bytes. Repeated scans use operation identities derived from the source file
version and archive identity; unchanged work is idempotent across manager
restart. Database-backed callers must load existing Store configurations before
the scan (normally `manager.load_from_database(startup=True)`). A changed image
at a previously claimed path fails closed as a Location identity conflict.

The default metadata is intentionally technical and path-derived: original
filename, guessed media type, ingest origin, and container format. A
`member_metadata_factory` can enrich this without coupling storage to ebook
parsers. This initial workflow creates Digital Assets and Replicas, not
bibliographic Items; it does not recursively interpret archive members as
containers automatically and does not invent provenance for pre-existing
packs. The Asset-backed Store surface above is now the tested mechanism that a
later bounded mixed RAR/ISO/ebook ingestion coordinator will invoke.

Discovery, archive opening, inventory, archive-image adoption, and individual
member failures are distinguishable in the report. The default continues after
a bad image/member. `max_archives` and `max_members_per_archive` make an
operator-selected partial scan explicit through `truncated` and an issue rather
than silently claiming completion.

POSIX surrogate-escaped byte names remain lossless through local file-URI
reconstruction and the authoritative database scratch envelope. Legacy scalar
columns render lone surrogates visibly as backslash escapes so SQLite and
PostgreSQL text bindings do not reject the entire ingest.

### Persistence SPI

Repository and transaction contracts live under
`LiuXin_alpha.storage.api.persistence_api`. They are implementation-facing
ports for durable manager adapters, not another application API. The package
contains repositories for Assets, Replicas, Composites, and derivations plus a
unit-of-work factory. The old `storage_manager_api.repositories_api` module
reexports those same protocols for compatibility.

A durable Replica repository must round-trip the complete
`ReplicaDeclaration`/`ReplicaRecord`, including its placement-hint snapshot.
`DatabaseStorageMetadataRepository` persists the complete domain values in
versioned envelopes while retaining useful scalar columns for ordinary schema
queries. Its repository views may read through Core's cache but always write
to the database first. `DatabaseStorageUnitOfWorkFactory` is the concrete
database implementation of these ports. A context rolls back unless the
caller explicitly calls `commit()`; `rollback()` is also explicit. The
manager's mapping-shaped collections are compatibility adapters backed by
those repository ports and retain no private record dictionary.

Database-backed record IDs are allocated by the database inside the caller's
transaction, and record revisions are globally unique UUID values. One-row
writes invalidate the corresponding ID in the shared cache. The schema-backed
plugin refreshes that row and dependent projections without a table scan;
plugins that do not implement bounded refresh fall back to a whole-table reload
for correctness.

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
`workflow_id` groups edges produced by one legacy transform-run execution.
Other workflow families use the durable namespaced `workflow_reference`
instead—for example, `backup:42`. Keeping those identity spaces distinct
avoids accidentally treating a backup workflow ID as a foreign key to a
transform run. Neither operational grouping becomes the provenance
relationship itself. A single recipe may still contain an internally
multi-stage command when its intermediates are deliberately ephemeral and
never become managed Assets.

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

The database repository persists the complete public derivation record in its
versioned storage envelope while also projecting the legacy parent, child,
kind, workflow, timestamp, and note columns. Canonical environment data,
commands, executor/dependency identities, recipe claims, ordered input paths,
and Composite provenance therefore survive manager restart losslessly. Future
normalized schema work may project more of that envelope for SQL querying, but
is not a prerequisite for safe persistence.

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
