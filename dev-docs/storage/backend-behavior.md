# Storage backend behavior and safety

This guide records behavior that is specific to concrete storage backends. The
shared driver, Store, and manager contracts remain in [Storage API
architecture](storage_api.md). Configuration options described here are durable
`StoreConfiguration` backend options and must round-trip through database reload.

## Choosing a backend

| Backend family | Access model | Publication model | Intended use |
| --- | --- | --- | --- |
| Filesystem, SQLite | Read/write | Per object | General ingest and serving |
| HTTP, FTP, read-only rclone | Read-only | None | Import/discovery source |
| S3, writable rclone | Read/write | Provider-dependent | Remote object storage |
| ISO, ZIP, TAR writable modes | Read/write | Validated whole-container rebuild | Occasional archival snapshots |
| RAR, 7z, SquashFS readers | Read-only | None | Existing immutable archives |
| RAR, SquashFS builders | Build once, then seal | Validated create-only container | New immutable archive artifacts |

Static costs and limits are exposed through `StorageCharacteristics`; dynamic
availability and warnings are exposed through `StoreStatus`. Unknown limits are
reported as unknown, never treated as unlimited.

## Registered backend reference

The canonical kind is the durable `StoreConfiguration.store_kind`; aliases are
accepted only at registry lookup. This table is checked against
`DEFAULT_BACKEND_REGISTRY`, so adding or retiring a selectable backend requires
an accompanying documentation change.

| Canonical kind | Common aliases | Endpoint | Default mode |
| --- | --- | --- | --- |
| `memory` | `ram`, `in_memory` | process memory | read/write |
| `filesystem` | `file`, `on_disk` | directory | read/write |
| `on_disk_existing_managed_drive` | `managed_drive` | directory | read/write |
| `on_disk_existing_unmanaged_drive` | `unmanaged_drive` | directory | read-only |
| `on_disk_flat` | `flat`, `flat_store` | directory | read/write |
| `on_disk_calibre_like` | `calibre_like` | directory | read/write |
| `single_file_sqlite` | `sqlite`, `sqlite_blob`, `sqlite_store` | file | read/write |
| `http_readonly` | `http` | remote | read-only |
| `native_html_readonly` | `native_html` | remote | read-only |
| `wget_html_readonly` | `wget_html` | remote | read-only |
| `ftp_readonly` | `ftp`, `ftps` | remote | read-only |
| `rclone_http_readonly` | `rclone`, `rclone_readonly` | remote | read-only |
| `rclone_writable` | `rclone_readwrite` | remote | read/write |
| `s3` | `s3_compatible` | remote | read/write |
| `encrypted` | `encrypted_store`, `aes_gcm` | wrapping Store | read/write |
| `squashfs_readonly` | `squashfs`, `sealed_squashfs` | file | read-only |
| `squashfs_build` | `squashfs_backup`, `open_squashfs_store` | file | build/seal |
| `iso_readonly` | `iso`, `iso9660`, `joliet`, `rock_ridge`, `udf` | file | read-only |
| `iso_writable` | `iso_readwrite`, `iso_rw`, `iso_build` | file | rebuild |
| `zip_readonly` | `zip` | file | read-only |
| `zip_writable` | `zip_readwrite`, `zip_rw`, `zip_build` | file | rebuild |
| `tar_readonly` | `tar`, `tgz`, `tar_gz` | file | read-only |
| `tar_writable` | `tar_readwrite`, `tar_rw`, `tar_build` | file | rebuild |
| `rar_readonly` | `rar` | file | read-only |
| `rar_build` | `rar_backup`, `rar_seal` | file | build/seal |
| `sevenzip_readonly` | `7z`, `sevenzip` | file | read-only |

## Durable option defaults

Options not listed for a backend are rejected by option dataclasses or concrete
constructors unless that constructor explicitly documents extension keywords.
Runtime dependencies—clients, credentials, Store resolvers, and encryption key
providers—belong in `StoreConstructionContext`, not durable options.

| Backend | Durable option | Default |
| --- | --- | --- |
| `memory` | `max_bytes` | `None` |
| `http_readonly` | `timeout_s` / `max_requests_per_hour` / `max_inventory_entries` | `30.0` / `None` / `100000` |
| `ftp_readonly` | `timeout_s` / `passive` / `secure_data_channel` / `encoding` | `30.0` / `True` / `True` / `"utf-8"` |
| `ftp_readonly` | `spool_limit_bytes` / `max_inventory_depth` / `max_directory_entries` / `max_inventory_entries` | `8388608` / `256` / `100000` / `100000` |
| rclone readers/writers | `rclone_exe` / `rclone_args` / `timeout_s` | `"rclone"` / `()` / `60.0` |
| rclone readers/writers | `max_http_requests_per_hour` / `apply_rclone_tpslimit` / `rclone_tpslimit_burst` / `enforce_global_rate_limit` | `None` / `True` / `1` / `True` |
| rclone readers/writers | `max_inventory_entries` / `max_json_token_chars` | `100000` / `8388608` |
| `rclone_writable` | `local_staging_directory` | `None` |
| `s3` | `region_name` / `endpoint_url` / `profile_name` | `None` / `None` / `None` |
| `s3` | `multipart_threshold` / `multipart_part_size` | `67108864` / `16777216` |
| `s3` | `local_staging_directory` / `max_inventory_pages` / `max_inventory_entries` | `None` / `10000` / `100000` |
| `s3` | `max_inventory_page_entries` / `max_inventory_cursor_chars` | `10000` / `4096` |
| `native_html_readonly` | `timeout_s` / `max_http_requests_per_hour` / `recurse` / `max_depth` / `no_parent` / `span_hosts` | `30.0` / `None` / `True` / `None` / `True` / `False` |
| `native_html_readonly` | `respect_robots` / `user_agent` / `max_html_bytes` / `max_pages` / `max_observed_urls` | `True` / `None` / `2000000` / `10000` / `100000` |
| `wget_html_readonly` | `wget_exe` / `wget_args` / `timeout_s` / `max_http_requests_per_hour` | `"wget"` / `()` / `300.0` / `None` |
| `wget_html_readonly` | `recurse` / `max_depth` / `no_parent` / `span_hosts` / `respect_robots` | `True` / `None` / `True` / `False` / `True` |
| `wget_html_readonly` | `user_agent` / `no_verbose` / `max_observed_urls` / `max_output_chars` | `None` / `True` / `100000` / `8388608` |
| `encrypted` | `inner_store_uuid` | required, or parsed from the `encrypted:` root URI |
| `encrypted` | `chunk_size` / `forward_placement_hints` / `inner_prefix` / `local_staging_directory` | `1048576` / `False` / `""` / `None` |
| writable ISO/ZIP/TAR | `allow_lossy_rebuild` | `False` |

Archive readers and builders additionally accept the bounded parser, expansion,
staging, and subprocess limits described in their concrete constructor
signatures. Those signatures are the authoritative defaults; the safety-focused
sections below summarize the shared ceilings and publication behavior.

## ISO images

`IsoStorageDriver` and `iso_readonly` read ISO 9660 images without mounting or
calling a shell utility. Namespace choice is deterministic: Rock Ridge, then an
available UDF namespace on a bridge image, then the highest Joliet volume, then
the primary ISO 9660 volume. Optional `pycdlib` enables UDF; without it a hybrid
image remains readable through ISO/Joliet. UDF-only and zisofs-compressed images
are explicitly unsupported.

Inventory and reads are bounded. The reader handles SUSP continuation records,
multi-extent files, conditional range reads, and surrogate-escaped Rock Ridge
byte names. Symbolic links, non-regular entries, unsafe names, and ambiguous
topology reject the selected namespace instead of being followed or omitted.
UDF members are staged in bounded private temporary files.

`WritableIsoStorageDriver` and `iso_writable` add create, replace, upsert,
conditional delete, and allocation. Every mutation streams retained members and
the new payload into a sibling image, validates it independently, then publishes
it with atomic filesystem replacement. Failure leaves the original image
unchanged; no extracted mirror or in-memory payload cache is retained.

Writers publish a conservative ISO 9660/Rock Ridge image and add Joliet when all
names fit. Before rewriting existing media they audit links, unpreserved SUSP
fields, boot/partition descriptors, unknown supplementary descriptors, and UDF
markers. Potentially lossy rebuilding is blocked unless the durable
`allow_lossy_rebuild=True` option is explicit; warnings remain visible after
opt-in. Member count/size, total logical size, path bytes, and the ISO 4 GiB
individual-member limit are enforced. Whole-image rebuilding makes writable ISO
an archival target, not a high-volume ingest Store.

## ZIP and TAR

`zip_readonly` and `tar_readonly` expose complete regular-file inventory, exact
full/ranged reads, and conditional versions identifying the containing archive.
Writable variants rebuild, validate, and atomically replace a complete sibling
archive. They never extract to a caller-selected tree or keep a payload cache.

Member names are opaque relative POSIX keys. Absolute paths, dot/empty/parent
components, backslashes, NULs, excessive depth, duplicate keys, topology
conflicts, links, devices, and other special entries fail closed. Unicode is not
normalized, so NFC and NFD names remain distinct. TAR uses UTF-8 with surrogate
escapes where the format can preserve legacy POSIX byte names.

ZIP preflights central-directory records before `zipfile` builds inventory. It
requires declared/actual count agreement, matching local and central names, and
non-overlapping local headers. Durable policy separately bounds entry count,
central-directory bytes, per-member and total expansion, path depth, and
compression ratio. Default ceilings are 128 MiB of central-directory data,
4 GiB per member, 64 GiB total expansion, and 200:1 per-member ratio. Encrypted,
unknown-method, special, and multi-disk ZIPs are unsupported. Writers support
stored, Deflate, BZIP2, and LZMA methods.

TAR auto-detects uncompressed, gzip, bzip2, and xz input; writable Stores publish
PAX TAR in the configured compression. Its parser bounds decompressed input,
individual PAX/GNU records, aggregate metadata, all entries, member/total bytes,
ratio, and depth before `tarfile` can allocate unbounded structures. Compressed
ranged reads may decompress from an earlier stream position.

Container comments and metadata that the regular-file Store model cannot retain
make mutation fail closed. `allow_lossy_rebuild=True` permits only a known-safe
normalizing conversion; unsafe entries and ambiguous topology always reject the
archive. Writable ZIP/TAR are classified as archival-snapshot Stores.

## RAR and 7z

`rar_readonly` indexes RAR 3/4/5 in process. A vendored parser supplies the
dependency-free RAR 3/4 fallback; maintained optional `rarfile` support is
required for RAR 5. Stored members read directly. Compressed members require a
configured or discoverable `unrar`/`rar` executable: execution time and output
are bounded, bytes are staged privately, and size plus available CRC-32 or
BLAKE2sp evidence is verified before returning a range. Password-protected,
multi-volume, linked, or redirected archives are rejected.

`sevenzip_readonly` uses optional `py7zr`. It preserves opaque Unicode names,
provides complete inventory and conditional ranges, and verifies size and CRC in
a private temporary file. Solid archives are supported with amplification
reported; encrypted and multi-volume inputs remain limitations. RAR and 7z
policies bound parser/header data, all entries, member and total logical bytes,
available compression ratios, path bytes/depth, staging, and execution time. The
default content envelope is 4 GiB per member, 64 GiB total, and 200:1 ratio.

RAR deliberately has no mutable archive Store. `rar_build` accepts ordinary
Store writes into a durable staging directory, then `seal()` creates one
non-solid RAR 4 archive. It requires an operator-installed, appropriately
licensed `rar` creator; LiuXin neither downloads nor bundles it. Sealing tests the
candidate, compares its key/size/CRC manifest through LiuXin's reader, and
publishes create-only. Failure retains staging for diagnosis; success permanently
locks the builder and returns a read-only facade.

## SquashFS

The reader bounds `unsquashfs` listing output, entries, topology, path sizes,
member/total logical bytes, and archive expansion ratio. Each selected member is
extracted by a timed subprocess into size-bounded private staging, with bounded
diagnostics and archive-identity checks before and after.

`squashfs_build` preflights durable staging without following links, bounds the
creator, and independently inventories and hashes every candidate member before
create-only publication. A successful builder remains sealed and immutable.

## Recursive containers

The limits above apply to one container. Recursive ingest additionally owns a
run-wide budget for nesting depth, members, expanded bytes, wall time, temporary
space, and ancestry/cycles. It must not multiply each container's allowance at
every nesting level. See [mixed-ingest operations](mixed_ingest_operations.md).

## Unicode path conformance

Concrete Store tests share `tests.storage.contracts.unicode_paths`. The contract
requires opaque key identity through locate, inventory, stat, full/ranged reads,
and—where advertised—external URI round trips. It covers distinct NFC/NFD names,
case-sensitive scripts, bidi and format controls, astral characters, emoji,
variation selectors, noncharacters, private-use characters, significant spaces,
URL punctuation, and combining-mark storms.

Backends that can contain non-Unicode POSIX byte names also exercise a
surrogateescape case; URL backends retain opaque percent-encoded octets. A
backend may reject an unpaired surrogate supplied through a Unicode API, but it
must do so with a typed invalid-address error before backend I/O.

## Optional dependencies

The `archives` project extra supplies `py7zr`, `pycdlib`, and maintained
`rarfile` support. Importing the storage package itself requires none of these.
External creator/extractor tools remain explicit operator dependencies and are
never downloaded automatically.
