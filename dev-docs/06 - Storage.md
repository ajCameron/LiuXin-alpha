
As with everything else in LiuXin storage aims to be modular and extensible.
The aim is to be able to handle truly vast amounts of data.
With something resembling archival quality data handling.

# Storage philosophy

Storage is composed of a series of stores, all of which is managed by the StorageManager.
Stores can have different properties, and don't even need to be online.
Cold, or tape, storage is just another form of storage.

Some stores have different properties for different purposes.
e.g. you can have a store which is just dedicated to new_books storage, or a cache store.

# Storage hierarchy

## StorageManager

Top level - actually responsible for the storage in totality.

This is the only thing that you should ever actually touch. At all.
Everything else is internals.

The storage manager can
 - CRUD stores
 - store files
 - retrieve files (either into memory or into a local cache).
 - retrieving folders

Internally, it does a lot of work with hashes and so forth to protect against bit rot and the like.

The StorageManager makes no promises at all about _how_ the files are stored internally.
Internally, the files might, or might not, be in folders (indeed, many stores will not support folders at all).
They might, or might not, be compressed.
You don't get to know. Leave that all to the StorageManager.

Folders in a storage manager are entirely a virtual concept.
There is no guarantee that they actually exist before they are rendered by the StorageManager.
(This rendering might just be a copy - but, again, you don't need to know that).

## Stores

The actual storage objects.
These are handled by the StorageManager alone.

The stores are responsible for
 - storing files
 - retrieving files

### Managed drive layout note

`on_disk_existing_managed_drive` is the "LiuXin takes over this path" plugin.
It makes one important distinction:

- **explicit writes** go exactly where the caller asked, so rename/edit/move style
  workflows stay clean and unsurprising
- **implicit writes** ("store these bytes for me somewhere sensible") go into a
  reserved LiuXin-owned folder under the root using a deterministic hash layout:
  `.liuxin/managed_drive/<first five hash chars>/<full hash>`

This avoids spraying ad-hoc files into the visible root while still keeping the
store human-inspectable on disk. The plugin is allowed to manage the whole tree,
but it keeps its default spill area namespaced and obvious.

Implicit writes also have a strict safety rule: they may dedupe identical bytes,
but they must never silently overwrite an incompatible existing file. Writable
plugins now raise storage-specific implicit-overwrite errors off a shared base
(`StorageImplicitOverwriteError`) when the canonical implicit target is already
occupied by different bytes or by a non-file path.

# Files

When you put a request in to a store (either by hash, or id from the files table) you get a file object back.


# Remote mirroring (read-only HTTP via rclone)

There is now a read-only remote store backend for site/file-tree style mirrors:

- Store kind: `rclone_http_readonly`
- Typical root URI: `remote:` or `:http,url=https://example.com:`
- It can iterate remote files and register them into the `files` table.

The terminal interface route is:

- `sync store <store_id|store_name> to-db [options]`

This works for both local disk stores and rclone HTTP read-only stores.

## Rate limiting

For rclone HTTP stores, default crawl speed is intentionally low/polite:

- `20` HTTP requests per minute (default, i.e. `1200`/hour)
- Preference key: `rclone_http_max_requests_per_hour_default` (section: `Storage`)

You can set this at sync time:

- `sync store 12 to-db --max-http-requests-per-hour 60`

You can also effectively disable backend throttling for a run:

- `sync store 12 to-db --max-http-requests-per-hour 0`

## Useful sync options

- `--extensions epub,mobi,pdf` : only ingest listed extensions
- `--source <label>` : set `files.file_source`
- `--no-refresh` : skip storage manager bootstrap refresh after sync
- `--no-links` : skip `file_store_links` rows
- `--json` : emit JSON report
- `--background` : enqueue sync as a managed job (inspect with `jobs list` / `jobs show <id>`)
- `--job-backend process|serial` : override jobs backend for the background run
- `--job-timeout-s <sec|none>` : background job timeout (`none` disables timeout)
- `--job-output|--job-no-output` : capture or suppress background job logs

Local disk specific:

- `--no-hash` / `--hash`
- `--follow-symlinks` / `--no-follow-symlinks`

Rclone HTTP specific:

- `--capture-hashes` / `--no-capture-hashes`
- `--max-http-requests-per-hour <N>`

## API entry points

If using Python directly:

- `LiuXin_alpha.storage.reconcile.register_rclone_http_readonly_store_files(...)`
- `LiuXin_alpha.storage.reconcile.register_rclone_http_readonly_with_database_path(...)`
- `Library.register_rclone_http_store(...)`

These write/reuse a `stores` row, iterate remote files, and upsert into `files` with deterministic `file_storage_key` values relative to `store_root_uri`.

# Remote mirroring (read-only HTML spider via wget)

There is also a read-only crawler backend for plain HTML directory/index pages:

- Store kind: `wget_html_readonly`
- Typical root URI: `https://example.com/`
- It spiders links with `wget --spider --recursive` and registers discovered files into `files`.

Default crawl speed is also polite:

- `20` HTTP requests per minute (default, i.e. `1200`/hour)
- Preference key: `wget_http_max_requests_per_hour_default` (section: `Storage`)

The same sync command is used:

- `sync store <store_id|store_name> to-db [options]`

Python API entry points:

- `LiuXin_alpha.storage.reconcile.register_wget_html_readonly_store_files(...)`
- `LiuXin_alpha.storage.reconcile.register_wget_html_readonly_with_database_path(...)`
- `Library.register_wget_html_store(...)`

Wget sync tuning flags:

- `--wget-no-recurse`
- `--wget-max-depth <n|none>`
- `--wget-timeout-s <sec|none>` (terminal sync defaults to `none`)
- `--wget-parent` / `--wget-no-parent`
- `--wget-span-hosts` / `--wget-no-span-hosts`
- `--wget-ignore-robots` / `--wget-respect-robots`
- `--wget-user-agent <ua>`
- `--wget-verbose` / `--wget-no-verbose`
- `--wget-arg <arg>` (repeatable)

By default, terminal `sync` runs wget in verbose mode so crawler output is visible.
Use `--wget-no-verbose` to reduce noise.

Checksum capability note:

- Capability is tracked on `stores.store_supports_checksums`.
- `wget_html_readonly` is explicitly marked `0` (no checksum support in spider/list mode).
- `rclone_http_readonly` and local disk ingest paths are marked `1`.






## Current storage graph

Storage now reasons in three layers:

- `items` are the library-facing exemplars
- `digital_assets` are the managed payloads
- `asset_replicas` are the concrete copies on stores

Composite payloads use `digital_asset_compositions`.
Semantic roles such as `primary_payload` and `cover` live on the item<->digital_asset link, not on the asset row itself.

Replication and backup policy are now first-class tables and can be assigned directly to digital assets.


## Replica modes and store suitability

The storage schema now distinguishes between the *kind* of copy a replica is and the *kind* of copies a store can legitimately hold.
This is important because not all stores are suitable for all policy goals.

Examples:

- a fast local SSD may be suitable for `active` and `cache` replicas
- a network mirror may be suitable for `backup`, but poor for `active`
- tape may be suitable for `archive`, but not for `active`

This means a storage policy is not just “how many copies should exist?”.
It is also “how many copies of which mode should exist, and on what kinds of stores?”.

In practice, this means the database needs to know two separate things:

- each `asset_replica` has a replica mode such as `active`, `backup`, `archive`, `cache`, `transient`, or `unmanaged`
- each `store` advertises which replica modes it supports

A replica only counts toward a policy if its mode and its store both make sense for that policy.
A tape copy may satisfy an archive requirement without satisfying a live-read replication requirement.
A transient local work copy should not silently count as durable backup.

## Policy resolution

Storage policy resolution now has three layers:

- explicit policy on the `digital_asset`
- default policy on the enclosing storage location (`folder`, then `store`)
- global fallback policy

This is meant to keep the desired-state model queryable and durable in the database, rather than hiding important decisions in Python alone.

## Atomic assets, composite assets, and replicas

The storage model distinguishes between:

- atomic digital assets — one directly replicated byte-bearing payload
- composite digital assets — one logical multipart payload assembled from ordered members

Replicas belong to atomic assets.
Composite assets are resolved by following composition membership to their members.
This is what lets a multi-file audiobook be treated as one library-facing thing while still allowing each MP3 to be hashed, replicated, verified, and healed independently.

## What storage should count as success

A healthy storage system must be able to answer more than “is there a file somewhere?”.
It should be able to answer:

- is there at least one readable active copy?
- does the asset meet its replication floor?
- does it meet its backup/archive expectations?
- are the existing copies on stores that are actually suitable for their intended mode?
- are composite members complete and ordered?

That is the practical reason the schema now separates items, digital assets, replicas, composition, and policy.
It lets storage reason about physical reality without dragging library meaning down into backend mechanics.


## Current strict structure

Storage now has a deliberately strict three-part runtime shape:

- `StorageManager` orchestrates storage as a whole
- `StoreContainer` represents one configured store
- `StorePlugin` talks to one physical backend only

### StorageManager

The manager owns the collection of configured stores. Its responsibilities are:

- loading store specs from the database
- constructing plugins and wrapping them in containers
- choosing which store/container should satisfy a request
- returning `Location` handles to callers
- coordinating storage-facing workflows that span multiple stores

The manager should **not** contain backend-specific filesystem / HTTP / archive logic.
That belongs in plugins.

### StoreContainer

A store container is one configured store. Singular, not plural. It holds:

- one store spec / identity
- one raw plugin instance
- optional database binding for store-row level operations
- cached startup / probe / health information

A container should not become a second storage manager, and it should not become a raw backend driver.
It is the narrow managed wrapper around one plugin.

### StorePlugin

A store plugin is the reusable raw-backend layer. It should know about:

- one root location / endpoint
- how to resolve and return `Location` objects
- how to stat, iterate, read, write, update, copy, and delete bytes on that backend
- backend-local health checks

A plugin should **not** know about:

- database rows
- item / work / expression / manifestation semantics
- replica policy
- storage-manager orchestration

If code such as ingest or repair wants direct physical-media access, this is the layer it should reuse.

### Location is the file handle

Concrete file access is now standardized on `Location`.
A location is:

- bound to exactly one plugin
- path-like / pathlib-like
- the object returned for concrete file access

We do not want a second near-duplicate “single file” abstraction sitting beside it.

### Dependency direction inside storage

The intended dependency direction is:

- manager -> container -> plugin -> location

Not the other way around.
Plugins should not import the manager.
Containers should not become plugin subclasses.
The public `storage.api` barrel is for external callers; internal storage code should prefer direct sibling imports.


## Location capability advertisement

Read-only plugins must return read-only `Location` subclasses as well as refusing mutation at the plugin layer.
A `LocationCapabilities` dataclass is now part of the storage contract so tests and higher layers can inspect
what a location advertises (`can_open_write`, `can_unlink`, `can_rename`, etc.) instead of guessing from the
backend type.

This is intentionally stricter than a single boolean. A plugin may be readable but not iterable, or readable and
iterable but not appendable, and the location capability surface is where that nuance belongs.
