
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




