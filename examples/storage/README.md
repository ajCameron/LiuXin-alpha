# Storage examples

This directory covers the storage stack at three different levels.

## Drivers

`filesystem_driver_example.py` and `sqlite_driver_example.py` show the raw
driver lifecycle and an explicit `begin_write()` → `write()` → `commit()`
transaction. This is the right level for backend diagnostics and import tools
which already own policy and catalog decisions.

`http_remote_read_example.py` demonstrates a scoped, read-only remote driver.
The supplied object key must stay beneath `--base-url`; absolute or traversing
keys are rejected. The example performs HEAD and GET requests, calculates a
SHA-256 digest, can enforce `--expected-sha256`, and writes locally only when
`--output` is supplied.

## Stores and managers

`storage_manager_manual_roundtrip_example.py` is the shortest useful manager
example. `storage_manager_workflows_example.py` expands that into portable
Store configuration, rich placement hints, replication, verification,
ID/digest lookup, and Composite export. Both use a database-backed manager so
their catalogues and ingest operation IDs survive a process restart. Use
`TransientStorageManager` only when deliberately disposable state is useful,
most commonly in focused tests.

## Existing storage

There are two deliberately different assimilation workflows:

- `assimilate_existing_disk_example.py` mounts the source tree read-only and
  **copies** selected files into a manager-owned destination Store. The source
  remains unchanged. Repeated content is deduplicated at the Digital Asset
  layer.
- `library_register_unmanaged_disk_example.py` and
  `reconcile_with_database_path_example.py` catalog the existing paths in a
  LiuXin database as an unmanaged Store. They do not copy or take ownership of
  the source bytes.

`ingest_squashfs_drive_example.py` is the first mess-ingestion workflow. It
walks an existing drive without following symlinks, recognizes SquashFS images
by suffix or magic, and registers each readable image as a separate immutable
Store. The image itself and every regular member become durable Digital Assets
and in-place Replicas; no archive is unpacked or copied. A broken image is
reported without preventing later images from being processed, and rerunning
the command resumes idempotently.

`storage_bootstrap_report_example.py` then shows how persisted Store rows are
loaded and how bootstrap issues are reported.

```bash
python examples/storage/assimilate_existing_disk_example.py \
  --source-root /media/existing-books \
  --destination-root /srv/liuxin/managed \
  --extension epub --extension mobi --workers 4

python examples/storage/ingest_squashfs_drive_example.py \
  --drive-root /media/archive-drives/disk-01 \
  --database /srv/liuxin/catalogue.sqlite
```
