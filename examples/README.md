# LiuXin Alpha Examples

The examples are grouped by the layer they demonstrate:

- [`catalog/`](catalog/) — WEMI repositories, matching, mutations, and writers.
- [`conversion/`](conversion/) — input-to-OEB and OEB-to-output conversions.
- [`library/`](library/) — the application-facing `Library` facade.
- [`metadata/`](metadata/) — online metadata and cover plugins.
- [`storage/`](storage/) — drivers, Stores, managers, ingest, and DB reconciliation.
- [`utilities/`](utilities/) — small reusable library helpers.

All commands below assume the repository root as the working directory. Set up
the repo-local environment first with:

```bash
bash scripts/create_venv.sh
```

Every Python example supports `--help` and can be invoked by path:

```bash
python examples/storage/filesystem_driver_example.py --help
```

## Smoke tours

Run the local, non-network tour across Library, storage, utilities, and
conversion:

```bash
bash examples/quickstart.sh
```

Run all catalog examples against disposable databases:

```bash
bash examples/catalog/catalog_quickstart.sh
```

Set `KEEP_EXAMPLE_WORKDIR=1` when running the main quickstart if you want to
inspect its generated databases and Store roots.

## Storage starting points

The storage examples form a progression rather than requiring callers to begin
with the lowest-level contracts:

1. `storage/storage_manager_manual_roundtrip_example.py` — attach a Store and
   use the ordinary manager surface.
2. `storage/storage_manager_workflows_example.py` — configure two Stores and
   exercise metadata hints, replication, lookup, verification, and Composite
   delivery.
3. `storage/filesystem_driver_example.py` and
   `storage/sqlite_driver_example.py` — use raw drivers and explicitly commit a
   staged write session.
4. `storage/http_remote_read_example.py` — scope a read-only HTTP driver to a
   URL root, stat a remote object, verify it, and optionally save it locally.
5. `storage/assimilate_existing_disk_example.py` — expose an existing directory
   read-only and copy selected objects into manager-owned storage.
6. `storage/library_register_unmanaged_disk_example.py` and
   `storage/reconcile_with_database_path_example.py` — register an existing disk
   with a LiuXin database without taking ownership of its bytes.

For example:

```bash
python examples/storage/storage_manager_manual_roundtrip_example.py \
  --store-root /tmp/liuxin-manual-store

python examples/storage/filesystem_driver_example.py \
  --store-root /tmp/liuxin-driver-store \
  --object-key incoming/book.epub

python examples/storage/assimilate_existing_disk_example.py \
  --source-root /media/books \
  --destination-root /srv/liuxin/managed \
  --extension epub --extension mobi

python examples/storage/http_remote_read_example.py \
  --base-url https://files.example.org/library/ \
  --object-key books/example.epub \
  --output /tmp/example.epub
```

See [`storage/README.md`](storage/README.md) for the ownership distinction
between copying, adopting, and database registration.

## Conversion starting points

```bash
python examples/conversion/conversion_to_oeb_example.py \
  --input /path/to/book.epub \
  --output-dir /tmp/book-oeb \
  --clean-output

python examples/conversion/conversion_batch_to_oeb_example.py \
  --output-root /tmp/oeb-batch \
  --inputs /path/to/a.epub /path/to/b.mobi \
  --clean-output
```

Each category README lists the scripts in that directory and calls out any
network or data-mutation behavior.
