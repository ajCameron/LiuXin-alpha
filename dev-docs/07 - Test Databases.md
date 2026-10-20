# Test databases: selection, provisioning and resource use

Reviewed: **2026-10-20**, against the fixture providers and collected contracts.

Named fixtures provide repeatable correctness cases. Choose the smallest fixture
that represents the behaviour under test; choose benchmark resources explicitly
when measuring scale. A historical fixture number is not a specification of its
current semantic contents.

## What exists now

The [resource manager](../tests/support/test_resources_manager.py) discovers three
groups. Discovery lists names; it does not build every listed database.

| Group | Current members | Purpose |
| --- | --- | --- |
| Retained numbered profiles | `test_db_0` through `test_db_25` | Existing smoke, blank, volume and profiled FRBR fixtures. Many differ mainly by counts or optional-data profiles. |
| Semantic families | Twelve members listed below | Distinct metadata, file, cover, identifier and relation cases. |
| Benchmarks | `benchmark_db_smoke`, `benchmark_db_medium`, `benchmark_db_large` | Explicit performance/scale inputs, not a reason to enlarge every correctness test. |

The [provisioning contracts](../tests/databases/test_test_resources_manager.py)
check discovery, shape, isolation and usable assets. They are the detailed source
for what a named fixture currently contains. Some retained identifiers and raw
fixture inserts deliberately exercise compatibility or historical data; successful
construction is not evidence that every row satisfies the public Catalog write
validators.

## Semantic families

Builders live in [tests/support/test_databases](../tests/support/test_databases/).

| Family | Live members | Distinct coverage |
| --- | --- | --- |
| `metadata_rich_db` | `_0`, `_1` | Agent roles, notes/comments/synopses, annotations and richer multilingual relationships. |
| `stores_assets_db` | `_0`, `_1` | Real fixture files, Store-root rewriting and retrieval, including multiple Stores. |
| `images_covers_db` | `_0`, `_1` | Cover links, usable image files, variants and records without covers. |
| `custom_columns_populated_db` | `_0`, `_1` | Generated custom-column schema and populated scalar, series and other supported shapes. |
| `identifiers_db` | `_0`, `_1` | Curated/observed identifier and compatibility-view shapes. |
| `pathological_relations_db` | `_0` | Dense valid relation fan-out and deep/wide structures. |
| `weird_data_db` | `_0` | Unicode, long/messy values and unusual paths. |

For example, the first metadata member is named `metadata_rich_db_0`.
The custom-column builders use `CustomColumns` for schema creation but seed
values directly into generated tables; they do not establish end-to-end public
setter coverage. Store/file fixtures likewise retain compatibility rows and
should not substitute for the dedicated modern Asset/Replica persistence tests.

There is no named `compat_projection_db_0` builder yet. Compatibility views do
exist and have [their own tests](../tests/databases/test_frbr_aggregate_views_wemi.py).
Add a reusable family only when a concrete cross-consumer case justifies it.
The [worked WEMI examples](databases/wemi-link-examples.md) suggest useful future
shapes, including several Works sharing one edition and Item.

## How resolution and isolation work

The default provider order is:

1. Prebuilt bundles from configured locations and available `LiuXin_data` assets.
2. Imported builder modules, normally under `tests.support.test_databases`.
3. Built-in `TestDatabaseSpec` profiles.

Prebuilt precedence matters when reproducing a failure. `LIUXIN_TEST_DATABASES_DIR`
selects an additional prebuilt directory; `LIUXIN_DATA_DIR` and
`LIUXIN_TEST_DATABASE_BUILDER_PREFIXES` affect data discovery and imported builders.
Record relevant overrides when reporting a fixture discrepancy.

An imported module participates only if it exposes a callable `populate_bundle`,
`build`, `build_database` or `build_test_database`. Helper/test modules are excluded.
The module's mere presence is not a supported provider entry point.

The session resource manager caches reusable templates beneath pytest's base
scratch directory. `provision_test_database(name)` copies a bundle into the
current test's `tmp_path`, including associated assets and relocated Store roots
where applicable. Mutate that provisioned copy, not the shared template or data
submodule. Explicit destination directories keep their own lifetime; they are
not automatically swept as per-test scratch.

### A small fixture consumer

This is a pytest test using the root `conftest.py` fixture. It checks that the
provisioned database opens and has the expected smoke projection, then closes the
connection explicitly:

```python
import sqlite3
from contextlib import closing


def test_smoke_projection(provision_test_database):
    provisioned = provision_test_database("test_db_0")
    with closing(sqlite3.connect(str(provisioned.db_path))) as connection:
        assert connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE name = 'titles' AND type IN ('table', 'view')"
        ).fetchone() == (1,)
```

A SQLite transaction context alone does not close its connection. For tests that
need the composed application/storage graph, use the owned `LibraryServices`
lifecycle illustrated in the provisioning contracts rather than reopening a
legacy Library facade.

## Scratch cleanup and telemetry

Use serial execution first when investigating VM or disk pressure. This bounded
example runs one existing provisioning test with a fresh, dedicated run directory:

```bash
run_dir="$(mktemp -d /tmp/liuxin-fixtures.XXXXXX)"
PYTHONPATH="$PWD/src" .venv/bin/python -m pytest -q -n 0 \
  tests/databases/test_test_resources_manager.py::test_provisioned_database_opens \
  --telemetry-dir="$run_dir/telemetry" \
  --basetemp="$run_dir/scratch" \
  -o cache_dir="$run_dir/cache" \
  -o tmp_path_retention_policy=none
```

Run from the repository root. Pytest clears an explicit `--basetemp` on a later
run, so give it a dedicated scratch directory rather than a directory containing
evidence or other work. Keep telemetry outside that scratch tree.

The [telemetry plugin](../tests/support/pytest_telemetry.py) records collection,
test phases/outcomes and session events. On Linux its independent resource sampler
records process and host observations. An unfinished test/phase identifies where
the run stopped, not by itself the cause of a VM failure.

The [scratch plugin](../tests/support/pytest_scratch.py) redirects ordinary
per-test temporary allocations, including child-process temp environment paths.
With `tmp_path_retention_policy=none`, it removes the current test's scratch after
finalizers, including failed tests. Use `all` when failed/setup/teardown files are
needed for investigation. Session templates remain until session-level cleanup;
close database handles and child processes before removing their files.

On WSL, compare guest scratch use with backing-volume free space. Removing a
Linux file does not establish that the Windows volume immediately recovered the
same space. Retain useful logs, then remove only the run directory you own once
all processes and handles have closed. The
[storage-pressure investigation](../working-memory/test-vm-telemetry-2026-10-17.md)
records the earlier evidence and fixes; it is not a current machine-health report.

## Benchmarks are explicit campaigns

[build_benchmark_test_db.py](../scripts/build_benchmark_test_db.py) supports named
or custom-count builds. The [baseline runner](../scripts/benchmark_baseline_suite.py)
combines [read-path](../scripts/benchmark_read_paths.py) and
[surface-path](../scripts/benchmark_surface_paths.py) measurements. Its default
`interactive` profile excludes the medium corpus; `nightly` adds it.

Choose a cache and output location with enough space, for example:

```bash
PYTHONPATH="$PWD/src" .venv/bin/python scripts/benchmark_baseline_suite.py \
  --profile interactive --cache-dir /tmp/liuxin-benchmark-cache \
  --output /tmp/liuxin-benchmark-results/baseline.json
.venv/bin/python scripts/summarize_benchmark_report.py \
  /tmp/liuxin-benchmark-results/baseline.json --format markdown \
  --output /tmp/liuxin-benchmark-results/baseline.md
```

This is a benchmark run, not the quick provisioning smoke check above. Its
`--keep-provisioned` option retains copies for inspection; template caches have
an independent lifetime. Record profile, fixture source, environment and report
path with results. Historical pass totals and March benchmark filenames do not
establish current correctness or throughput.

## Extending coverage

Give a new fixture a semantic purpose and one collected contract proving it.
Add another family member only for a distinct shape, not just a historical name
or larger row count. Keep resource use bounded and close all owned resources.
Test metadata acceptance through public Catalog APIs when that is the claim;
use explicitly low-level fixtures for malformed/history/schema cases.

Run the relevant provisioning/asset contracts and
[property-support registry tests](../tests/support/test_databases/test_db_properties/test_property_support_registry.py).
Use [test streams](hacking/test-streams.md) to choose wider verification. Report
fresh results with their date and scope instead of maintaining an undated green
count in this guide. Return to the [testing index](hacking/testing/README.md).
