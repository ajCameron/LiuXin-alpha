# Optimization Pass: Driver Wrapper And OPDS

## Scope

First targeted optimization pass based on the new benchmark harness and direct
profiles.

Touched code:

- `src/LiuXin_alpha/databases/database_driver_plugins/driver_wrapper.py`
- [row.py](../src/LiuXin_alpha/databases/row.py)
- [read_model/api.py](../src/LiuXin_alpha/surfaces/read_model/api.py)
- [opds_readonly/app.py](../src/LiuXin_alpha/surfaces/opds_readonly/app.py)
- [web_calibre_readonly/app.py](../src/LiuXin_alpha/surfaces/web_calibre_readonly/app.py)
- [benchmark_read_paths.py](../scripts/benchmark_read_paths.py)

Validation:

- database contract slice: `46 passed`
- OPDS / Calibre interface slice: `15 passed`

## Changes

### 1. Wrapper-level derived schema caches

Added cache layers in `DriverWrapper` for:

- `get_column_base(...)`
- `get_link_table_name(...)`
- `check_for_intralink_table(...)`
- `get_interlinked_tables(...)`
- `identify_table_from_column(...)`
- `identify_table_from_row_dict(...)`

The cache is keyed off the live `tables_and_columns` object identity so it is
cleared when the driver refreshes schema caches.

### 2. Row refresh cleanup

`Row.refresh_db_properties()` no longer re-identifies the row table through
`get_id_from_row(...)` after it has already resolved the table.

It now:

- reuses the current table
- pulls the id directly from that table's id column
- reuses the wrapper's allowed-table snapshot

### 3. Narrow OPDS work metadata path

`opds_work_metadata_payload(...)` no longer builds the full generic
related-entity graph for each visible work.

It now fetches only the tables OPDS actually needs for entry generation:

- `expressions`
- `files`
- `labels`
- `series`

This leaves the generic rich detail path intact for the HTML interfaces.

### 4. Benchmark harness setup fix

`benchmark_read_paths.py` used to eagerly prepare:

- work detail payload data
- file rows
- image rows

even when the requested scenarios only needed list/search.

It now prepares only what the requested scenario set actually uses.

## Measured Results

Reference nightly baseline:

- [benchmark-baseline-nightly-2026-03-19.json](test-results/benchmark-baseline-nightly-2026-03-19.json)

Post-pass artifacts:

- [benchmark-read-hotpaths-2026-03-19.json](test-results/benchmark-read-hotpaths-2026-03-19.json)
- [benchmark-opds-hotpaths-2026-03-19.json](test-results/benchmark-opds-hotpaths-2026-03-19.json)

Before vs after:

- `benchmark_db_medium`
  - `work_list_title`
    - before: `9046.279ms`
    - after: `275.126ms`
  - `work_search_global`
    - before: `9743.671ms`
    - after: `867.667ms`
- `metadata_rich_db_1`
  - `opds:titles`
    - before: `4451.690ms`
    - after: `2130.647ms`
  - `opds:search`
    - before: `4458.867ms`
    - after: `2111.079ms`

## Conclusion

The first pass removed the schema/row-construction bottleneck and materially
reduced OPDS entry generation cost.

The next likely hotspot is no longer generic schema introspection. It is the
remaining search/result assembly path, especially:

- `_global_search_entries(...)`
- any remaining per-row metadata work done on OPDS search feeds

## Next Step

Profile and optimize OPDS search/result assembly directly, now that row/schema
overhead is no longer dominating the numbers.
