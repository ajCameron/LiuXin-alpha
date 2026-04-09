# Benchmark Harness

Date: 2026-03-19

## What Landed

The first alpha-native benchmark script suite is now live.

Scripts:

- [benchmark_read_paths.py](/home/blackjane/LiuXin-alpha-wsl/scripts/benchmark_read_paths.py)
- [benchmark_interface_paths.py](/home/blackjane/LiuXin-alpha-wsl/scripts/benchmark_interface_paths.py)
- [benchmark_baseline_suite.py](/home/blackjane/LiuXin-alpha-wsl/scripts/benchmark_baseline_suite.py)
- [summarize_benchmark_report.py](/home/blackjane/LiuXin-alpha-wsl/scripts/summarize_benchmark_report.py)
- shared support:
  - [\_benchmark_common.py](/home/blackjane/LiuXin-alpha-wsl/scripts/_benchmark_common.py)

## Scope

### `benchmark_read_paths.py`

Benchmarks backend-facing hot paths on a provisioned named fixture or explicit
database path:

- open/close database
- title-sorted work listing
- read-model search payload generation
- work-detail payload generation
- file download path
- image-byte resolution path

### `benchmark_interface_paths.py`

Benchmarks WSGI request handling for:

- [web_readonly](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_alpha/interfaces/web_readonly/app.py)
- [api_readonly](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_alpha/interfaces/api_readonly/app.py)
- [opds_readonly](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_alpha/interfaces/opds_readonly/app.py)

Default route coverage:

- web:
  - `/`
  - `/search?global_q=...`
  - `/tables/works/<id>`
- api:
  - `/api`
  - `/api/works`
  - `/api/works/<id>`
  - `/api/search?q=...`
- opds:
  - `/opds`
  - `/opds/navcatalog/...`
  - `/opds/search/...`
  - `/get/<format>/<work>/main` when the fixture has formats

### `benchmark_baseline_suite.py`

Runs a combined JSON baseline over a small default matrix.

Default backend targets:

- `benchmark_db_smoke`
- `metadata_rich_db_1`
- `stores_assets_db_1`
- `images_covers_db_1`
- `pathological_relations_db_0`
- `weird_data_db_0`

Default interface targets:

- `metadata_rich_db_1`
- `stores_assets_db_1`
- `images_covers_db_1`
- `weird_data_db_0`

Important default:

- `--profile interactive`
  - default
  - excludes `benchmark_db_medium`
  - intended for iterative local runs
- `--profile nightly`
  - includes `benchmark_db_medium`
  - intended for slower scheduled baselines

### `summarize_benchmark_report.py`

Renders a benchmark JSON report into either:

- plain text
- markdown

This is intended to make the JSON artifacts usable in code review and handoff
without opening them directly.

## Output Shape

All scripts emit JSON with:

- script name
- environment metadata
- database source info
- input parameters
- per-scenario timing stats
  - `min_ms`
  - `mean_ms`
  - `median_ms`
  - `max_ms`
  - raw `durations_ms`
- a small scenario sample payload

Default runtime behavior:

- benchmark scripts are now chatty on `stderr`
- they report:
  - target preparation
  - resolved DB path/source
  - scenario start/finish
  - per-scenario timing summaries
- use `--quiet` to suppress progress logging and keep only the final summary
  line

## First Baseline Artifact

Combined report:

- [benchmark-baseline-2026-03-18.json](/home/blackjane/LiuXin-alpha-wsl/working-memory/test-results/benchmark-baseline-2026-03-18.json)

Summary derived from that report:

- [benchmark-baseline-2026-03-18-summary.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/test-results/benchmark-baseline-2026-03-18-summary.md)

This was generated with:

- `benchmark_baseline_suite.py --iterations 1 --warmups 0`

It is a shape/baseline artifact, not a statistically serious performance run.

## Validation

- `python3 -m py_compile` passed for all four benchmark scripts
- `python3 -m py_compile` passed for the summarizer as well
- direct backend benchmark smoke:
  - `stores_assets_db_1`
- direct interface benchmark smoke:
  - `metadata_rich_db_1`
- combined baseline suite completed and wrote the JSON report above
- summarizer ran against the combined baseline report and produced the markdown
  summary above

## Known Constraints

- benchmark scripts intentionally suppress fixture-builder chatter and row
  fallback diagnostics so the output stays benchmark-shaped
- `custom_columns_populated_db_*` still reflects the current product seam where
  columns are created through the live API but values are seeded directly into
  generated tables
- `benchmark_db_smoke` has synthetic file rows but not real downloadable assets,
  so file-download scenarios there can legitimately return `404`

## Optimization Follow-up

First hot-path optimization pass is now recorded in:

- [optimization-pass-driver-wrapper-opds-2026-03-19.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/optimization-pass-driver-wrapper-opds-2026-03-19.md)

Key measured improvements from that pass:

- `benchmark_db_medium`
  - `work_list_title`: `9046.279ms -> 275.126ms`
  - `work_search_global`: `9743.671ms -> 867.667ms`
- `metadata_rich_db_1`
  - `opds:titles`: `4451.690ms -> 2130.647ms`
  - `opds:search`: `4458.867ms -> 2111.079ms`

## Next Likely Steps

1. profile `_global_search_entries(...)` and remaining OPDS search assembly now
   that schema/row overhead is substantially reduced
2. add an explicit nightly summary artifact alongside the nightly JSON report
3. decide whether the summarizer should also emit a condensed machine-readable
   top-slowest list
