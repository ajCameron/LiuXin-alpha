# Benchmark Test Database Plan

Date: 2026-03-18

## Decision

Add explicit alpha-native benchmark databases as opt-in resources, not as part of
the ordinary `test_db_0..25` compatibility corpus.

## Why

- Performance and benchmark work wants a larger synthetic corpus than ordinary
  correctness tests.
- The existing `tests/support/test_resources_manager.py` seam is already the
  right place to define named, cached test DB templates.
- Large benchmark fixtures should not silently burden the default unit-test
  path.

## What Landed

- Added named benchmark specs to the resource manager:
  - `benchmark_db_smoke`
  - `benchmark_db_medium`
  - `benchmark_db_large`
- Added a public helper for deterministic profiled synthetic DB creation:
  - `build_profiled_test_database(...)`
- Added a standalone builder script:
  - `scripts/build_benchmark_test_db.py`
- Added the first benchmark script suite:
  - `scripts/benchmark_read_paths.py`
  - `scripts/benchmark_surface_paths.py`
  - `scripts/benchmark_baseline_suite.py`
  - `scripts/summarize_benchmark_report.py`

## Current Shapes

- `benchmark_db_smoke`
  - `250` books
  - `1000` folders
  - `4000` files
- `benchmark_db_medium`
  - `2500` books
  - `10000` folders
  - `40000` files
- `benchmark_db_large`
  - `10000` books
  - `40000` folders
  - `160000` files

## Validation

- Resource manager now lists the benchmark DB names.
- `benchmark_db_smoke` provisions and passes FK/integrity checks.
- The standalone script works for:
  - named benchmark profiles
  - custom `--books/--folders/--files` builds
- The new benchmark scripts compile and run against live named fixtures.
- A first combined JSON baseline report now exists under:
  - [benchmark-baseline-2026-03-18.json](test-results/benchmark-baseline-2026-03-18.json)
- A markdown summary can now be generated from the JSON report with:
  - [summarize_benchmark_report.py](../scripts/summarize_benchmark_report.py)

## Use

Named profile:

```bash
cd /home/blackjane/LiuXin-alpha-wsl
python3 scripts/build_benchmark_test_db.py \
  --name benchmark_db_large \
  --output /tmp/benchmark_db_large.test_db
```

Custom profile:

```bash
cd /home/blackjane/LiuXin-alpha-wsl
python3 scripts/build_benchmark_test_db.py \
  --books 5000 \
  --folders 20000 \
  --files 80000 \
  --output /tmp/benchmark_db_custom.test_db
```

## Next Likely Steps

- Keep `benchmark_db_medium` in the slower `nightly` profile rather than the
  default interactive suite.
- Add an explicit nightly baseline artifact once we are ready to pay the longer
  run time.
- If current sizes are too small or too large, tune the named benchmark
  profiles without disturbing the ordinary `test_db_*` fixtures.
