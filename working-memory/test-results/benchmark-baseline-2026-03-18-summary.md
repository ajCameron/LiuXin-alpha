# Benchmark Summary

- script: `benchmark_baseline_suite`
- created_utc: `2026-03-19T16:56:47+00:00`
- timed_scenarios: `72`
- skipped_scenarios: `6`

## Slowest Scenarios

| scenario | database | mean | median | max |
|---|---|---:|---:|---:|
| `opds:titles` | `named:metadata_rich_db_1` | 4334.559 ms | 4334.559 ms | 4334.559 ms |
| `opds:search` | `named:metadata_rich_db_1` | 4290.607 ms | 4290.607 ms | 4290.607 ms |
| `api:works` | `named:metadata_rich_db_1` | 3651.638 ms | 3651.638 ms | 3651.638 ms |
| `opds:titles` | `named:images_covers_db_1` | 3080.240 ms | 3080.240 ms | 3080.240 ms |
| `opds:search` | `named:images_covers_db_1` | 2491.454 ms | 2491.454 ms | 2491.454 ms |
| `api:works` | `named:images_covers_db_1` | 1974.467 ms | 1974.467 ms | 1974.467 ms |
| `opds:titles` | `named:weird_data_db_0` | 1958.628 ms | 1958.628 ms | 1958.628 ms |
| `opds:titles` | `named:stores_assets_db_1` | 1907.719 ms | 1907.719 ms | 1907.719 ms |
| `opds:search` | `named:stores_assets_db_1` | 1897.608 ms | 1897.608 ms | 1897.608 ms |
| `api:index` | `named:metadata_rich_db_1` | 1856.118 ms | 1856.118 ms | 1856.118 ms |

## Skipped Scenarios

- `image_bytes` on `named:benchmark_db_smoke`: `unsupported_or_missing_fixture_data`
- `file_download` on `named:metadata_rich_db_1`: `unsupported_or_missing_fixture_data`
- `image_bytes` on `named:metadata_rich_db_1`: `unsupported_or_missing_fixture_data`
- `file_download` on `named:images_covers_db_1`: `unsupported_or_missing_fixture_data`
- `file_download` on `named:pathological_relations_db_0`: `unsupported_or_missing_fixture_data`
- `image_bytes` on `named:pathological_relations_db_0`: `unsupported_or_missing_fixture_data`
