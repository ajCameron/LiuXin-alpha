# Todo

- Decide whether we want `python-event-bus` for a unified event system.
- Work down the conversion pipeline sign-off plan in
  [conversion_pipeline_todo.md](conversion_pipeline_todo.md), using
  [conversion_pipeline_signoff.md](conversion_pipeline_signoff.md) as the
  durable format/edge status matrix: visible/reportable loss diagnostics,
  explicit capability edges, fallback reporting, shared archive preflight, and
  format-level done criteria.
- Continue the alpha-native semantic test DB series from [07 - Test Databases.md](/home/blackjane/LiuXin-alpha-wsl/dev-docs/07%20-%20Test%20Databases.md) by adding `_db_2` members only where a third semantic shape is justified, and only add `compat_projection_db_0` if we decide to support a real compatibility contract.
- Add an explicit nightly benchmark summary artifact alongside the `nightly` JSON output from [benchmark_baseline_suite.py](/home/blackjane/LiuXin-alpha-wsl/scripts/benchmark_baseline_suite.py), and decide whether the benchmark summarizer should also emit a condensed machine-readable slowest-path list.
- Continue the optimization work from [optimization-pass-driver-wrapper-opds-2026-03-19.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/optimization-pass-driver-wrapper-opds-2026-03-19.md) by profiling `_global_search_entries(...)` and the remaining OPDS search/result assembly path.
