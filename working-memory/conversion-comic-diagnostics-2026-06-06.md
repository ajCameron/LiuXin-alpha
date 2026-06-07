# Conversion Comic Diagnostics Slice - 2026-06-06

## Decision

Comic CBZ/CBC/CBR structured diagnostics are promoted to candidate for focused
sign-off review after this implementation slice merges.

The implemented scope preserves strict archive failures and adds structured
recoverable diagnostics:

- `cbc-listed-comic-missing` when CBC skips a listed comic member but still
  converts at least one remaining comic
- `rar-names-only-preflight-limited` when CBR/RAR preflight falls back to a
  names-only external listing, so member-count and path-safety checks run but
  size and compression-ratio budgets cannot run before extraction

## Validation

```text
python3 -m py_compile src/LiuXin_alpha/file_formats/conversion/plugins/comic_input.py tests/file_formats/comic/test_comic_malformed_hostile.py tests/file_formats/comic/test_comic_container_framework.py
clean

python3 -m pytest tests/file_formats/comic/test_comic_malformed_hostile.py tests/file_formats/comic/test_comic_container_framework.py -q
50 passed in 14.76s

python3 -m pytest tests/file_formats/comic -q
67 passed in 10.47s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/comic/test_comic_malformed_hostile.py -q
54 passed in 10.28s

python3 -m pytest tests/file_formats/conversion/test_conversion_report.py tests/file_formats/conversion/test_conversion_edges.py tests/file_formats/conversion/test_conversion_top_level_smoke.py tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py -q
13 passed in 10.03s
```

## Boundaries

Unsafe paths, invalid archives, password entries, budget failures, invalid
`comics.txt`, all-listed-CBC-missing, and no-page outputs remain strict failures.
A small redistributable real CBR corpus remains future regression coverage rather
than a blocker for this diagnostic row.

## Next Useful Step

After this branch merges, perform a focused Comic sign-off review for the current
CBZ/CBC/CBR diagnostics scope.
