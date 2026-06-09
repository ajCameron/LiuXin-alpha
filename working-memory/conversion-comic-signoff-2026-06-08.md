# Conversion Comic Sign-Off - 2026-06-08

## Decision

Comic CBZ/CBC/CBR structured diagnostics are signed off for the current row
scope.

The signed-off scope includes:

- ZIP-backed CBZ/CBC archive preflight
- CBR/RAR listing and extraction boundaries
- path safety, password rejection, and archive budgets where the listing backend
  exposes sizes
- multilingual CBC and comic-page product assertions
- recoverable `cbc-listed-comic-missing` events for CBC missing-member salvage
- recoverable `rar-names-only-preflight-limited` events for names-only CBR/RAR
  preflight variance

## Validation

```text
python3 -m py_compile src/LiuXin_alpha/file_formats/conversion/plugins/comic_input.py tests/file_formats/comic/test_comic_malformed_hostile.py tests/file_formats/comic/test_comic_container_framework.py
clean

python3 -m pytest tests/file_formats/comic/test_comic_malformed_hostile.py tests/file_formats/comic/test_comic_container_framework.py -q
50 passed in 11.70s

python3 -m pytest tests/file_formats/comic -q
67 passed in 8.31s

python3 -m pytest tests/file_formats/test_archive_preflight.py tests/file_formats/comic/test_comic_malformed_hostile.py -q
54 passed in 9.39s

python3 -m pytest tests/file_formats/conversion/test_conversion_report.py tests/file_formats/conversion/test_conversion_edges.py tests/file_formats/conversion/test_conversion_top_level_smoke.py tests/file_formats/conversion/plugins/test_plugins_runtime_smoke.py -q
13 passed in 7.24s
```

## Boundaries

Unsafe paths, invalid archives, password entries, budget failures, invalid
`comics.txt`, all-listed-CBC-missing, and no-page outputs remain strict failures.
A small redistributable real CBR corpus remains future regression coverage rather
than a blocker for this diagnostics row. Future real-corpus comic defects should
be added as regressions or new follow-up rows.

## Next Useful Step

There are no remaining candidate rows after Comic sign-off. Pick a
provisional/open row and remove a named blocker before the next sign-off review:
MOBI/KF8 richer product coverage, pipeline-wide report/fallback semantics, or
Markdown/Textile direct or external edge diagnostics.
